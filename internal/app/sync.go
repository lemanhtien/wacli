package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/wa"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
)

const maxAuthConnectAttempts = 3

type SyncMode string

const (
	SyncModeBootstrap SyncMode = "bootstrap"
	SyncModeOnce      SyncMode = "once"
	SyncModeFollow    SyncMode = "follow"
)

type SyncOptions struct {
	Mode                SyncMode
	AllowQR             bool
	OnQRCode            func(string)
	PairPhoneNumber     string
	OnPairCode          func(string)
	AfterConnect        func(context.Context) error
	DownloadMedia       bool
	RefreshContacts     bool
	RefreshGroups       bool
	RefreshChannels     bool
	IdleExit            time.Duration // only used for bootstrap/once
	MaxReconnect        time.Duration // max time to attempt reconnection before giving up (0 = unlimited)
	MaxMessages         int64         // 0 = unlimited
	MaxDBSizeBytes      int64         // 0 = unlimited
	WarnNoLimits        bool
	WebhookURL          string
	WebhookSecret       string
	WebhookAllowPrivate bool
	Verbosity           int // future
}

type SyncResult struct {
	MessagesStored int64
}

func (a *App) Sync(ctx context.Context, opts SyncOptions) (SyncResult, error) {
	status := a.beginSyncStatus()
	defer a.endSyncStatus(status)

	if opts.Mode == "" {
		opts.Mode = SyncModeFollow
	}
	if (opts.Mode == SyncModeBootstrap || opts.Mode == SyncModeOnce) && opts.IdleExit <= 0 {
		opts.IdleExit = 30 * time.Second
	}
	if opts.WarnNoLimits && opts.MaxMessages <= 0 && opts.MaxDBSizeBytes <= 0 {
		a.emitWarning(
			"sync_storage_uncapped",
			"warning: sync storage is uncapped; use --max-messages or --max-db-size to bound local history growth",
			nil,
		)
	}
	if err := a.checkSyncStorageLimits(opts); err != nil {
		return SyncResult{}, err
	}

	syncCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	limits := &syncStorageLimits{app: a, opts: opts, cancel: cancel}

	if err := a.OpenWA(); err != nil {
		return SyncResult{}, err
	}
	a.wa.SetManualHistorySyncDownload(true)
	defer a.wa.SetManualHistorySyncDownload(false)

	var messagesStored atomic.Int64
	lastEvent := atomic.Int64{}
	lastEvent.Store(nowUTC().UnixNano())

	disconnected := make(chan struct{}, 1)

	var stopMedia func()
	var mediaJobs chan mediaJob
	enqueueMedia := func(chatJID, msgID string) {}
	if opts.DownloadMedia {
		mediaJobs = make(chan mediaJob, 512)
		enqueueMedia = newMediaEnqueuer(syncCtx, mediaJobs)
	}

	if opts.DownloadMedia {
		var err error
		stopMedia, err = a.runMediaWorkers(syncCtx, mediaJobs, 4)
		if err != nil {
			return SyncResult{}, err
		}
		defer stopMedia()
	}

	var stopWebhook func()
	var webhookJobs chan wa.ParsedMessage
	enqueueWebhook := func(wa.ParsedMessage) {}
	if syncWebhookEnabled(opts) {
		webhookJobs = make(chan wa.ParsedMessage, 512)
		enqueueWebhook = a.newSyncWebhookEnqueuer(syncCtx, webhookJobs)
		stopWebhook = a.runSyncWebhookWorker(syncCtx, opts, webhookJobs)
		defer stopWebhook()
	}

	// History chunks are replayed by one dedicated worker so the whatsmeow node
	// handler returns immediately; see runHistoryWorker for the incident that
	// motivated this. The group-info cache is per run.
	a.groups.reset()
	a.historySkipped.Store(0)
	enqueueHistory, stopHistory := a.runHistoryWorker(syncCtx, opts, &messagesStored, &lastEvent, enqueueMedia, limits)
	defer stopHistory()

	handlerID := a.addSyncEventHandler(syncCtx, opts, &messagesStored, &lastEvent, disconnected, enqueueMedia, enqueueWebhook, enqueueHistory, limits)
	defer a.wa.RemoveEventHandler(handlerID)

	if err := a.connectForSync(syncCtx, opts); err != nil {
		return SyncResult{}, err
	}
	lastEvent.Store(nowUTC().UnixNano())
	if err := a.migrateHistoricalLIDs(syncCtx); err != nil {
		return SyncResult{MessagesStored: messagesStored.Load()}, err
	}
	a.syncAppStateDeltas(syncCtx)

	// Optional: bootstrap imports (helps contacts/groups management without waiting for events).
	if opts.RefreshContacts {
		if err := a.refreshContacts(syncCtx); err != nil {
			a.emitWarning(
				"refresh_contacts_failed",
				fmt.Sprintf("warning: failed to refresh contacts: %v", err),
				map[string]any{"error": err.Error()},
			)
		}
	}
	if opts.RefreshGroups {
		if err := a.refreshGroups(syncCtx); err != nil {
			a.emitWarning(
				"refresh_groups_failed",
				fmt.Sprintf("warning: failed to refresh groups: %v", err),
				map[string]any{"error": err.Error()},
			)
		}
	}
	if opts.RefreshChannels {
		if err := a.refreshNewsletters(syncCtx); err != nil {
			a.emitWarning(
				"refresh_channels_failed",
				fmt.Sprintf("warning: failed to refresh channels: %v", err),
				map[string]any{"error": err.Error()},
			)
		}
	}
	if opts.AfterConnect != nil {
		if err := opts.AfterConnect(syncCtx); err != nil {
			return SyncResult{MessagesStored: messagesStored.Load()}, err
		}
	}

	var err error
	if opts.Mode == SyncModeFollow {
		_, err = a.runSyncFollow(syncCtx, opts.MaxReconnect, &messagesStored, disconnected)
	} else {
		_, err = a.runSyncUntilIdle(syncCtx, opts.IdleExit, opts.MaxReconnect, &messagesStored, &lastEvent, disconnected)
	}
	if limitErr := limits.Err(); limitErr != nil {
		return SyncResult{MessagesStored: messagesStored.Load()}, limitErr
	}
	if err != nil {
		return SyncResult{MessagesStored: messagesStored.Load()}, err
	}
	return SyncResult{MessagesStored: messagesStored.Load()}, nil
}

func (a *App) syncAppStateDeltas(ctx context.Context) {
	for _, name := range []appstate.WAPatchName{appstate.WAPatchRegularHigh, appstate.WAPatchRegularLow, appstate.WAPatchRegular} {
		fullSync := name == appstate.WAPatchRegular
		if err := a.wa.FetchAppState(ctx, string(name), fullSync, false); err != nil {
			a.emitWarning(
				"app_state_sync_failed",
				fmt.Sprintf("warning: failed to sync WhatsApp app state %s: %v", name, err),
				map[string]any{"name": string(name), "error": err.Error()},
			)
		}
	}
}

func (a *App) connectForSync(ctx context.Context, opts SyncOptions) error {
	connectOpts := wa.ConnectOptions{
		AllowQR:         opts.AllowQR,
		OnQRCode:        opts.OnQRCode,
		PairPhoneNumber: opts.PairPhoneNumber,
		OnPairCode:      opts.OnPairCode,
	}

	attempts := 1
	if opts.AllowQR || opts.PairPhoneNumber != "" {
		attempts = maxAuthConnectAttempts
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		err := a.wa.Connect(ctx, connectOpts)
		if err == nil {
			return nil
		}
		if attempt == attempts || ctx.Err() != nil || !isRetryableAuthConnectError(err) {
			return err
		}
		a.emitWarning(
			"auth_connect_retry",
			fmt.Sprintf("warning: auth connection dropped before pairing completed; retrying (%d/%d)", attempt+1, attempts),
			map[string]any{"attempt": attempt + 1, "attempts": attempts},
		)
		select {
		case <-time.After(authConnectRetryDelay(attempt)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func authConnectRetryDelay(attempt int) time.Duration {
	return time.Duration(attempt) * 500 * time.Millisecond
}

func isRetryableAuthConnectError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"qr code timed out",
		"qr channel closed",
		"websocket",
		"failed to read frame header",
		"connection reset",
		"broken pipe",
		"i/o timeout",
		"eof",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

func (a *App) checkSyncStorageLimits(opts SyncOptions) error {
	if opts.MaxMessages > 0 {
		count, err := a.db.CountMessages()
		if err != nil {
			return fmt.Errorf("check message limit: %w", err)
		}
		if count >= opts.MaxMessages {
			return syncStorageLimitError("message", count, opts.MaxMessages)
		}
	}
	if opts.MaxDBSizeBytes > 0 {
		size, err := a.dbDiskSize()
		if err != nil {
			return fmt.Errorf("check database size limit: %w", err)
		}
		if size >= opts.MaxDBSizeBytes {
			return syncStorageLimitError("database size", size, opts.MaxDBSizeBytes)
		}
	}
	return nil
}

func (a *App) dbDiskSize() (int64, error) {
	var total int64
	for _, path := range []string{
		filepath.Join(a.opts.StoreDir, "wacli.db"),
		filepath.Join(a.opts.StoreDir, "wacli.db-wal"),
		filepath.Join(a.opts.StoreDir, "wacli.db-shm"),
	} {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		if !info.IsDir() {
			total += info.Size()
		}
	}
	return total, nil
}

func syncStorageLimitError(kind string, got, limit int64) error {
	return fmt.Errorf("sync storage limit reached: %s is %d, limit is %d", kind, got, limit)
}

func chatKind(chat types.JID) string {
	if chat.Server == types.NewsletterServer {
		return "newsletter"
	}
	if chat.Server == types.GroupServer {
		return "group"
	}
	if chat.IsBroadcastList() {
		return "broadcast"
	}
	if chat.Server == types.DefaultUserServer {
		return "dm"
	}
	return "unknown"
}

// contactRecord is a contacts-table upsert derived while resolving a sender or
// DM peer. It is returned separately so the history path can dedupe it per
// conversation instead of rewriting the row for every message.
type contactRecord struct {
	jid, phone, pushName, fullName, firstName, businessName string
}

func contactRecordFrom(jid types.JID, info types.ContactInfo) contactRecord {
	return contactRecord{
		jid:          jid.String(),
		phone:        jid.User,
		pushName:     info.PushName,
		fullName:     info.FullName,
		firstName:    info.FirstName,
		businessName: info.BusinessName,
	}
}

func (a *App) upsertContactRecord(db *store.DB, c contactRecord) {
	_ = db.UpsertContact(c.jid, c.phone, c.pushName, c.fullName, c.firstName, c.businessName)
}

// dmContact returns the contact row for a DM peer, if the contact store knows
// it. Contact lookups are local (whatsmeow contact store), not network.
func (a *App) dmContact(ctx context.Context, chat types.JID) (contactRecord, bool) {
	if chat.Server != types.DefaultUserServer {
		return contactRecord{}, false
	}
	chat = canonicalJID(chat)
	info, err := a.wa.GetContact(ctx, chat)
	if err != nil {
		return contactRecord{}, false
	}
	return contactRecordFrom(chat, info), true
}

// resolveSender canonicalises the sender JID and picks the display name,
// returning the contact row to persist when the contact store knows the sender.
func (a *App) resolveSender(ctx context.Context, pm wa.ParsedMessage) (senderJID, senderName string, contact *contactRecord) {
	if pm.FromMe {
		senderName = "me"
	} else if s := strings.TrimSpace(pm.PushName); s != "" && s != "-" {
		senderName = s
	}
	senderJID = pm.SenderJID
	if pm.SenderJID == "" {
		return senderJID, senderName, nil
	}
	jid, err := types.ParseJID(pm.SenderJID)
	if err != nil {
		return senderJID, senderName, nil
	}
	contactJID := a.canonicalStoreJID(ctx, jid)
	senderJID = contactJID.String()
	info, err := a.wa.GetContact(ctx, contactJID)
	if err != nil {
		return senderJID, senderName, nil
	}
	if name := wa.BestContactName(info); name != "" {
		senderName = name
	}
	rec := contactRecordFrom(contactJID, info)
	return senderJID, senderName, &rec
}

type mediaFields struct {
	mediaType, caption, filename, mimeType, directPath string
	mediaKey, fileSha, fileEncSha                      []byte
	fileLen                                            uint64
}

func mediaFieldsOf(pm wa.ParsedMessage) mediaFields {
	var m mediaFields
	if pm.Media != nil {
		m.mediaType = pm.Media.Type
		m.caption = pm.Media.Caption
		m.filename = pm.Media.Filename
		m.mimeType = pm.Media.MimeType
		m.directPath = pm.Media.DirectPath
		m.mediaKey = pm.Media.MediaKey
		m.fileSha = pm.Media.FileSHA256
		m.fileEncSha = pm.Media.FileEncSHA256
		m.fileLen = pm.Media.FileLength
	}
	return m
}

func buildStatusMessageParams(pm wa.ParsedMessage, senderJID, senderName string) store.UpsertStatusMessageParams {
	m := mediaFieldsOf(pm)
	return store.UpsertStatusMessageParams{
		MsgID:         pm.ID,
		Timestamp:     pm.Timestamp,
		FromMe:        pm.FromMe,
		SenderJID:     senderJID,
		SenderName:    senderName,
		Text:          pm.Text,
		MediaType:     m.mediaType,
		MediaCaption:  m.caption,
		Filename:      m.filename,
		MimeType:      m.mimeType,
		DirectPath:    m.directPath,
		MediaKey:      m.mediaKey,
		FileSHA256:    m.fileSha,
		FileEncSHA256: m.fileEncSha,
		FileLength:    m.fileLen,
	}
}

// finalDisplayText computes the display text for pm, reading quoted/reacted
// messages through db (which may be a transaction-scoped store so rows written
// earlier in the same transaction are visible).
func (a *App) finalDisplayText(db *store.DB, pm wa.ParsedMessage) string {
	if pm.Revoked {
		return store.DeletedMessageDisplayText
	}
	return a.buildDisplayTextWith(db, pm)
}

// buildUpsertMessageParams assembles the messages row for pm. pm.Chat must
// already be canonical. DisplayText is left for the caller (finalDisplayText)
// because it reads the quoted/reacted row and must see the store the write
// goes to (the batch transaction in the history path).
func buildUpsertMessageParams(pm wa.ParsedMessage, chatJID, chatName, senderJID, senderName string) store.UpsertMessageParams {
	m := mediaFieldsOf(pm)
	return store.UpsertMessageParams{
		ChatJID:         chatJID,
		ChatName:        chatName,
		MsgID:           pm.ID,
		SenderJID:       senderJID,
		SenderName:      senderName,
		Timestamp:       pm.Timestamp,
		FromMe:          pm.FromMe,
		Text:            pm.Text,
		QuotedMsgID:     pm.ReplyToID,
		QuotedSenderJID: pm.ReplyToSenderJID,
		Buttons:         waButtonsToStore(pm.Buttons),
		IsForwarded:     pm.IsForwarded,
		ForwardingScore: pm.ForwardingScore,
		ReactionToID:    pm.ReactionToID,
		ReactionEmoji:   pm.ReactionEmoji,
		MediaType:       m.mediaType,
		MediaCaption:    m.caption,
		Filename:        m.filename,
		MimeType:        m.mimeType,
		DirectPath:      m.directPath,
		MediaKey:        m.mediaKey,
		FileSHA256:      m.fileSha,
		FileEncSHA256:   m.fileEncSha,
		FileLength:      m.fileLen,
		Edited:          pm.Edited,
		Revoked:         pm.Revoked,
	}
}

// storeMessageExtras persists the per-message side rows (call event, starred
// state) that accompany a stored message row.
func (a *App) storeMessageExtras(ctx context.Context, pm wa.ParsedMessage, chatJID, chatName, senderJID, senderName string) error {
	if pm.Call != nil {
		call := *pm.Call
		call.Chat = pm.Chat
		if call.SenderJID == "" {
			call.SenderJID = senderJID
		}
		if call.Timestamp.IsZero() {
			call.Timestamp = pm.Timestamp
		}
		if err := a.storeParsedCallEvent(ctx, call, chatName, senderName); err != nil {
			return err
		}
	}
	if pm.StarredKnown {
		return a.db.SetStarred(store.SetStarredParams{
			ChatJID:   chatJID,
			MsgID:     pm.ID,
			SenderJID: senderJID,
			FromMe:    pm.FromMe,
			Starred:   pm.Starred,
			StarredAt: pm.Timestamp,
		})
	}
	return nil
}

func (a *App) storeParsedMessage(ctx context.Context, pm wa.ParsedMessage) error {
	pm.Chat = a.canonicalStoreJID(ctx, pm.Chat)
	chatJID := canonicalJIDString(pm.Chat)
	chatName := a.resolveChatName(ctx, pm.Chat, pm.PushName)
	if pm.Chat != types.StatusBroadcastJID {
		if err := a.db.UpsertChat(chatJID, chatKind(pm.Chat), chatName, pm.Timestamp); err != nil {
			return err
		}
	}

	// Best-effort: store contact info for DMs.
	if rec, ok := a.dmContact(ctx, pm.Chat); ok {
		a.upsertContactRecord(a.db, rec)
	}

	senderJID, senderName, contact := a.resolveSender(ctx, pm)
	if contact != nil {
		a.upsertContactRecord(a.db, *contact)
	}

	// Best-effort: store group metadata (and participants) when available.
	a.ensureGroupStored(ctx, pm.Chat)

	if pm.Chat == types.StatusBroadcastJID {
		return a.db.UpsertStatusMessage(buildStatusMessageParams(pm, senderJID, senderName))
	}

	params := buildUpsertMessageParams(pm, chatJID, chatName, senderJID, senderName)
	params.DisplayText = a.finalDisplayText(a.db, pm)
	if err := a.db.UpsertMessage(params); err != nil {
		return err
	}
	return a.storeMessageExtras(ctx, pm, chatJID, chatName, senderJID, senderName)
}

func (a *App) storeParsedCallEvent(ctx context.Context, call wa.ParsedCallEvent, chatName, senderName string) error {
	call.Chat = a.canonicalStoreJID(ctx, call.Chat)
	chatJID := canonicalJIDString(call.Chat)
	if chatJID == "" {
		return fmt.Errorf("call chat JID is required")
	}
	if chatName == "" {
		chatName = a.wa.ResolveChatName(ctx, call.Chat, "")
	}
	if err := a.db.UpsertChat(chatJID, chatKind(call.Chat), chatName, call.Timestamp); err != nil {
		return err
	}

	senderJID := strings.TrimSpace(call.SenderJID)
	if senderJID != "" {
		if jid, err := types.ParseJID(senderJID); err == nil {
			contactJID := a.canonicalStoreJID(ctx, jid)
			senderJID = contactJID.String()
			if senderName == "" {
				if info, err := a.wa.GetContact(ctx, contactJID); err == nil {
					senderName = wa.BestContactName(info)
				}
			}
		}
	}

	participants := make([]store.CallParticipant, 0, len(call.Participants))
	for _, p := range call.Participants {
		jid := strings.TrimSpace(p.JID)
		if jid != "" {
			if parsed, err := types.ParseJID(jid); err == nil {
				jid = canonicalJIDString(a.canonicalStoreJID(ctx, parsed))
			}
		}
		if jid == "" {
			continue
		}
		participants = append(participants, store.CallParticipant{
			JID:     jid,
			Outcome: p.Outcome,
		})
	}

	return a.db.UpsertCallEvent(store.UpsertCallEventParams{
		ChatJID:      chatJID,
		ChatName:     chatName,
		SenderJID:    senderJID,
		SenderName:   senderName,
		CallID:       call.CallID,
		MsgID:        call.MsgID,
		EventType:    call.EventType,
		Direction:    call.Direction,
		Media:        call.Media,
		Outcome:      call.Outcome,
		Reason:       call.Reason,
		CallType:     call.CallType,
		DurationSecs: call.DurationSecs,
		Timestamp:    call.Timestamp,
		Participants: participants,
	})
}

func (a *App) deleteParsedCallEvents(ctx context.Context, deleted wa.ParsedCallDelete) error {
	chat := a.canonicalStoreJID(ctx, deleted.Chat)
	chatJID := canonicalJIDString(chat)
	if chatJID == "" {
		return fmt.Errorf("call chat JID is required")
	}
	_, err := a.db.DeleteCallEvents(store.DeleteCallEventsParams{
		ChatJID:   chatJID,
		Direction: deleted.Direction,
	})
	return err
}

func waButtonsToStore(buttons []wa.Button) []store.Button {
	if len(buttons) == 0 {
		return nil
	}
	out := make([]store.Button, len(buttons))
	for i, b := range buttons {
		out[i] = store.Button{
			Type:         b.Type,
			DisplayText:  b.DisplayText,
			ID:           b.ID,
			URL:          b.URL,
			PhoneNumber:  b.PhoneNumber,
			Description:  b.Description,
			ResponseType: b.ResponseType,
			Index:        b.Index,
		}
	}
	return out
}

// buildDisplayTextWith renders the display text for pm, reading quoted/reacted
// messages through db (which may be transaction-scoped).
func (a *App) buildDisplayTextWith(db *store.DB, pm wa.ParsedMessage) string {
	base := baseDisplayText(pm)

	if pm.ReactionToID != "" || strings.TrimSpace(pm.ReactionEmoji) != "" {
		target := strings.TrimSpace(pm.ReactionToID)
		display := ""
		if target != "" {
			display = a.lookupMessageDisplayTextIn(db, pm.Chat.String(), target)
		}
		if display == "" {
			display = "message"
		}
		emoji := strings.TrimSpace(pm.ReactionEmoji)
		if emoji != "" {
			return fmt.Sprintf("Reacted %s to %s", emoji, display)
		}
		return fmt.Sprintf("Reacted to %s", display)
	}

	if pm.ReplyToID != "" {
		quoted := strings.TrimSpace(pm.ReplyToDisplay)
		if quoted == "" {
			quoted = a.lookupMessageDisplayTextIn(db, pm.Chat.String(), pm.ReplyToID)
		}
		if quoted == "" {
			quoted = "message"
		}
		if base == "" {
			base = "(message)"
		}
		return fmt.Sprintf("> %s\n%s", quoted, base)
	}

	if base == "" {
		base = "(message)"
	}
	return base
}

func baseDisplayText(pm wa.ParsedMessage) string {
	if pm.Call != nil {
		return callDisplayText(*pm.Call)
	}
	if pm.Media != nil {
		return "Sent " + mediaLabel(pm.Media.Type)
	}
	if text := strings.TrimSpace(pm.Text); text != "" {
		return text
	}
	return ""
}

func callDisplayText(call wa.ParsedCallEvent) string {
	parts := []string{"WhatsApp"}
	if call.Media != "" {
		parts = append(parts, call.Media)
	}
	parts = append(parts, "call")
	if call.Outcome != "" {
		parts = append(parts, call.Outcome)
	} else if call.EventType != "" && call.EventType != "call_log" {
		parts = append(parts, call.EventType)
	}
	if call.DurationSecs > 0 {
		parts = append(parts, fmt.Sprintf("(%s)", formatCallDuration(call.DurationSecs)))
	}
	return strings.Join(parts, " ")
}

func formatCallDuration(seconds int64) string {
	if seconds <= 0 {
		return ""
	}
	minutes := seconds / 60
	secs := seconds % 60
	if minutes <= 0 {
		return fmt.Sprintf("%ds", secs)
	}
	if secs == 0 {
		return fmt.Sprintf("%dm", minutes)
	}
	return fmt.Sprintf("%dm%02ds", minutes, secs)
}

func (a *App) lookupMessageDisplayTextIn(db *store.DB, chatJID, msgID string) string {
	if strings.TrimSpace(chatJID) == "" || strings.TrimSpace(msgID) == "" {
		return ""
	}
	msg, err := db.GetMessage(chatJID, msgID)
	if err != nil {
		return ""
	}
	if text := strings.TrimSpace(msg.DisplayText); text != "" {
		return text
	}
	if text := strings.TrimSpace(msg.Text); text != "" {
		return text
	}
	if strings.TrimSpace(msg.MediaType) != "" {
		return "Sent " + mediaLabel(msg.MediaType)
	}
	return ""
}

func mediaLabel(mediaType string) string {
	mt := strings.ToLower(strings.TrimSpace(mediaType))
	switch mt {
	case "gif":
		return "gif"
	case "image":
		return "image"
	case "video":
		return "video"
	case "audio":
		return "audio"
	case "sticker":
		return "sticker"
	case "document":
		return "document"
	case "location":
		return "location"
	case "contact":
		return "contact"
	case "contacts":
		return "contacts"
	case "":
		return "message"
	default:
		return mt
	}
}
