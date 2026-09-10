package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/wa"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func newMediaEnqueuer(ctx context.Context, jobs chan<- mediaJob) func(chatJID, msgID string) {
	return func(chatJID, msgID string) {
		if strings.TrimSpace(chatJID) == "" || strings.TrimSpace(msgID) == "" {
			return
		}
		select {
		case jobs <- mediaJob{chatJID: chatJID, msgID: msgID}:
		case <-ctx.Done():
		}
	}
}

func (a *App) addSyncEventHandler(ctx context.Context, opts SyncOptions, messagesStored, lastEvent *atomic.Int64, disconnected chan<- struct{}, enqueueMedia func(string, string), enqueueWebhook func(wa.ParsedMessage), enqueueHistory func(historyJob), limits *syncStorageLimits) uint32 {
	var panicCount atomic.Int64
	var appStateRecoveries sync.Map
	return a.wa.AddEventHandler(func(evt interface{}) {
		// Recover from panics so unexpected message structures do not crash the
		// process. Include event type, stack trace, and a running counter.
		defer func() {
			if r := recover(); r != nil {
				n := panicCount.Add(1)
				if a.eventsEnabled() {
					a.emitEvent("event_handler_panic", map[string]any{
						"total": n,
						"event": fmt.Sprintf("%T", evt),
						"panic": fmt.Sprint(r),
						"stack": string(debug.Stack()),
					})
				} else {
					fmt.Fprintf(os.Stderr, "\nevent handler panic (recovered, total=%d) event=%T: %v\n%s\n",
						n, evt, r, debug.Stack())
				}
			}
		}()
		switch v := evt.(type) {
		case *events.Message:
			lastEvent.Store(nowUTC().UnixNano())
			if notif := historySyncNotificationFromMessage(v); notif != nil {
				if notif.GetSyncType() == waE2E.HistorySyncType_ON_DEMAND {
					return
				}
				// Never download or replay a chunk inside the node handler:
				// whatsmeow serialises node handling, so anything slow here
				// stalls (and, after ~5 min, loses) the realtime messages
				// queued behind it. The history worker owns the chunk.
				enqueueHistory(historyJob{notif: notif})
				return
			}
			a.handleLiveSyncMessage(ctx, opts, v, messagesStored, enqueueMedia, enqueueWebhook, limits)
		case *events.CallOffer, *events.CallAccept, *events.CallPreAccept, *events.CallTransport,
			*events.CallOfferNotice, *events.CallRelayLatency, *events.CallTerminate, *events.CallReject,
			*events.AppState:
			lastEvent.Store(nowUTC().UnixNano())
			a.handleLiveCallEvent(ctx, v)
		case *events.HistorySync:
			lastEvent.Store(nowUTC().UnixNano())
			enqueueHistory(historyJob{evt: v})
		case *events.Star:
			lastEvent.Store(nowUTC().UnixNano())
			a.handleStarEvent(ctx, v)
		case *events.DeleteForMe:
			lastEvent.Store(nowUTC().UnixNano())
			a.handleDeleteForMeEvent(ctx, v)
		case *events.Archive, *events.Pin, *events.Mute, *events.MarkChatAsRead:
			lastEvent.Store(nowUTC().UnixNano())
			a.handleChatStateEvent(ctx, v)
		case *events.Connected:
			a.emitOrPrint("connected", nil, "\nConnected.\n")
		case *events.Disconnected:
			a.emitOrPrint("disconnected", nil, "\nDisconnected.\n")
			select {
			case disconnected <- struct{}{}:
			default:
			}
		case *events.StreamReplaced:
			a.emitOrPrint("stream_replaced", nil, "\nStream replaced.\n")
			// whatsmeow emits StreamReplaced before onDisconnect necessarily
			// clears the socket, so force-close before reconnecting.
			a.wa.Close()
			select {
			case disconnected <- struct{}{}:
			default:
			}
		case *events.AppStateSyncError:
			a.handleAppStateSyncError(ctx, v, &appStateRecoveries)
		}
	})
}

func (a *App) handleDeleteForMeEvent(ctx context.Context, evt *events.DeleteForMe) {
	if evt == nil || evt.ChatJID.IsEmpty() || strings.TrimSpace(evt.MessageID) == "" {
		return
	}
	chat := a.canonicalStoreJID(ctx, evt.ChatJID)
	chatJID := canonicalJIDString(chat)
	if err := a.db.UpsertChat(chatJID, chatKind(chat), a.wa.ResolveChatName(ctx, chat, ""), evt.Timestamp); err != nil {
		a.emitWarning(
			"delete_for_me_chat_store_failed",
			fmt.Sprintf("warning: failed to store chat for delete-for-me message %s: %v", evt.MessageID, err),
			map[string]any{"message_id": evt.MessageID, "error": err.Error()},
		)
		return
	}

	senderJID := ""
	if !evt.IsFromMe {
		switch {
		case !evt.SenderJID.IsEmpty():
			senderJID = canonicalJIDString(a.canonicalStoreJID(ctx, evt.SenderJID))
		case chat.Server == types.DefaultUserServer:
			senderJID = chatJID
		}
	}
	if err := a.db.MarkMessageDeletedForMe(chatJID, evt.MessageID, senderJID, evt.IsFromMe, evt.Timestamp); err != nil {
		a.emitWarning(
			"delete_for_me_store_failed",
			fmt.Sprintf("warning: failed to store delete-for-me state for message %s: %v", evt.MessageID, err),
			map[string]any{"message_id": evt.MessageID, "error": err.Error()},
		)
	}
}

func (a *App) handleLiveCallEvent(ctx context.Context, evt interface{}) {
	self := a.linkedLiveCallIdentity()
	var alternateSelf []types.JID
	if _, ok := evt.(*events.AppState); ok {
		identities := a.linkedCallIdentities()
		if len(identities) > 0 {
			self = identities[0]
			alternateSelf = identities[1:]
		}
	}
	call, ok := wa.ParseLiveCallEvent(evt, self, alternateSelf...)
	if ok {
		if err := a.storeParsedCallEvent(ctx, call, "", ""); err != nil {
			a.emitWarning(
				"call_event_store_failed",
				fmt.Sprintf("warning: failed to store call event %s: %v", call.EventType, err),
				map[string]any{"event_type": call.EventType, "call_id": call.CallID, "error": err.Error()},
			)
		}
		return
	}

	deleted, ok := wa.ParseCallLogDeleteEvent(evt)
	if !ok {
		return
	}
	if err := a.deleteParsedCallEvents(ctx, deleted); err != nil {
		a.emitWarning(
			"call_event_delete_failed",
			fmt.Sprintf("warning: failed to delete call log events: %v", err),
			map[string]any{"chat_jid": deleted.Chat.String(), "direction": deleted.Direction, "error": err.Error()},
		)
	}
}

func (a *App) linkedCallIdentities() []types.JID {
	identities := make([]types.JID, 0, 2)
	if linked := strings.TrimSpace(a.wa.LinkedLID()); linked != "" {
		if jid, err := types.ParseJID(linked); err == nil {
			identities = append(identities, jid)
		}
	}
	if linked := strings.TrimSpace(a.wa.LinkedJID()); linked != "" {
		if jid, err := types.ParseJID(linked); err == nil {
			identities = append(identities, jid)
		}
	}
	return identities
}

func (a *App) linkedLiveCallIdentity() types.JID {
	if linked := strings.TrimSpace(a.wa.LinkedJID()); linked != "" {
		if jid, err := types.ParseJID(linked); err == nil {
			return jid
		}
	}
	return types.JID{}
}

func (a *App) handleStarEvent(ctx context.Context, evt *events.Star) {
	if evt == nil || evt.ChatJID.IsEmpty() || strings.TrimSpace(evt.MessageID) == "" || evt.Action == nil {
		return
	}
	senderJID := ""
	if !evt.SenderJID.IsEmpty() {
		senderJID = canonicalJIDString(a.canonicalStoreJID(ctx, evt.SenderJID))
	}
	if err := a.db.SetStarred(store.SetStarredParams{
		ChatJID:   canonicalJIDString(a.canonicalStoreJID(ctx, evt.ChatJID)),
		MsgID:     evt.MessageID,
		SenderJID: senderJID,
		FromMe:    evt.IsFromMe,
		Starred:   evt.Action.GetStarred(),
		StarredAt: evt.Timestamp,
	}); err != nil {
		a.emitWarning(
			"starred_store_failed",
			fmt.Sprintf("warning: failed to store starred state for message %s: %v", evt.MessageID, err),
			map[string]any{"message_id": evt.MessageID, "error": err.Error()},
		)
	}
}

func (a *App) handleAppStateSyncError(ctx context.Context, evt *events.AppStateSyncError, recoveries *sync.Map) {
	if evt == nil || !errors.Is(evt.Error, appstate.ErrMismatchingLTHash) {
		return
	}
	name := strings.TrimSpace(string(evt.Name))
	if name == "" {
		return
	}
	if recoveries == nil {
		recoveries = &sync.Map{}
	}
	if _, loaded := recoveries.LoadOrStore(name, struct{}{}); loaded {
		return
	}

	a.emitWarning(
		"app_state_lthash_mismatch",
		fmt.Sprintf("warning: app state %s hit an LTHash mismatch; requesting recovery snapshot", name),
		map[string]any{"name": name},
	)
	go func() {
		reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		reqID, err := a.wa.RequestAppStateRecovery(reqCtx, name)
		if err != nil {
			a.emitWarning(
				"app_state_recovery_failed",
				fmt.Sprintf("warning: app state %s recovery request failed: %v", name, err),
				map[string]any{"name": name, "error": err.Error()},
			)
			return
		}
		if a.eventsEnabled() {
			a.emitEvent("app_state_recovery_requested", map[string]any{"name": name, "id": string(reqID)})
		} else {
			fmt.Fprintf(os.Stderr, "\rRequested app state %s recovery (id %s)\n", name, reqID)
		}
	}()
}

func (a *App) handleLiveSyncMessage(ctx context.Context, opts SyncOptions, v *events.Message, messagesStored *atomic.Int64, enqueueMedia func(string, string), enqueueWebhook func(wa.ParsedMessage), limits ...*syncStorageLimits) {
	if historySyncNotificationFromMessage(v) != nil {
		return
	}
	pm := wa.ParseLiveMessage(v)
	if pm.ReactionToID != "" && pm.ReactionEmoji == "" && v.Message != nil && v.Message.GetEncReactionMessage() != nil {
		a.decryptEncryptedReaction(ctx, &pm, v)
	}
	incrementUnread := a.shouldIncrementLiveUnread(ctx, pm)
	attempts, err := retryOnBusy(ctx, liveStoreRetryDelays, func() error {
		return a.storeParsedMessageForSync(ctx, pm, limits...)
	})
	if err != nil {
		// A realtime message that fails to store is gone for good (the server
		// will not resend it), so never fail silently.
		chatJID := canonicalJIDString(a.canonicalStoreJID(ctx, pm.Chat))
		a.emitWarning(
			"live_store_failed",
			fmt.Sprintf("warning: failed to store live message %s in chat %s after %d attempt(s): %v", pm.ID, chatJID, attempts, err),
			map[string]any{"chat_jid": chatJID, "message_id": pm.ID, "attempts": attempts, "error": err.Error()},
		)
	} else {
		if incrementUnread {
			a.incrementLiveUnread(ctx, pm)
		}
		a.emitSyncProgress(messagesStored.Add(1))
		if enqueueWebhook != nil {
			enqueueWebhook(pm)
		}
		sideEffectCtx := ctx
		if ctx.Err() != nil {
			sideEffectCtx = context.WithoutCancel(ctx)
		}
		a.handlePollSideEffects(sideEffectCtx, pm, v)
	}
	if opts.DownloadMedia && pm.Media != nil && pm.ID != "" {
		enqueueMedia(canonicalJIDString(a.canonicalStoreJID(ctx, pm.Chat)), pm.ID)
	}
}

func (a *App) downloadAndHandleHistorySync(ctx context.Context, opts SyncOptions, notif *waE2E.HistorySyncNotification, messagesStored, lastEvent *atomic.Int64, enqueueMedia func(string, string), limits ...*syncStorageLimits) {
	stopKeepAlive := keepLastEventAlive(lastEvent, 5*time.Second)
	data, err := a.wa.DownloadHistorySync(ctx, notif)
	stopKeepAlive()
	lastEvent.Store(nowUTC().UnixNano())
	if err != nil {
		a.emitWarning(
			"history_download_failed",
			fmt.Sprintf("warning: failed to download history sync: %v", err),
			map[string]any{"error": err.Error()},
		)
		return
	}
	a.handleHistorySync(ctx, opts, &events.HistorySync{Data: data}, messagesStored, lastEvent, enqueueMedia, limits...)
	if err := a.wa.DeleteHistorySyncMedia(ctx, notif); err != nil {
		a.emitWarning(
			"history_delete_failed",
			fmt.Sprintf("warning: failed to delete history sync media: %v", err),
			map[string]any{"error": err.Error()},
		)
	}
}

func historySyncNotificationFromMessage(v *events.Message) *waE2E.HistorySyncNotification {
	if v == nil || v.Message == nil {
		return nil
	}
	return v.Message.GetProtocolMessage().GetHistorySyncNotification()
}

// liveStoreRetryDelays is the backoff used when a realtime store hits SQLite
// lock contention (e.g. a history batch commit in progress).
var liveStoreRetryDelays = []time.Duration{200 * time.Millisecond, 800 * time.Millisecond, 2 * time.Second}

// retryOnBusy runs fn, retrying after each delay in delays while the error is
// a transient SQLite busy/locked error. Any other error (or success) returns
// immediately. It reports the number of attempts made.
func retryOnBusy(ctx context.Context, delays []time.Duration, fn func() error) (attempts int, err error) {
	for i := 0; ; i++ {
		attempts = i + 1
		err = fn()
		if err == nil || i >= len(delays) || !store.IsBusyError(err) {
			return attempts, err
		}
		timer := time.NewTimer(delays[i])
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return attempts, err
		}
	}
}

func (a *App) handleHistorySync(ctx context.Context, opts SyncOptions, v *events.HistorySync, messagesStored, lastEvent *atomic.Int64, enqueueMedia func(string, string), limits ...*syncStorageLimits) {
	var lim *syncStorageLimits
	if len(limits) > 0 {
		lim = limits[0]
	}
	a.emitOrPrint("history_sync", map[string]any{"conversations": len(v.Data.Conversations)}, "\nProcessing history sync (%d conversations)...\n", len(v.Data.Conversations))
	a.storeHistoryCallLogRecords(ctx, v, lastEvent)
	var skipped int64
	aborted := false
	for i, conv := range v.Data.Conversations {
		lastEvent.Store(nowUTC().UnixNano())
		chatID := strings.TrimSpace(conv.GetID())
		if chatID == "" {
			continue
		}
		// Shutdown: finish the conversation in progress, do not start the next.
		if i > 0 && ctx.Err() != nil {
			aborted = true
			break
		}
		a.storeHistoryUnreadCount(ctx, chatID, conv)
		res := a.storeHistoryConversation(ctx, opts, chatID, conv, messagesStored, lastEvent, enqueueMedia, lim)
		skipped += res.skipped
		if res.aborted {
			aborted = true
			break
		}
	}
	totalSkipped := a.historySkipped.Add(skipped)
	if aborted {
		return
	}
	// One progress heartbeat per chunk (in every output mode) so the caller
	// can see replay advancing even when every message was already stored.
	a.emitOrPrint("progress", map[string]any{
		"messages_synced":  messagesStored.Load(),
		"messages_skipped": totalSkipped,
	}, "\rSynced %d messages (%d already stored)...", messagesStored.Load(), totalSkipped)
}

func (a *App) storeHistoryCallLogRecords(ctx context.Context, v *events.HistorySync, lastEvent *atomic.Int64) {
	if v == nil || v.Data == nil {
		return
	}
	identities := a.linkedCallIdentities()
	self := types.JID{}
	var alternateSelf []types.JID
	if len(identities) > 0 {
		self = identities[0]
		alternateSelf = identities[1:]
	}
	for _, record := range v.Data.GetCallLogRecords() {
		lastEvent.Store(nowUTC().UnixNano())
		call, ok := wa.ParseCallLogRecord(record, self, alternateSelf...)
		if !ok {
			continue
		}
		if err := a.storeParsedCallEvent(ctx, call, "", ""); err != nil {
			a.emitWarning(
				"history_call_log_store_failed",
				fmt.Sprintf("warning: failed to store history call log %s: %v", call.CallID, err),
				map[string]any{"call_id": call.CallID, "error": err.Error()},
			)
		}
	}
}

func (a *App) incrementLiveUnread(ctx context.Context, pm wa.ParsedMessage) {
	chat := a.canonicalStoreJID(ctx, pm.Chat)
	if err := a.db.IncrementChatUnread(canonicalJIDString(chat)); err != nil {
		a.emitWarning(
			"live_unread_store_failed",
			fmt.Sprintf("warning: failed to increment unread count for chat %s: %v", chat, err),
			map[string]any{"chat_jid": chat.String(), "error": err.Error()},
		)
	}
}

func (a *App) shouldIncrementLiveUnread(ctx context.Context, pm wa.ParsedMessage) bool {
	if pm.FromMe || pm.ID == "" || pm.Chat.IsEmpty() || pm.Chat == types.StatusBroadcastJID {
		return false
	}
	chat := canonicalJIDString(a.canonicalStoreJID(ctx, pm.Chat))
	if chat == "" {
		return false
	}
	_, err := a.db.GetMessage(chat, pm.ID)
	return errors.Is(err, sql.ErrNoRows)
}

func (a *App) storeHistoryUnreadCount(ctx context.Context, chatID string, conv *waHistorySync.Conversation) {
	if conv == nil || (conv.UnreadCount == nil && conv.MarkedAsUnread == nil) {
		return
	}
	chat, err := types.ParseJID(chatID)
	if err != nil || chat.IsEmpty() {
		return
	}
	count := int(conv.GetUnreadCount())
	chat = a.canonicalStoreJID(ctx, chat)
	var storeErr error
	if count > 0 {
		storeErr = a.db.SetChatUnreadCount(canonicalJIDString(chat), count)
	} else if conv.GetMarkedAsUnread() {
		storeErr = a.db.SetChatUnread(canonicalJIDString(chat), true)
	} else {
		storeErr = a.db.SetChatUnreadCount(canonicalJIDString(chat), 0)
	}
	if storeErr != nil {
		a.emitWarning(
			"history_unread_store_failed",
			fmt.Sprintf("warning: failed to store unread count for chat %s: %v", chat, storeErr),
			map[string]any{"chat_jid": chat.String(), "unread_count": count, "error": storeErr.Error()},
		)
	}
}

func (a *App) emitSyncProgress(total int64) {
	if total <= 0 || total%25 != 0 {
		return
	}
	a.emitOrPrint("progress", map[string]any{"messages_synced": total}, "\rSynced %d messages...", total)
}

func (a *App) storeParsedMessageForSync(ctx context.Context, pm wa.ParsedMessage, limits ...*syncStorageLimits) error {
	if len(limits) > 0 && limits[0] != nil {
		return limits[0].StoreParsedMessage(ctx, pm)
	}
	return a.storeParsedMessage(ctx, pm)
}

func (a *App) decryptEncryptedReaction(ctx context.Context, pm *wa.ParsedMessage, msg *events.Message) {
	reaction, err := a.wa.DecryptReaction(ctx, msg)
	if err != nil {
		a.emitWarning(
			"encrypted_reaction_decrypt_failed",
			fmt.Sprintf("warning: failed to decrypt reaction message %s: %v", pm.ID, err),
			map[string]any{"message_id": pm.ID, "error": err.Error()},
		)
		return
	}
	if reaction == nil {
		return
	}
	pm.ReactionEmoji = reaction.GetText()
	if pm.ReactionToID == "" {
		if key := reaction.GetKey(); key != nil {
			pm.ReactionToID = key.GetID()
		}
	}
}
