package app

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/wa"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	// historyQueueSize bounds the number of history chunks waiting for the
	// worker. Notification jobs are small (the blob is downloaded lazily by the
	// worker), so this can be generous; whatsmeow's own queue is 32.
	historyQueueSize = 256
	// historyBatchSize is the number of message rows committed per
	// transaction while replaying one conversation. Committing between batches
	// lets realtime writes interleave.
	historyBatchSize = 200
)

// historyJob is one unit of work for the history worker: either a history
// sync notification that still has to be downloaded, or a chunk that was
// already downloaded and dispatched as events.HistorySync.
type historyJob struct {
	notif *waE2E.HistorySyncNotification
	evt   *events.HistorySync
}

// runHistoryWorker starts the single goroutine that replays history chunks and
// returns the enqueue function for the event handler plus a stop function.
//
// Why a worker: whatsmeow's handlerQueueLoop processes nodes one at a time and
// waits (up to 10 x 30s) for each handler. Processing a history chunk inline
// took >5 minutes per chunk, every realtime message node behind it waited
// undecrypted and unacked, and the server closed the socket every ~5 minutes,
// dropping the queued nodes. After a relink on 2026-09-02 that lost ~150k
// realtime messages over 14h. The handler now only enqueues; the worker does
// the download and the store work.
//
// Shutdown: stop cancels the worker context and waits. The worker finishes the
// conversation it is writing (under context.WithoutCancel) and discards the
// rest of the queue. Those chunks are NOT re-sent: whatsmeow acked their
// notification when it was decrypted, so a shutdown mid-replay loses them
// (same as the old inline path being killed) and only `history backfill` can
// ask for that range again.
func (a *App) runHistoryWorker(ctx context.Context, opts SyncOptions, messagesStored, lastEvent *atomic.Int64, enqueueMedia func(string, string), limits *syncStorageLimits) (enqueue func(historyJob), stop func()) {
	jobs := make(chan historyJob, historyQueueSize)
	workerCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-workerCtx.Done():
				return
			case job := <-jobs:
				if workerCtx.Err() != nil {
					return
				}
				lastEvent.Store(nowUTC().UnixNano())
				a.processHistoryJob(workerCtx, opts, job, messagesStored, lastEvent, enqueueMedia, limits)
			}
		}
	}()
	enqueue = func(job historyJob) {
		if job.notif == nil && job.evt == nil {
			return
		}
		select {
		case jobs <- job:
		case <-workerCtx.Done():
		}
	}
	stop = func() {
		cancel()
		wg.Wait()
	}
	return enqueue, stop
}

func (a *App) processHistoryJob(ctx context.Context, opts SyncOptions, job historyJob, messagesStored, lastEvent *atomic.Int64, enqueueMedia func(string, string), limits *syncStorageLimits) {
	// Recover per chunk so a malformed chunk cannot kill the worker (the media
	// and webhook workers do the same).
	defer func() {
		if r := recover(); r != nil {
			if a.eventsEnabled() {
				a.emitEvent("history_worker_panic", map[string]any{
					"panic": fmt.Sprint(r),
					"stack": string(debug.Stack()),
				})
			} else {
				fmt.Fprintf(os.Stderr, "history worker panic (recovered): %v\n%s\n", r, debug.Stack())
			}
		}
	}()
	switch {
	case job.notif != nil:
		a.downloadAndHandleHistorySync(ctx, opts, job.notif, messagesStored, lastEvent, enqueueMedia, limits)
	case job.evt != nil && job.evt.Data != nil:
		a.handleHistorySync(ctx, opts, job.evt, messagesStored, lastEvent, enqueueMedia, limits)
	}
}

// keepLastEventAlive bumps lastEvent every interval until the returned stop
// function is called. It is used only around the chunk download: a large chunk
// on a slow link can take longer than the bootstrap idle-exit (30s), and an
// idle exit mid-download would cancel the download and lose the chunk. It is
// deliberately not used around the store work, so a wedged store still looks
// idle to the watchdog.
func keepLastEventAlive(lastEvent *atomic.Int64, interval time.Duration) (stop func()) {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				lastEvent.Store(nowUTC().UnixNano())
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

// historyConversationResult summarises one replayed conversation.
type historyConversationResult struct {
	stored  int64
	skipped int64
	aborted bool
}

type historyEntry struct {
	pm     wa.ParsedMessage
	params store.UpsertMessageParams
	poll   *historyPollSideEffect
}

type senderKey struct {
	sender   string
	pushName string
	fromMe   bool
}

type senderResolution struct {
	jid, name string
	contact   *contactRecord
}

// historyConversationWriter accumulates the rows of one conversation and
// commits them in batches.
type historyConversationWriter struct {
	app            *App
	opts           SyncOptions
	limits         *syncStorageLimits
	batchSize      int
	messagesStored *atomic.Int64
	lastEvent      *atomic.Int64
	enqueueMedia   func(string, string)

	chatJID  string
	chatName string
	chat     types.JID

	entries      []historyEntry
	contacts     []contactRecord
	seenContacts map[string]struct{}
	senders      map[senderKey]senderResolution
	canonical    map[types.JID]types.JID
	polls        []historyPollSideEffect
	stored       int64
}

func (w *historyConversationWriter) canonicalChat(ctx context.Context, jid types.JID) types.JID {
	if c, ok := w.canonical[jid]; ok {
		return c
	}
	c := w.app.canonicalStoreJID(ctx, jid)
	w.canonical[jid] = c
	return c
}

func (w *historyConversationWriter) resolveSender(ctx context.Context, pm wa.ParsedMessage) senderResolution {
	key := senderKey{sender: pm.SenderJID, pushName: pm.PushName, fromMe: pm.FromMe}
	if r, ok := w.senders[key]; ok {
		return r
	}
	jid, name, contact := w.app.resolveSender(ctx, pm)
	r := senderResolution{jid: jid, name: name, contact: contact}
	w.senders[key] = r
	if contact != nil {
		if _, seen := w.seenContacts[contact.jid]; !seen {
			w.seenContacts[contact.jid] = struct{}{}
			w.contacts = append(w.contacts, *contact)
		}
	}
	return r
}

func (w *historyConversationWriter) add(e historyEntry) {
	w.entries = append(w.entries, e)
}

func (w *historyConversationWriter) full() bool {
	return len(w.entries) >= w.batchSize
}

// flush commits the pending rows in one transaction. When the transaction
// fails the rows are retried one by one so a single poison row cannot drop a
// whole batch. Side rows (call events, starred state), poll side effects,
// progress accounting and media enqueueing happen after the commit.
func (w *historyConversationWriter) flush(ctx context.Context) error {
	if len(w.entries) == 0 {
		return nil
	}
	if err := w.limits.check(); err != nil {
		return err
	}
	a := w.app
	entries := w.entries
	contacts := w.contacts
	w.entries = nil
	w.contacts = nil

	committed := make([]bool, len(entries))
	err := a.db.WithTx(func(tx *store.DB) error {
		for _, c := range contacts {
			a.upsertContactRecord(tx, c)
		}
		for _, e := range entries {
			// Display text of replies/reactions quotes the target row, which
			// may have been written earlier in this transaction.
			p := e.params
			p.DisplayText = a.finalDisplayText(tx, e.pm)
			if err := tx.UpsertMessage(p); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		for i := range committed {
			committed[i] = true
		}
	} else {
		a.emitWarning(
			"history_batch_store_failed",
			fmt.Sprintf("warning: batched history write for chat %s failed (%d rows), retrying rows individually: %v", w.chatJID, len(entries), err),
			map[string]any{"chat_jid": w.chatJID, "rows": len(entries), "error": err.Error()},
		)
		for _, c := range contacts {
			a.upsertContactRecord(a.db, c)
		}
		for i, e := range entries {
			p := e.params
			p.DisplayText = a.finalDisplayText(a.db, e.pm)
			committed[i] = a.db.UpsertMessage(p) == nil
		}
	}

	for i, e := range entries {
		if !committed[i] {
			continue
		}
		w.lastEvent.Store(nowUTC().UnixNano())
		w.stored++
		a.emitSyncProgress(w.messagesStored.Add(1))
		if err := a.storeMessageExtras(ctx, e.pm, w.chatJID, w.chatName, e.params.SenderJID, e.params.SenderName); err != nil {
			a.emitWarning(
				"history_message_extras_failed",
				fmt.Sprintf("warning: failed to store side rows for history message %s: %v", e.pm.ID, err),
				map[string]any{"chat_jid": w.chatJID, "message_id": e.pm.ID, "error": err.Error()},
			)
		}
		if e.poll != nil {
			w.polls = append(w.polls, *e.poll)
		}
		if w.opts.DownloadMedia && e.pm.Media != nil && e.pm.ID != "" {
			w.enqueueMedia(w.chatJID, e.pm.ID)
		}
	}
	_ = w.limits.check()
	return nil
}

// storeHistoryConversation replays one history conversation. Messages that are
// already in the store are skipped before any name/contact/network work;
// edits and revokes are never skipped because they update an existing row.
//
// Cancellation: a conversation that has started is always finished (the
// store writes do not depend on ctx, and the remaining work is a bounded
// number of local batches), so a shutdown never leaves a conversation half
// written. Only a latched storage-limit breach aborts mid-conversation, which
// keeps --max-messages exact. Callers check ctx between conversations.
func (a *App) storeHistoryConversation(ctx context.Context, opts SyncOptions, chatID string, conv *waHistorySync.Conversation, messagesStored, lastEvent *atomic.Int64, enqueueMedia func(string, string), limits *syncStorageLimits) historyConversationResult {
	w := &historyConversationWriter{
		app:            a,
		opts:           opts,
		limits:         limits,
		batchSize:      historyBatchSize,
		messagesStored: messagesStored,
		lastEvent:      lastEvent,
		enqueueMedia:   enqueueMedia,
		seenContacts:   map[string]struct{}{},
		senders:        map[senderKey]senderResolution{},
		canonical:      map[types.JID]types.JID{},
	}
	if limits.enabled() {
		// Storage caps are checked around every row so the sync stops at
		// exactly the configured count, as it did before batching.
		w.batchSize = 1
	}
	var res historyConversationResult
	var maxTS time.Time
	chatReady := false

	finish := func() historyConversationResult {
		flushCtx := ctx
		if ctx.Err() != nil {
			flushCtx = context.WithoutCancel(ctx)
		}
		if err := w.flush(flushCtx); err != nil && limits.Err() != nil {
			res.aborted = true
		}
		if chatReady && !maxTS.IsZero() {
			_ = a.db.UpsertChat(w.chatJID, chatKind(w.chat), w.chatName, maxTS)
		}
		a.handleHistoryPollSideEffectsBatch(flushCtx, w.polls)
		res.stored = w.stored
		return res
	}

	for _, m := range conv.Messages {
		lastEvent.Store(nowUTC().UnixNano())
		if m.Message == nil {
			continue
		}
		if limits.Err() != nil {
			res.aborted = true
			return finish()
		}
		pm := wa.ParseHistoryMessage(chatID, m.Message)
		if pm.ID == "" || pm.Chat.IsEmpty() {
			continue
		}
		pm.Chat = w.canonicalChat(ctx, pm.Chat)
		chatJID := canonicalJIDString(pm.Chat)

		if pm.Chat == types.StatusBroadcastJID {
			// Status updates live in their own table; keep the per-message path.
			if err := a.storeParsedMessageForSync(ctx, pm, limits); err == nil {
				res.stored++
				a.emitSyncProgress(messagesStored.Add(1))
			} else if limits.Err() != nil {
				res.aborted = true
				return finish()
			}
			if opts.DownloadMedia && pm.Media != nil && pm.ID != "" {
				enqueueMedia(chatJID, pm.ID)
			}
			continue
		}

		if !pm.Edited && !pm.Revoked {
			if exists, err := a.db.MessageExists(chatJID, pm.ID); err == nil && exists {
				res.skipped++
				continue
			}
		}

		var pollEvt *events.Message
		if normalized, evt, ok := a.normalizeHistoryPollMessage(pm, m.Message); ok {
			pm = normalized
			pollEvt = evt
		}
		if pm.ReactionToID != "" && pm.ReactionEmoji == "" && m.Message.GetMessage().GetEncReactionMessage() != nil {
			evt, err := a.wa.ParseWebMessage(pm.Chat, m.Message)
			if err != nil {
				a.emitWarning(
					"encrypted_reaction_parse_failed",
					fmt.Sprintf("warning: failed to parse encrypted reaction message %s: %v", pm.ID, err),
					map[string]any{"message_id": pm.ID, "error": err.Error()},
				)
			} else {
				a.decryptEncryptedReaction(ctx, &pm, evt)
			}
		}

		if !chatReady || chatJID != w.chatJID {
			if chatReady {
				// Conversation switched chats (should not happen); commit what we have.
				if err := w.flush(ctx); err != nil && limits.Err() != nil {
					res.aborted = true
					return finish()
				}
			}
			w.chat = pm.Chat
			w.chatJID = chatJID
			w.chatName = a.resolveChatName(ctx, pm.Chat, pm.PushName)
			if err := a.db.UpsertChat(chatJID, chatKind(pm.Chat), w.chatName, pm.Timestamp); err != nil {
				if limits.Err() != nil {
					res.aborted = true
					return finish()
				}
				continue
			}
			if rec, ok := a.dmContact(ctx, pm.Chat); ok {
				a.upsertContactRecord(a.db, rec)
			}
			a.ensureGroupStored(ctx, pm.Chat)
			chatReady = true
			maxTS = pm.Timestamp
		} else if w.chatName == pm.Chat.String() {
			// First message may have had no usable push name; try again once one appears.
			if s := strings.TrimSpace(pm.PushName); s != "" && s != "-" && !pm.FromMe {
				w.chatName = a.resolveChatName(ctx, pm.Chat, pm.PushName)
			}
		}
		if pm.Timestamp.After(maxTS) {
			maxTS = pm.Timestamp
		}

		sender := w.resolveSender(ctx, pm)
		entry := historyEntry{
			pm:     pm,
			params: buildUpsertMessageParams(pm, chatJID, w.chatName, sender.jid, sender.name),
		}
		if pm.Poll != nil || pm.PollAdd != nil || pm.PollVote != nil {
			entry.poll = &historyPollSideEffect{pm: pm, evt: pollEvt, hist: m.Message}
		}
		w.add(entry)
		if w.full() {
			if err := w.flush(ctx); err != nil && limits.Err() != nil {
				res.aborted = true
				return finish()
			}
		}
	}
	return finish()
}
