package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/wa"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestHistorySyncSkipsAlreadyStoredMessages(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if err := a.db.UpsertChat(chat.String(), "dm", "Alice", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(store.UpsertMessageParams{ChatJID: chat.String(), MsgID: "m1", Timestamp: base, Text: "old copy", DisplayText: "old copy"}); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	var messagesStored, lastEvent atomic.Int64
	a.handleHistorySync(context.Background(), SyncOptions{}, historySyncWithTextMessages(chat, base, "m1", "m2", "m3"), &messagesStored, &lastEvent, func(string, string) {})

	if got := messagesStored.Load(); got != 2 {
		t.Fatalf("messagesStored = %d, want 2 (m1 skipped)", got)
	}
	if got := a.historySkipped.Load(); got != 1 {
		t.Fatalf("historySkipped = %d, want 1", got)
	}
	msg, err := a.db.GetMessage(chat.String(), "m1")
	if err != nil || msg.Text != "old copy" {
		t.Fatalf("existing row should be untouched: %+v err=%v", msg, err)
	}
	if n, _ := a.db.CountMessages(); n != 3 {
		t.Fatalf("db messages = %d, want 3", n)
	}
	if lastEvent.Load() == 0 {
		t.Fatalf("lastEvent not updated by history replay")
	}
}

func TestHistorySyncDoesNotSkipEditsOfStoredMessages(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	var messagesStored, lastEvent atomic.Int64
	a.handleHistorySync(context.Background(), SyncOptions{}, historySyncWithTextMessages(chat, base, "original-id"), &messagesStored, &lastEvent, func(string, string) {})

	editMsg := &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{
			RemoteJID: proto.String(chat.String()),
			FromMe:    proto.Bool(false),
			ID:        proto.String("edit-event"),
		},
		MessageTimestamp: proto.Uint64(uint64(base.Add(time.Minute).Unix())),
		Message: &waProto.Message{
			ProtocolMessage: &waProto.ProtocolMessage{
				Type: waProto.ProtocolMessage_MESSAGE_EDIT.Enum(),
				Key: &waCommon.MessageKey{
					RemoteJID: proto.String(chat.String()),
					FromMe:    proto.Bool(false),
					ID:        proto.String("original-id"),
				},
				EditedMessage: &waProto.Message{Conversation: proto.String("edited body")},
			},
		},
	}
	replay := &events.HistorySync{Data: &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_FULL.Enum(),
		Conversations: []*waHistorySync.Conversation{{
			ID:       proto.String(chat.String()),
			Messages: []*waHistorySync.HistorySyncMsg{{Message: editMsg}},
		}},
	}}
	a.historySkipped.Store(0)
	a.handleHistorySync(context.Background(), SyncOptions{}, replay, &messagesStored, &lastEvent, func(string, string) {})

	if got := a.historySkipped.Load(); got != 0 {
		t.Fatalf("edit was skipped (historySkipped=%d)", got)
	}
	msg, err := a.db.GetMessage(chat.String(), "original-id")
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg.Text != "edited body" {
		t.Fatalf("edit not applied: %+v", msg)
	}
}

func TestHistorySyncBatchesConversationAcrossTransactions(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	n := historyBatchSize*2 + 37
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("m%04d", i)
	}
	var messagesStored, lastEvent atomic.Int64
	media := 0
	a.handleHistorySync(context.Background(), SyncOptions{}, historySyncWithTextMessages(chat, base, ids...), &messagesStored, &lastEvent, func(string, string) { media++ })

	if got := messagesStored.Load(); got != int64(n) {
		t.Fatalf("messagesStored = %d, want %d", got, n)
	}
	if count, _ := a.db.CountMessages(); count != int64(n) {
		t.Fatalf("db messages = %d, want %d", count, n)
	}
	if media != 0 {
		t.Fatalf("media enqueued for text messages: %d", media)
	}
	chatRow, err := a.db.GetChat(chat.String())
	if err != nil {
		t.Fatalf("GetChat: %v", err)
	}
	wantTS := base.Add(time.Duration(n-1) * time.Second)
	if !chatRow.LastMessageTS.Equal(wantTS) {
		t.Fatalf("chat last_message_ts = %s, want %s", chatRow.LastMessageTS, wantTS)
	}
}

func TestHistoryWorkerDoesNotBlockEventHandler(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "555", Server: types.DefaultUserServer}
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	f.downloadHistory = func(notif *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		started <- struct{}{}
		<-release
		return historySyncWithTextMessages(chat, base, "m-slow").Data, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var messagesStored, lastEvent atomic.Int64
	enqueueHistory, stop := a.runHistoryWorker(ctx, SyncOptions{}, &messagesStored, &lastEvent, func(string, string) {}, nil)
	handlerID := a.addSyncEventHandler(ctx, SyncOptions{}, &messagesStored, &lastEvent, make(chan struct{}, 1), func(string, string) {}, func(wa.ParsedMessage) {}, enqueueHistory, nil)
	defer a.wa.RemoveEventHandler(handlerID)

	syncType := waE2E.HistorySyncType_RECENT
	notif := &waE2E.HistorySyncNotification{SyncType: &syncType}
	done := make(chan struct{})
	go func() {
		f.emit(&events.Message{Message: &waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{HistorySyncNotification: notif}}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("event handler blocked on history chunk")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("worker did not pick up the chunk")
	}
	// A realtime message is handled while the chunk is still downloading.
	live := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            "m-live",
			Timestamp:     base.Add(time.Hour),
			PushName:      "Alice",
		},
		Message: &waProto.Message{Conversation: proto.String("hello")},
	}
	f.emit(live)
	if ok, _ := a.db.MessageExists(chat.String(), "m-live"); !ok {
		t.Fatalf("live message not stored while history chunk pending")
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ok, _ := a.db.MessageExists(chat.String(), "m-slow"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("history chunk not stored by worker after release")
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	if got := f.deleteHistoryCalls; len(got) != 1 || got[0] != notif {
		t.Fatalf("delete history calls = %v", got)
	}
	if messagesStored.Load() != 2 {
		t.Fatalf("messagesStored = %d, want 2", messagesStored.Load())
	}
}

func TestRetryOnBusy(t *testing.T) {
	busy := sqlite3.Error{Code: sqlite3.ErrBusy}
	calls := 0
	attempts, err := retryOnBusy(context.Background(), []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}, func() error {
		calls++
		if calls < 3 {
			return busy
		}
		return nil
	})
	if err != nil || attempts != 3 || calls != 3 {
		t.Fatalf("busy then success: attempts=%d calls=%d err=%v", attempts, calls, err)
	}

	calls = 0
	attempts, err = retryOnBusy(context.Background(), []time.Duration{time.Millisecond, time.Millisecond}, func() error {
		calls++
		return busy
	})
	if !errors.Is(err, busy) || attempts != 3 || calls != 3 {
		t.Fatalf("always busy: attempts=%d calls=%d err=%v", attempts, calls, err)
	}

	calls = 0
	other := errors.New("constraint failed")
	attempts, err = retryOnBusy(context.Background(), []time.Duration{time.Millisecond, time.Millisecond}, func() error {
		calls++
		return other
	})
	if !errors.Is(err, other) || attempts != 1 || calls != 1 {
		t.Fatalf("non-busy error retried: attempts=%d calls=%d err=%v", attempts, calls, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls = 0
	attempts, err = retryOnBusy(ctx, []time.Duration{time.Hour}, func() error {
		calls++
		return busy
	})
	if !errors.Is(err, busy) || attempts != 1 || calls != 1 {
		t.Fatalf("cancelled ctx should stop retrying: attempts=%d calls=%d err=%v", attempts, calls, err)
	}
}

func TestLiveSyncWarnsWhenStoreFails(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f
	// Closing the store makes every write fail with a non-transient error.
	_ = a.db.Close()

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            "m-fail",
			Timestamp:     time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
			PushName:      "Alice",
		},
		Message: &waProto.Message{Conversation: proto.String("hello")},
	}
	var messagesStored atomic.Int64
	out := captureStderr(t, func() {
		a.handleLiveSyncMessage(context.Background(), SyncOptions{}, evt, &messagesStored, func(string, string) {}, nil)
	})
	if !strings.Contains(out, "warning: failed to store live message m-fail in chat "+chat.String()) {
		t.Fatalf("expected live_store_failed warning, got:\n%s", out)
	}
	if messagesStored.Load() != 0 {
		t.Fatalf("messagesStored = %d, want 0", messagesStored.Load())
	}
}

func TestGroupInfoCachedPerRun(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	group := types.JID{User: "120363000000000001", Server: types.GroupServer}
	member := types.JID{User: "111", Server: types.DefaultUserServer}
	f.groups[group] = &types.GroupInfo{
		JID:       group,
		GroupName: types.GroupName{Name: "Ops"},
		Participants: []types.GroupParticipant{
			{JID: member, IsAdmin: true},
		},
	}

	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	ids := []string{"g1", "g2", "g3"}
	var messagesStored, lastEvent atomic.Int64
	a.handleHistorySync(context.Background(), SyncOptions{}, historySyncWithTextMessages(group, base, ids...), &messagesStored, &lastEvent, func(string, string) {})
	if messagesStored.Load() != 3 {
		t.Fatalf("messagesStored = %d, want 3", messagesStored.Load())
	}
	if f.groupInfoCalls != 1 {
		t.Fatalf("GetGroupInfo calls after history replay = %d, want 1", f.groupInfoCalls)
	}
	msg, err := a.db.GetMessage(group.String(), "g1")
	if err != nil || msg.ChatName != "Ops" {
		t.Fatalf("group chat name not resolved through cache: %+v err=%v", msg, err)
	}

	// Live messages for the same group reuse the entry.
	live := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: group, Sender: member, IsGroup: true},
			ID:            "g-live",
			Timestamp:     base.Add(time.Hour),
			PushName:      "Bob",
		},
		Message: &waProto.Message{Conversation: proto.String("hi")},
	}
	a.handleLiveSyncMessage(context.Background(), SyncOptions{}, live, &messagesStored, func(string, string) {}, nil)
	if f.groupInfoCalls != 1 {
		t.Fatalf("GetGroupInfo calls after live message = %d, want 1", f.groupInfoCalls)
	}
	groups, err := a.db.ListGroups("", 10)
	if err != nil || len(groups) != 1 || groups[0].Name != "Ops" {
		t.Fatalf("group metadata not stored: %+v err=%v", groups, err)
	}

	// A new run starts cold.
	a.groups.reset()
	a.handleLiveSyncMessage(context.Background(), SyncOptions{}, live, &messagesStored, func(string, string) {}, nil)
	if f.groupInfoCalls != 2 {
		t.Fatalf("GetGroupInfo calls after reset = %d, want 2", f.groupInfoCalls)
	}
}

func TestGroupInfoCacheTTLAndErrors(t *testing.T) {
	c := newGroupInfoCache(10*time.Minute, time.Minute)
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }

	fetches := 0
	fetch := func() (*types.GroupInfo, error) {
		fetches++
		return &types.GroupInfo{GroupName: types.GroupName{Name: "n"}}, nil
	}
	if _, err := c.get(context.Background(), "g", fetch); err != nil || fetches != 1 {
		t.Fatalf("first get: fetches=%d err=%v", fetches, err)
	}
	if _, _ = c.get(context.Background(), "g", fetch); fetches != 1 {
		t.Fatalf("second get refetched: %d", fetches)
	}
	now = now.Add(11 * time.Minute)
	if _, _ = c.get(context.Background(), "g", fetch); fetches != 2 {
		t.Fatalf("expired entry not refetched: %d", fetches)
	}

	// Errors are cached for the shorter TTL only.
	errFetches := 0
	errFetch := func() (*types.GroupInfo, error) {
		errFetches++
		return nil, errors.New("not-authorized")
	}
	if _, err := c.get(context.Background(), "gone", errFetch); err == nil || errFetches != 1 {
		t.Fatalf("error fetch: %d %v", errFetches, err)
	}
	if _, _ = c.get(context.Background(), "gone", errFetch); errFetches != 1 {
		t.Fatalf("error not cached: %d", errFetches)
	}
	now = now.Add(2 * time.Minute)
	if _, _ = c.get(context.Background(), "gone", errFetch); errFetches != 2 {
		t.Fatalf("expired error not refetched: %d", errFetches)
	}

	// Context errors are never memoised.
	ctxFetches := 0
	ctxFetch := func() (*types.GroupInfo, error) {
		ctxFetches++
		return nil, context.Canceled
	}
	_, _ = c.get(context.Background(), "ctx", ctxFetch)
	_, _ = c.get(context.Background(), "ctx", ctxFetch)
	if ctxFetches != 2 {
		t.Fatalf("context error was cached: %d", ctxFetches)
	}

	// markStored wins exactly once per entry.
	e, _ := c.get(context.Background(), "g", fetch)
	if !c.markStored(e) || c.markStored(e) {
		t.Fatalf("markStored should succeed once")
	}
}

func TestHistoryConversationFinishesAfterCancel(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	n := historyBatchSize + 5
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("c%04d", i)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var messagesStored, lastEvent atomic.Int64
	conv := historySyncWithTextMessages(chat, base, ids...).Data.Conversations[0]
	res := a.storeHistoryConversation(ctx, SyncOptions{}, chat.String(), conv, &messagesStored, &lastEvent, func(string, string) {}, nil)
	if res.aborted || res.stored != int64(n) {
		t.Fatalf("conversation not finished after cancel: %+v", res)
	}
	if count, _ := a.db.CountMessages(); count != int64(n) {
		t.Fatalf("db messages = %d, want %d", count, n)
	}

	// A second conversation in the same chunk is not started once ctx is done.
	two := historySyncWithTextMessages(chat, base.Add(time.Hour), "d1")
	two.Data.Conversations = append(two.Data.Conversations, historySyncWithTextMessages(types.JID{User: "456", Server: types.DefaultUserServer}, base, "e1").Data.Conversations...)
	a.handleHistorySync(ctx, SyncOptions{}, two, &messagesStored, &lastEvent, func(string, string) {})
	if ok, _ := a.db.MessageExists(chat.String(), "d1"); !ok {
		t.Fatalf("first conversation of a chunk should be stored even after cancel")
	}
	if ok, _ := a.db.MessageExists("456@s.whatsapp.net", "e1"); ok {
		t.Fatalf("second conversation should not start after cancel")
	}
}

func TestKeepLastEventAlive(t *testing.T) {
	var lastEvent atomic.Int64
	stop := keepLastEventAlive(&lastEvent, 2*time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for lastEvent.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("lastEvent never bumped")
		}
		time.Sleep(time.Millisecond)
	}
	stop()
	stop() // idempotent
	v := lastEvent.Load()
	time.Sleep(20 * time.Millisecond)
	if lastEvent.Load() != v {
		t.Fatalf("lastEvent bumped after stop")
	}
}
