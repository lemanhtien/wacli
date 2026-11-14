//go:build sqlite_fts5

package store

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSearchMessagesUsesFTSWhenEnabled(t *testing.T) {
	db := openTestDB(t)
	if !db.HasFTS() {
		t.Fatalf("expected HasFTS=true in sqlite_fts5 build")
	}

	chat := "123@s.whatsapp.net"
	if err := db.UpsertChat(chat, "dm", "Alice", time.Now()); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := db.UpsertMessage(UpsertMessageParams{
		ChatJID:    chat,
		ChatName:   "Alice",
		MsgID:      "m1",
		SenderJID:  chat,
		SenderName: "Alice",
		Timestamp:  time.Now(),
		FromMe:     false,
		Text:       "hello world",
	}); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	ms, err := db.SearchMessages(SearchMessagesParams{Query: "hello", Limit: 10})
	if err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	if len(ms) != 1 {
		t.Fatalf("expected 1 result, got %d", len(ms))
	}
	if ms[0].Snippet == "" {
		t.Fatalf("expected snippet for FTS search, got empty")
	}
}

func TestExistingEmptyFTSTableDetectedOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !db.HasFTS() {
		t.Fatalf("expected initial FTS migration to enable FTS")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if !db.HasFTS() {
		t.Fatalf("expected existing empty FTS table to enable FTS after reopen")
	}
}

func TestMigrateLIDToPNMaintainsFTSRows(t *testing.T) {
	db := openTestDB(t)
	if !db.HasFTS() {
		t.Skip("FTS5 not enabled")
	}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pn := "15551234567@s.whatsapp.net"
	lid := "999123456789@lid"
	if err := db.UpsertChat(lid, "unknown", lid, base); err != nil {
		t.Fatalf("UpsertChat lid: %v", err)
	}
	if err := db.UpsertMessage(UpsertMessageParams{
		ChatJID:   lid,
		MsgID:     "m-lid",
		SenderJID: lid,
		Timestamp: base,
		Text:      "needle migrated",
	}); err != nil {
		t.Fatalf("UpsertMessage lid: %v", err)
	}

	if err := db.MigrateLIDToPN(lid, pn); err != nil {
		t.Fatalf("MigrateLIDToPN: %v", err)
	}

	msgs, err := db.SearchMessages(SearchMessagesParams{Query: "needle", ChatJID: pn, Limit: 10})
	if err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("search results = %d, want 1", len(msgs))
	}
	if msgs[0].ChatJID != pn {
		t.Fatalf("search result chat = %q, want %q", msgs[0].ChatJID, pn)
	}
}

// TestSanitizeFTSQuery verifies that user input is sanitized before being
// passed to the FTS5 MATCH clause, preventing query-syntax injection (#57).
func TestSanitizeFTSQuery(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		// Basic tokens are individually quoted.
		{"hello", `"hello"`},
		{"hello world", `"hello" "world"`},
		// FTS5 operators are neutralised — treated as literal tokens.
		{"hello OR world", `"hello" "OR" "world"`},
		{"NOT secret", `"NOT" "secret"`},
		{"hello AND world", `"hello" "AND" "world"`},
		// Column filter syntax is neutralised.
		{"col:value", `"col:value"`},
		// Prefix wildcard is neutralised.
		{"test*", `"test*"`},
		// Code-like tokens (letters/digits only, has a digit, >= 6 runes)
		// become prefix queries.
		{"228345", `"228345"*`},
		{"15500st", `"15500st"*`},
		{"đơn 228345", `"đơn" "228345"*`},
		// Tokens the tokenizer would split stay exact: a trailing `*` would
		// apply to the last sub-token only ("5711/1" -> "1*").
		{"5711/1", `"5711/1"`},
		{"228345-A", `"228345-A"`},
		{"228345*", `"228345*"`},
		{`228345"`, `"228345"""`},
		// Short or digit-free tokens stay exact.
		{"12345", `"12345"`},
		{"nguyenvan", `"nguyenvan"`},
		// NEAR operator is neutralised.
		{"NEAR(a b)", `"NEAR(a" "b)"`},
		// Embedded double-quotes are escaped by doubling.
		{`say "hi"`, `"say" """hi"""`},
		// Extra whitespace is collapsed.
		{"  spaced  ", `"spaced"`},
		// Empty / blank input returns empty quoted token.
		{"", `""`},
		{"   ", `""`},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got := sanitizeFTSQuery(tc.input)
			if got != tc.want {
				t.Errorf("sanitizeFTSQuery(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// TestFTSInjectionPrevented verifies end-to-end that FTS5 syntax in user
// queries does not cause errors or unexpected results (#57).
func TestFTSInjectionPrevented(t *testing.T) {
	db := openTestDB(t)
	if !db.HasFTS() {
		t.Skip("FTS5 not enabled")
	}

	chat := "555@s.whatsapp.net"
	if err := db.UpsertChat(chat, "dm", "Bob", time.Now()); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	msgs := []struct{ id, text string }{
		{"m1", "hello world"},
		{"m2", "price is 100% confirmed"},
		{"m3", "OR operator test"},
	}
	for _, m := range msgs {
		if err := db.UpsertMessage(UpsertMessageParams{
			ChatJID: chat, MsgID: m.id, Timestamp: time.Now(), Text: m.text,
		}); err != nil {
			t.Fatalf("UpsertMessage: %v", err)
		}
	}

	injectionQueries := []string{
		"OR hello",          // bare OR would be a syntax error in raw FTS5
		"NOT hello",         // bare NOT would be a syntax error
		"hello AND world",   // AND as operator vs literal
		"NEAR(hello world)", // NEAR function syntax
		`"hello"`,           // raw quoted phrase
	}

	for _, q := range injectionQueries {
		t.Run(q, func(t *testing.T) {
			// Must not panic or return an error — injection is neutralised.
			_, err := db.SearchMessages(SearchMessagesParams{Query: q, Limit: 10})
			if err != nil {
				t.Errorf("SearchMessages(%q) returned unexpected error: %v", q, err)
			}
		})
	}

	// Multi-word search should still work (implicit AND between tokens).
	t.Run("multi-word implicit AND", func(t *testing.T) {
		ms, err := db.SearchMessages(SearchMessagesParams{Query: "hello world", Limit: 10})
		if err != nil {
			t.Fatalf("SearchMessages: %v", err)
		}
		if len(ms) != 1 || ms[0].MsgID != "m1" {
			t.Errorf("expected m1 for 'hello world', got %v", ms)
		}
	})
}

// TestFTSCodePrefixSearch verifies that a numeric/code query matches indexed
// terms that extend it (e.g. "228345" finds "228345A"), while plain words keep
// exact-term matching.
func TestFTSCodePrefixSearch(t *testing.T) {
	db := openTestDB(t)
	if !db.HasFTS() {
		t.Skip("FTS5 not enabled")
	}

	chat := "777@s.whatsapp.net"
	if err := db.UpsertChat(chat, "dm", "Carol", time.Now()); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	msgs := []struct{ id, text string }{
		{"m1", "Mã đơn 228345A đã giao"},
		{"m2", "đơn 228345 khác"},
		{"m3", "ref:228345B-01"},
		{"m4", "SKU A228345"},
		{"m5", "helloworld"},
	}
	for _, m := range msgs {
		if err := db.UpsertMessage(UpsertMessageParams{
			ChatJID: chat, MsgID: m.id, Timestamp: time.Now(), Text: m.text,
		}); err != nil {
			t.Fatalf("UpsertMessage: %v", err)
		}
	}

	ids := func(q string) map[string]bool {
		t.Helper()
		ms, err := db.SearchMessages(SearchMessagesParams{Query: q, Limit: 10})
		if err != nil {
			t.Fatalf("SearchMessages(%q): %v", q, err)
		}
		got := map[string]bool{}
		for _, m := range ms {
			got[m.MsgID] = true
		}
		return got
	}

	got := ids("228345")
	for _, want := range []string{"m1", "m2", "m3"} {
		if !got[want] {
			t.Errorf("query 228345: missing %s, got %v", want, got)
		}
	}
	// Prefix, not substring: a code embedded mid-token is not matched.
	if got["m4"] {
		t.Errorf("query 228345: unexpected m4 (mid-token match), got %v", got)
	}

	if got := ids("228345A"); len(got) != 1 || !got["m1"] {
		t.Errorf("query 228345A: want only m1, got %v", got)
	}

	// Digit-free words stay exact-term.
	if got := ids("hello"); len(got) != 0 {
		t.Errorf("query hello: want no prefix match on helloworld, got %v", got)
	}
}

// ftsColumns returns the raw values stored in messages_fts for one message.
func ftsColumns(t *testing.T, db *DB, msgID string) (text, caption, display string) {
	t.Helper()
	err := db.sql.QueryRow(`
		SELECT f.text, f.media_caption, f.display_text
		FROM messages_fts f JOIN messages m ON m.rowid = f.rowid
		WHERE m.msg_id = ?`, msgID).Scan(&text, &caption, &display)
	if err != nil {
		t.Fatalf("fts row for %s: %v", msgID, err)
	}
	return text, caption, display
}

// TestFTSSkipsDuplicateColumns verifies that a column which only repeats the
// message body (display_text == text, media_caption == text) is not indexed a
// second time, while search still finds the message and distinct values are
// still indexed.
func TestFTSSkipsDuplicateColumns(t *testing.T) {
	db := openTestDB(t)
	if !db.HasFTS() {
		t.Skip("FTS5 not enabled")
	}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	chat := "123@g.us"
	if err := db.UpsertChat(chat, "group", "Watches", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	upsert := func(p UpsertMessageParams) {
		t.Helper()
		p.ChatJID = chat
		p.SenderJID = "111@s.whatsapp.net"
		if p.Timestamp.IsZero() {
			p.Timestamp = base
		}
		if err := db.UpsertMessage(p); err != nil {
			t.Fatalf("UpsertMessage %s: %v", p.MsgID, err)
		}
	}
	search := func(q string) []string {
		t.Helper()
		msgs, err := db.SearchMessages(SearchMessagesParams{Query: q, Limit: 10})
		if err != nil {
			t.Fatalf("SearchMessages(%q): %v", q, err)
		}
		ids := make([]string, 0, len(msgs))
		for _, m := range msgs {
			ids = append(ids, m.MsgID)
		}
		return ids
	}

	// Plain text: display_text repeats text.
	upsert(UpsertMessageParams{MsgID: "plain", Text: "rolex submariner", DisplayText: "rolex submariner"})
	// Captioned image: text repeats media_caption, display_text is a label.
	upsert(UpsertMessageParams{MsgID: "img", Text: "patek nautilus", MediaType: "image", MediaCaption: "patek nautilus", DisplayText: "Sent image"})
	// Reply: display_text carries extra context and must stay searchable.
	upsert(UpsertMessageParams{MsgID: "reply", Text: "deal", DisplayText: "> quotedneedle\ndeal"})
	// No text at all: display_text repeats the caption.
	upsert(UpsertMessageParams{MsgID: "caponly", MediaType: "video", MediaCaption: "omega speedmaster", DisplayText: "omega speedmaster"})

	if text, caption, display := ftsColumns(t, db, "plain"); text != "rolex submariner" || caption != "" || display != "" {
		t.Fatalf("plain fts = (%q,%q,%q), want text only", text, caption, display)
	}
	if text, caption, display := ftsColumns(t, db, "img"); text != "patek nautilus" || caption != "" || display != "Sent image" {
		t.Fatalf("img fts = (%q,%q,%q)", text, caption, display)
	}
	if _, _, display := ftsColumns(t, db, "reply"); display != "> quotedneedle\ndeal" {
		t.Fatalf("reply display_text not indexed: %q", display)
	}
	if text, caption, display := ftsColumns(t, db, "caponly"); text != "" || caption != "omega speedmaster" || display != "" {
		t.Fatalf("caponly fts = (%q,%q,%q), want caption only", text, caption, display)
	}

	for q, want := range map[string]string{
		"submariner":   "plain",
		"nautilus":     "img",
		"quotedneedle": "reply",
		"speedmaster":  "caponly",
	} {
		if got := search(q); len(got) != 1 || got[0] != want {
			t.Fatalf("search %q = %v, want [%s]", q, got, want)
		}
	}

	// Update path (messages_au): re-upserting keeps exactly one FTS row.
	upsert(UpsertMessageParams{MsgID: "plain", Text: "rolex submariner", DisplayText: "rolex submariner", ChatName: "Watches"})
	if got := search("submariner"); len(got) != 1 {
		t.Fatalf("after update search = %v, want one row", got)
	}
	// Delete path (messages_ad).
	if _, err := db.sql.Exec(`DELETE FROM messages WHERE msg_id = 'plain'`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := search("submariner"); len(got) != 0 {
		t.Fatalf("after delete search = %v, want none", got)
	}
	if n := countRows(t, db.sql, `SELECT count(*) FROM messages_fts`); n != 3 {
		t.Fatalf("messages_fts rows = %d, want 3", n)
	}
}

// TestFTSDedupTriggerMigrationUpgradesExistingStore simulates a store created
// before migration 21: old triggers, rows indexed with duplicated columns. The
// migration must swap the triggers, and deleting/updating old rows must still
// keep messages_fts consistent.
func TestFTSDedupTriggerMigrationUpgradesExistingStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wacli.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !db.HasFTS() {
		_ = db.Close()
		t.Skip("FTS5 not enabled")
	}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	chat := "123@g.us"
	if err := db.UpsertChat(chat, "group", "Watches", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	// Roll the store back to the pre-21 triggers.
	if _, err := db.sql.Exec(`
		DROP TRIGGER messages_ai; DROP TRIGGER messages_ad; DROP TRIGGER messages_au;
		CREATE TRIGGER messages_ai AFTER INSERT ON messages WHEN new.revoked = 0 AND new.deleted_for_me = 0 BEGIN
			INSERT INTO messages_fts(rowid, text, media_caption, filename, chat_name, sender_name, display_text)
			VALUES (new.rowid, COALESCE(new.text,''), COALESCE(new.media_caption,''), COALESCE(new.filename,''), COALESCE(new.chat_name,''), COALESCE(new.sender_name,''), COALESCE(new.display_text,''));
		END;
		CREATE TRIGGER messages_ad AFTER DELETE ON messages BEGIN
			DELETE FROM messages_fts WHERE rowid = old.rowid;
		END;
		CREATE TRIGGER messages_au AFTER UPDATE ON messages BEGIN
			DELETE FROM messages_fts WHERE rowid = old.rowid;
			INSERT INTO messages_fts(rowid, text, media_caption, filename, chat_name, sender_name, display_text)
			SELECT new.rowid, COALESCE(new.text,''), COALESCE(new.media_caption,''), COALESCE(new.filename,''), COALESCE(new.chat_name,''), COALESCE(new.sender_name,''), COALESCE(new.display_text,'')
			WHERE new.revoked = 0 AND new.deleted_for_me = 0;
		END;
		DELETE FROM schema_migrations WHERE version = 21;
	`); err != nil {
		t.Fatalf("install old triggers: %v", err)
	}
	old := UpsertMessageParams{ChatJID: chat, MsgID: "old", SenderJID: "1@s.whatsapp.net", Timestamp: base, Text: "legacy daytona", DisplayText: "legacy daytona"}
	if err := db.UpsertMessage(old); err != nil {
		t.Fatalf("UpsertMessage old: %v", err)
	}
	if _, _, display := ftsColumns(t, db, "old"); display != "legacy daytona" {
		t.Fatalf("precondition: old trigger should duplicate display_text, got %q", display)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if n := countRows(t, db.sql, `SELECT count(*) FROM schema_migrations WHERE version = 21`); n != 1 {
		t.Fatalf("migration 21 not recorded")
	}
	if n := countRows(t, db.sql, `SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name IN ('messages_ai','messages_au') AND sql LIKE '%NULLIF(new.display_text, new.text)%'`); n != 2 {
		t.Fatalf("de-duplicating triggers installed = %d, want 2", n)
	}

	// Old row is untouched by the migration and still searchable.
	if _, _, display := ftsColumns(t, db, "old"); display != "legacy daytona" {
		t.Fatalf("old row display_text changed by migration: %q", display)
	}
	// New row is de-duplicated.
	if err := db.UpsertMessage(UpsertMessageParams{ChatJID: chat, MsgID: "new", SenderJID: "1@s.whatsapp.net", Timestamp: base, Text: "fresh daytona", DisplayText: "fresh daytona"}); err != nil {
		t.Fatalf("UpsertMessage new: %v", err)
	}
	if _, _, display := ftsColumns(t, db, "new"); display != "" {
		t.Fatalf("new row display_text = %q, want empty", display)
	}
	msgs, err := db.SearchMessages(SearchMessagesParams{Query: "daytona", Limit: 10})
	if err != nil || len(msgs) != 2 {
		t.Fatalf("search daytona = %d rows, err=%v, want 2", len(msgs), err)
	}
	// Updating then deleting an old-generation row leaves no stale index entry.
	if err := db.UpsertMessage(old); err != nil {
		t.Fatalf("re-upsert old: %v", err)
	}
	if _, err := db.sql.Exec(`DELETE FROM messages WHERE msg_id = 'old'`); err != nil {
		t.Fatalf("delete old: %v", err)
	}
	msgs, err = db.SearchMessages(SearchMessagesParams{Query: "legacy", Limit: 10})
	if err != nil || len(msgs) != 0 {
		t.Fatalf("search legacy after delete = %d rows, err=%v, want 0", len(msgs), err)
	}
	if n := countRows(t, db.sql, `SELECT count(*) FROM messages_fts`); n != 1 {
		t.Fatalf("messages_fts rows = %d, want 1", n)
	}
	if _, err := db.sql.Exec(`INSERT INTO messages_fts(messages_fts) VALUES('integrity-check')`); err != nil {
		t.Fatalf("fts integrity-check: %v", err)
	}
}
