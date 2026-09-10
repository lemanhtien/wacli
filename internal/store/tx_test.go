package store

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

func TestMessageExists(t *testing.T) {
	db := openTestDB(t)
	chat := "123@s.whatsapp.net"
	if err := db.UpsertChat(chat, "dm", "Alice", time.Now()); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	ok, err := db.MessageExists(chat, "m1")
	if err != nil || ok {
		t.Fatalf("MessageExists before insert = %v, %v; want false, nil", ok, err)
	}
	if err := db.UpsertMessage(UpsertMessageParams{ChatJID: chat, MsgID: "m1", Timestamp: time.Now(), Text: "hi"}); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	ok, err = db.MessageExists(chat, "m1")
	if err != nil || !ok {
		t.Fatalf("MessageExists after insert = %v, %v; want true, nil", ok, err)
	}
	ok, err = db.MessageExists("other@s.whatsapp.net", "m1")
	if err != nil || ok {
		t.Fatalf("MessageExists other chat = %v, %v; want false, nil", ok, err)
	}
}

func TestUpsertMessagesBatch(t *testing.T) {
	db := openTestDB(t)
	chat := "123@s.whatsapp.net"
	if err := db.UpsertChat(chat, "dm", "Alice", time.Now()); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	batch := make([]UpsertMessageParams, 0, 250)
	for i := 0; i < 250; i++ {
		batch = append(batch, UpsertMessageParams{
			ChatJID:   chat,
			MsgID:     fmt.Sprintf("m%03d", i),
			Timestamp: base.Add(time.Duration(i) * time.Second),
			Text:      fmt.Sprintf("text %d", i),
		})
	}
	if err := db.UpsertMessages(batch); err != nil {
		t.Fatalf("UpsertMessages: %v", err)
	}
	n, err := db.CountMessages()
	if err != nil || n != 250 {
		t.Fatalf("CountMessages = %d, %v; want 250", n, err)
	}
	if err := db.UpsertMessages(nil); err != nil {
		t.Fatalf("UpsertMessages(nil): %v", err)
	}
}

func TestWithTxRollsBackOnError(t *testing.T) {
	db := openTestDB(t)
	chat := "123@s.whatsapp.net"
	if err := db.UpsertChat(chat, "dm", "Alice", time.Now()); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	boom := errors.New("boom")
	err := db.WithTx(func(tx *DB) error {
		if err := tx.UpsertMessage(UpsertMessageParams{ChatJID: chat, MsgID: "m-rollback", Timestamp: time.Now(), Text: "x"}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("WithTx error = %v, want boom", err)
	}
	ok, err := db.MessageExists(chat, "m-rollback")
	if err != nil || ok {
		t.Fatalf("row survived rollback: exists=%v err=%v", ok, err)
	}
	// Rows written earlier in the transaction are visible to later reads
	// through the transaction-scoped handle.
	err = db.WithTx(func(tx *DB) error {
		if err := tx.UpsertMessage(UpsertMessageParams{ChatJID: chat, MsgID: "m-visible", Timestamp: time.Now(), Text: "seen"}); err != nil {
			return err
		}
		msg, err := tx.GetMessage(chat, "m-visible")
		if err != nil {
			return err
		}
		if msg.Text != "seen" {
			return fmt.Errorf("text = %q", msg.Text)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx visibility: %v", err)
	}
}

func TestIsBusyError(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("something else"), false},
		{sqlite3.Error{Code: sqlite3.ErrBusy}, true},
		{sqlite3.Error{Code: sqlite3.ErrLocked}, true},
		{sqlite3.Error{Code: sqlite3.ErrConstraint}, false},
		{fmt.Errorf("wrapped: %w", sqlite3.Error{Code: sqlite3.ErrBusy}), true},
		{errors.New("database is locked"), true},
		{errors.New("sync storage limit reached"), false},
	}
	for _, c := range cases {
		if got := IsBusyError(c.err); got != c.want {
			t.Errorf("IsBusyError(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
