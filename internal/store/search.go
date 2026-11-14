package store

import (
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type SearchMessagesParams struct {
	Query     string
	ChatJID   string
	ChatJIDs  []string
	From      string
	Limit     int
	Before    *time.Time
	After     *time.Time
	HasMedia  bool
	Type      string
	Forwarded bool
	Starred   bool
}

func (d *DB) SearchMessages(p SearchMessagesParams) ([]Message, error) {
	if strings.TrimSpace(p.Query) == "" {
		return nil, fmt.Errorf("query is required")
	}
	msgType := normalizedMessageType(p.Type)
	if msgType != "" && !validSearchMessageType(msgType) {
		return nil, fmt.Errorf("unsupported message type %q", p.Type)
	}
	if p.HasMedia && msgType == "text" {
		return nil, fmt.Errorf("cannot combine has-media with type=text")
	}
	if p.Limit <= 0 {
		p.Limit = 50
	}

	if d.ftsEnabled {
		return d.searchFTS(p)
	}
	return d.searchLIKE(p)
}

// escapeLIKE escapes SQL LIKE wildcard characters (%, _) and the escape
// character itself so that user input is treated as a literal string (#56).
func escapeLIKE(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

func likeContains(s string) string {
	return "%" + escapeLIKE(s) + "%"
}

func (d *DB) searchLIKE(p SearchMessagesParams) ([]Message, error) {
	query := `
		SELECT ` + messageSelectColumns("") + `
		FROM messages m
		LEFT JOIN chats c ON c.jid = m.chat_jid
		LEFT JOIN starred s ON s.chat_jid = m.chat_jid AND s.msg_id = m.msg_id
	WHERE m.revoked = 0 AND m.deleted_for_me = 0 AND (LOWER(m.text) LIKE LOWER(?) ESCAPE '\' OR LOWER(m.display_text) LIKE LOWER(?) ESCAPE '\' OR LOWER(m.media_caption) LIKE LOWER(?) ESCAPE '\' OR LOWER(m.filename) LIKE LOWER(?) ESCAPE '\' OR LOWER(COALESCE(m.chat_name,'')) LIKE LOWER(?) ESCAPE '\' OR LOWER(COALESCE(m.sender_name,'')) LIKE LOWER(?) ESCAPE '\' OR LOWER(COALESCE(c.name,'')) LIKE LOWER(?) ESCAPE '\')`
	// Escape wildcards before wrapping in % so user input is literal (#56).
	needle := likeContains(p.Query)
	args := []interface{}{needle, needle, needle, needle, needle, needle, needle}
	query, args = applyMessageFilters(query, args, p)
	query += " ORDER BY m.ts DESC, m.rowid DESC LIMIT ?"
	args = append(args, p.Limit)
	return d.scanMessages(query, args...)
}

// sanitizeFTSQuery converts a raw user query into a safe FTS5 expression by
// quoting each whitespace-delimited token individually. This prevents FTS5
// query-syntax injection (AND/OR/NOT/NEAR/column filters) while preserving
// intuitive multi-word search: "hello world" matches messages containing both
// words (implicit AND), not necessarily as an exact phrase.
//
// Code-like tokens (see isPrefixSearchToken) additionally get a trailing `*`
// so they match as a prefix: the unicode61 tokenizer indexes "228345A" as the
// single term "228345a", so an exact-term query for "228345" would miss it.
// The `*` sits outside the quotes, so it is FTS5 prefix syntax rather than
// user-controlled input; a `*` typed by the user stays inside the quotes and
// remains literal.
func sanitizeFTSQuery(q string) string {
	tokens := strings.Fields(q)
	if len(tokens) == 0 {
		return `""`
	}
	quoted := make([]string, len(tokens))
	for i, tok := range tokens {
		// Escape embedded double-quotes by doubling them (FTS5 convention).
		quoted[i] = `"` + strings.ReplaceAll(tok, `"`, `""`) + `"`
		if isPrefixSearchToken(tok) {
			quoted[i] += "*"
		}
	}
	return strings.Join(quoted, " ")
}

// minPrefixSearchLen is the shortest token that is searched as a prefix.
// Todo(@Tien): 4 is a guess — only benchmarked on a 46k-message local store
// (~0.03s). Check search-queries.log on the 29GB prod store for short numeric
// prefixes like "2025" / "1000"; raise the limit if they get slow.
const minPrefixSearchLen = 4

// isPrefixSearchToken reports whether tok should be matched as a prefix:
// it must contain a digit (order numbers, codes, phone fragments) and be at
// least minPrefixSearchLen runes long. Plain words keep exact-term matching on
// purpose: FTS5 without a prefix index evaluates "abc*" by merging the doclists
// of every indexed term starting with "abc" before the first row is returned,
// so a common word prefix (e.g. "nguy*" -> nguyen, nguyet, ...) on a multi-GB
// store would undo the rowid-order early termination in searchFTS. Terms that
// start with a 4+ char digit-bearing prefix are rare, so that merge stays small.
func isPrefixSearchToken(tok string) bool {
	if utf8.RuneCountInString(tok) < minPrefixSearchLen {
		return false
	}
	return strings.IndexFunc(tok, unicode.IsDigit) >= 0
}

func (d *DB) searchFTS(p SearchMessagesParams) ([]Message, error) {
	query := `
		SELECT ` + messageSelectColumns("snippet(messages_fts, 0, '[', ']', '…', 12)") + `
		FROM messages_fts
		JOIN messages m ON messages_fts.rowid = m.rowid
		LEFT JOIN chats c ON c.jid = m.chat_jid
		LEFT JOIN starred s ON s.chat_jid = m.chat_jid AND s.msg_id = m.msg_id
		WHERE messages_fts MATCH ? AND m.revoked = 0 AND m.deleted_for_me = 0`
	// Sanitize to prevent FTS5 query-syntax injection (#57).
	// Each token is individually quoted so multi-word queries still work
	// as implicit AND (both words present, any order).
	args := []interface{}{sanitizeFTSQuery(p.Query)}
	query, args = applyMessageFilters(query, args, p)
	// Order by rowid DESC (newest-first), not bm25: relevance ranking forces
	// SQLite to score and JOIN every matching row before LIMIT applies, which
	// on a multi-GB store with a high-frequency token means hundreds of
	// thousands of cold random reads (~220s measured on a 29GB DB with 123k
	// matches). FTS5 walks posting lists in rowid order natively, so this
	// terminates after LIMIT rows (~0.1s on the same query).
	query += " ORDER BY messages_fts.rowid DESC LIMIT ?"
	args = append(args, p.Limit)
	return d.scanMessages(query, args...)
}

func applyMessageFilters(query string, args []interface{}, p SearchMessagesParams) (string, []interface{}) {
	query, args = appendStringFilter(query, args, "m.chat_jid", p.ChatJID, p.ChatJIDs)
	if strings.TrimSpace(p.From) != "" {
		query += " AND m.sender_jid = ?"
		args = append(args, p.From)
	}
	if p.After != nil {
		query += " AND m.ts > ?"
		args = append(args, unix(*p.After))
	}
	if p.Before != nil {
		query += " AND m.ts < ?"
		args = append(args, unix(*p.Before))
	}
	if p.HasMedia {
		query += " AND COALESCE(m.media_type,'') != ''"
	}
	if p.Forwarded {
		query += " AND m.is_forwarded = 1"
	}
	if p.Starred {
		query += " AND s.msg_id IS NOT NULL"
	}
	if msgType := normalizedMessageType(p.Type); msgType != "" {
		if msgType == "text" {
			query += " AND COALESCE(m.media_type,'') = ''"
		} else {
			query += " AND LOWER(COALESCE(m.media_type,'')) = ?"
			args = append(args, msgType)
		}
	}
	return query, args
}

func normalizedMessageType(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func validSearchMessageType(s string) bool {
	switch s {
	case "text", "image", "video", "audio", "document":
		return true
	default:
		return false
	}
}
