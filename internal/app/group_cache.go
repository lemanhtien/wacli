package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/openclaw/wacli/internal/store"
	"go.mau.fi/whatsmeow/types"
)

const (
	groupInfoCacheTTL    = 10 * time.Minute
	groupInfoCacheErrTTL = time.Minute
)

// groupInfoEntry is one cached GetGroupInfo result. ready is closed once the
// fetch that created the entry has finished, so concurrent callers for the same
// group (history worker + live path) share a single IQ round trip.
type groupInfoEntry struct {
	ready     chan struct{}
	info      *types.GroupInfo
	err       error
	fetchedAt time.Time
	stored    bool
}

// groupInfoCache memoises whatsmeow GetGroupInfo, which is an IQ round trip
// with no cache of its own. Before this cache every stored group message cost
// two round trips (ResolveChatName + participants), which capped history
// replay at ~25 msg/s and, worse, kept the node handler busy while realtime
// nodes queued behind it. Entries live for one Sync run (reset on start) with a
// TTL so long-running follow syncs still pick up renames and membership
// changes.
type groupInfoCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	errTTL  time.Duration
	entries map[string]*groupInfoEntry
	now     func() time.Time
}

func newGroupInfoCache(ttl, errTTL time.Duration) *groupInfoCache {
	return &groupInfoCache{
		ttl:     ttl,
		errTTL:  errTTL,
		entries: map[string]*groupInfoEntry{},
		now:     nowUTC,
	}
}

func (c *groupInfoCache) reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries = map[string]*groupInfoEntry{}
	c.mu.Unlock()
}

func (c *groupInfoCache) expiredLocked(e *groupInfoEntry) bool {
	ttl := c.ttl
	if e.err != nil {
		ttl = c.errTTL
	}
	return c.now().Sub(e.fetchedAt) >= ttl
}

// get returns the cached entry for key, fetching it through fetch when the
// entry is missing or expired. Context errors are never cached so a cancelled
// caller does not poison the entry for others.
func (c *groupInfoCache) get(ctx context.Context, key string, fetch func() (*types.GroupInfo, error)) (*groupInfoEntry, error) {
	if c == nil {
		info, err := fetch()
		return &groupInfoEntry{info: info, err: err}, err
	}
	c.mu.Lock()
	e := c.entries[key]
	if e != nil {
		select {
		case <-e.ready:
			if c.expiredLocked(e) {
				e = nil
			}
		default:
		}
	}
	if e == nil {
		e = &groupInfoEntry{ready: make(chan struct{})}
		c.entries[key] = e
		c.mu.Unlock()
		info, err := fetch()
		c.mu.Lock()
		e.info, e.err, e.fetchedAt = info, err, c.now()
		if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			// Do not memoise the caller's own cancellation.
			if c.entries[key] == e {
				delete(c.entries, key)
			}
		}
		close(e.ready)
		c.mu.Unlock()
		return e, err
	}
	c.mu.Unlock()
	select {
	case <-e.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return e, e.err
}

// markStored flips the entry's stored flag and reports whether this caller
// won, i.e. whether it should persist the group metadata.
func (c *groupInfoCache) markStored(e *groupInfoEntry) bool {
	if e == nil {
		return false
	}
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e.stored {
		return false
	}
	e.stored = true
	return true
}

// groupInfo returns group metadata via the per-run cache.
func (a *App) groupInfo(ctx context.Context, chat types.JID) (*types.GroupInfo, error) {
	e, err := a.groupInfoEntry(ctx, chat)
	if err != nil {
		return nil, err
	}
	return e.info, nil
}

func (a *App) groupInfoEntry(ctx context.Context, chat types.JID) (*groupInfoEntry, error) {
	key := canonicalJIDString(chat)
	if key == "" {
		key = chat.String()
	}
	return a.groups.get(ctx, key, func() (*types.GroupInfo, error) {
		return a.wa.GetGroupInfo(ctx, chat)
	})
}

// resolveChatName mirrors wa.Client.ResolveChatName but serves group and
// broadcast-list names from the group-info cache instead of an IQ per message.
func (a *App) resolveChatName(ctx context.Context, chat types.JID, pushName string) string {
	if chat.Server != types.GroupServer && !chat.IsBroadcastList() {
		return a.wa.ResolveChatName(ctx, chat, pushName)
	}
	if info, err := a.groupInfo(ctx, chat); err == nil && info != nil {
		if name := strings.TrimSpace(info.GroupName.Name); name != "" {
			return name
		}
	}
	if name := strings.TrimSpace(pushName); name != "" && name != "-" {
		return name
	}
	return chat.String()
}

// ensureGroupStored persists group metadata and participants once per cache
// entry (i.e. once per group per TTL) instead of on every message. It is
// best-effort like the per-message writes it replaces.
func (a *App) ensureGroupStored(ctx context.Context, chat types.JID) {
	if chat.Server != types.GroupServer {
		return
	}
	e, err := a.groupInfoEntry(ctx, chat)
	if err != nil || e == nil || e.info == nil {
		return
	}
	if !a.groups.markStored(e) {
		return
	}
	gi := e.info
	_ = a.db.UpsertGroupWithHierarchy(gi.JID.String(), gi.GroupName.Name, gi.OwnerJID.String(), gi.GroupCreated, gi.IsParent, gi.LinkedParentJID.String())
	ps := make([]store.GroupParticipant, 0, len(gi.Participants))
	for _, p := range gi.Participants {
		role := "member"
		if p.IsSuperAdmin {
			role = "superadmin"
		} else if p.IsAdmin {
			role = "admin"
		}
		ps = append(ps, store.GroupParticipant{
			GroupJID: chat.String(),
			UserJID:  canonicalJIDString(p.JID),
			Role:     role,
		})
	}
	_ = a.db.ReplaceGroupParticipants(chat.String(), ps)
}
