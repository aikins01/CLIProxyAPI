package helps

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type codexCacheEntry struct {
	id     string
	expire time.Time
}

// codexCacheMap stores prompt cache IDs keyed by model+user_id.
// Protected by codexCacheMu. Entries expire after 1 hour.
var (
	codexCacheMap = make(map[string]codexCacheEntry)
	codexCacheMu  sync.RWMutex
)

// codexCacheCleanupInterval controls how often expired entries are purged.
const codexCacheCleanupInterval = 15 * time.Minute

const codexPromptCacheLimit = 4096

// codexCacheCleanupOnce ensures the background cleanup goroutine starts only once.
var codexCacheCleanupOnce sync.Once

// startCodexCacheCleanup launches a background goroutine that periodically
// removes expired entries from codexCacheMap to prevent memory leaks.
func startCodexCacheCleanup() {
	go func() {
		ticker := time.NewTicker(codexCacheCleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			purgeExpiredCodexCache()
		}
	}()
}

// purgeExpiredCodexCache removes entries that have expired.
func purgeExpiredCodexCache() {
	now := time.Now()
	codexCacheMu.Lock()
	defer codexCacheMu.Unlock()
	for key, cache := range codexCacheMap {
		if cache.expire.Before(now) {
			delete(codexCacheMap, key)
		}
	}
}

// CodexPromptCacheID returns the stable prompt cache ID for a model and user key, or an empty string when the cache is full.
func CodexPromptCacheID(key string) string {
	if key == "" {
		return ""
	}
	codexCacheCleanupOnce.Do(startCodexCacheCleanup)
	now := time.Now()
	codexCacheMu.Lock()
	defer codexCacheMu.Unlock()
	if cache, ok := codexCacheMap[key]; ok && cache.expire.After(now) {
		cache.expire = now.Add(time.Hour)
		codexCacheMap[key] = cache
		return cache.id
	}
	delete(codexCacheMap, key)
	if len(codexCacheMap) >= codexPromptCacheLimit {
		for cachedKey, cache := range codexCacheMap {
			if !cache.expire.After(now) {
				delete(codexCacheMap, cachedKey)
			}
		}
	}
	if len(codexCacheMap) >= codexPromptCacheLimit {
		return ""
	}
	id := uuid.NewString()
	codexCacheMap[key] = codexCacheEntry{id: id, expire: now.Add(time.Hour)}
	return id
}
