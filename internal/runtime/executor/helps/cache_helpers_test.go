package helps

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCodexPromptCacheIDStability(t *testing.T) {
	t.Cleanup(func() {
		codexCacheMu.Lock()
		codexCacheMap = make(map[string]codexCacheEntry)
		codexCacheMu.Unlock()
	})

	t.Run("capacity_reached_preserves_oldest_key", func(t *testing.T) {
		resetCodexPromptCache()
		oldestID := CodexPromptCacheID("key-0")
		for i := 1; i < codexPromptCacheLimit; i++ {
			if id := CodexPromptCacheID(fmt.Sprintf("key-%d", i)); id == "" {
				t.Fatalf("cache reached capacity at entry %d", i)
			}
		}

		if id := CodexPromptCacheID("overflow"); id != "" {
			t.Fatalf("overflow ID = %q, want empty", id)
		}
		if id := CodexPromptCacheID("key-0"); id != oldestID {
			t.Fatalf("oldest key ID = %q, want %q", id, oldestID)
		}
		if size := codexPromptCacheSize(); size != codexPromptCacheLimit {
			t.Fatalf("cache size = %d, want %d", size, codexPromptCacheLimit)
		}
	})

	t.Run("expiry_frees_capacity", func(t *testing.T) {
		resetCodexPromptCache()
		now := time.Now()
		codexCacheMu.Lock()
		for i := 0; i < codexPromptCacheLimit; i++ {
			codexCacheMap[fmt.Sprintf("key-%d", i)] = codexCacheEntry{
				id:     fmt.Sprintf("id-%d", i),
				expire: now.Add(time.Hour),
			}
		}
		codexCacheMap["key-0"] = codexCacheEntry{id: "expired-id", expire: now.Add(-time.Hour)}
		codexCacheMu.Unlock()

		if id := CodexPromptCacheID("replacement"); id == "" {
			t.Fatal("replacement ID is empty")
		}
		codexCacheMu.RLock()
		_, expiredExists := codexCacheMap["key-0"]
		size := len(codexCacheMap)
		codexCacheMu.RUnlock()
		if expiredExists {
			t.Fatal("expired entry remains cached")
		}
		if size != codexPromptCacheLimit {
			t.Fatalf("cache size = %d, want %d", size, codexPromptCacheLimit)
		}
	})

	t.Run("cleanup_removes_only_expired_entries", func(t *testing.T) {
		resetCodexPromptCache()
		now := time.Now()
		codexCacheMu.Lock()
		codexCacheMap["expired"] = codexCacheEntry{id: "expired-id", expire: now.Add(-time.Hour)}
		codexCacheMap["active"] = codexCacheEntry{id: "active-id", expire: now.Add(time.Hour)}
		codexCacheMu.Unlock()

		purgeExpiredCodexCache()

		codexCacheMu.RLock()
		_, expiredExists := codexCacheMap["expired"]
		active, activeExists := codexCacheMap["active"]
		codexCacheMu.RUnlock()
		if expiredExists {
			t.Fatal("expired entry remains cached")
		}
		if !activeExists || active.id != "active-id" {
			t.Fatalf("active entry = %#v, exists = %t", active, activeExists)
		}
	})

	t.Run("concurrent_access_reuses_one_id", func(t *testing.T) {
		resetCodexPromptCache()
		const requestCount = 64
		ids := make(chan string, requestCount)
		var wg sync.WaitGroup
		for range requestCount {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ids <- CodexPromptCacheID("shared-key")
			}()
		}
		wg.Wait()
		close(ids)

		var expected string
		for id := range ids {
			if id == "" {
				t.Fatal("prompt cache ID is empty")
			}
			if expected == "" {
				expected = id
				continue
			}
			if id != expected {
				t.Fatalf("prompt cache IDs differ: %q and %q", expected, id)
			}
		}
		if size := codexPromptCacheSize(); size != 1 {
			t.Fatalf("cache size = %d, want 1", size)
		}
	})
}

func resetCodexPromptCache() {
	codexCacheMu.Lock()
	codexCacheMap = make(map[string]codexCacheEntry)
	codexCacheMu.Unlock()
}

func codexPromptCacheSize() int {
	codexCacheMu.RLock()
	defer codexCacheMu.RUnlock()
	return len(codexCacheMap)
}
