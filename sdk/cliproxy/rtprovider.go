package cliproxy

import (
	"net/http"
	"strings"
	"sync"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

const roundTripperCacheLimit = 32

// defaultRoundTripperProvider returns a per-auth HTTP RoundTripper based on
// the Auth.ProxyURL value. It caches transports per proxy URL string.
type defaultRoundTripperProvider struct {
	mu    sync.Mutex
	cache map[string]http.RoundTripper
	order []string
}

func newDefaultRoundTripperProvider() *defaultRoundTripperProvider {
	return &defaultRoundTripperProvider{cache: make(map[string]http.RoundTripper)}
}

// RoundTripperFor implements coreauth.RoundTripperProvider.
func (p *defaultRoundTripperProvider) RoundTripperFor(auth *coreauth.Auth) http.RoundTripper {
	if auth == nil {
		return nil
	}
	proxyStr := strings.TrimSpace(auth.ProxyURL)
	if proxyStr == "" {
		return nil
	}
	p.mu.Lock()
	rt := p.cache[proxyStr]
	if rt != nil {
		p.touch(proxyStr)
		p.mu.Unlock()
		return rt
	}
	transport, _, errBuild := proxyutil.BuildHTTPTransport(proxyStr)
	if errBuild != nil {
		p.mu.Unlock()
		log.Errorf("%v", errBuild)
		return nil
	}
	if transport == nil {
		p.mu.Unlock()
		return nil
	}
	var evicted http.RoundTripper
	if len(p.cache) >= roundTripperCacheLimit && len(p.order) > 0 {
		evictKey := p.order[0]
		p.order = p.order[1:]
		evicted = p.cache[evictKey]
		delete(p.cache, evictKey)
	}
	p.cache[proxyStr] = transport
	p.order = append(p.order, proxyStr)
	p.mu.Unlock()
	if closer, ok := evicted.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
	return transport
}

func (p *defaultRoundTripperProvider) touch(key string) {
	for i, cachedKey := range p.order {
		if cachedKey != key {
			continue
		}
		copy(p.order[i:], p.order[i+1:])
		p.order[len(p.order)-1] = key
		return
	}
	p.order = append(p.order, key)
}
