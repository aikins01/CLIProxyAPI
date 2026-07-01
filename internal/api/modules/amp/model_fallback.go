package amp

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
)

// neoModelFallbackCooldown is how long a model is routed to its configured fallback after
// the source provider reports a quota/unavailable error, before the source is retried.
const neoModelFallbackCooldown = 5 * time.Minute

// neoModelFailureBreaker tracks per-model provider failures so the fallback handler can
// temporarily route a failing model (e.g. Gemini code review hitting a quota cap) to its
// configured Claude fallback instead of repeatedly hitting the exhausted provider. The
// first failure surfaces to the caller and trips the breaker; subsequent requests within
// the cooldown route to the fallback, and the source is retried once the cooldown elapses.
type neoModelFailureBreaker struct {
	mu           sync.Mutex
	cooldown     time.Duration
	trippedUntil map[string]time.Time
}

func newNeoModelFailureBreaker(cooldown time.Duration) *neoModelFailureBreaker {
	if cooldown <= 0 {
		cooldown = neoModelFallbackCooldown
	}
	return &neoModelFailureBreaker{cooldown: cooldown, trippedUntil: map[string]time.Time{}}
}

func neoFailureBreakerKey(model string) string {
	return strings.ToLower(strings.TrimSpace(thinking.ParseSuffix(model).ModelName))
}

// tripped reports whether the model is currently in its post-failure cooldown.
func (b *neoModelFailureBreaker) tripped(model string) bool {
	if b == nil {
		return false
	}
	key := neoFailureBreakerKey(model)
	b.mu.Lock()
	defer b.mu.Unlock()
	until, ok := b.trippedUntil[key]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(b.trippedUntil, key)
		return false
	}
	return true
}

// trip starts (or extends) the cooldown for the model after a provider failure.
func (b *neoModelFailureBreaker) trip(model string) {
	if b == nil {
		return
	}
	key := neoFailureBreakerKey(model)
	b.mu.Lock()
	b.trippedUntil[key] = time.Now().Add(b.cooldown)
	b.mu.Unlock()
}

func neoRetryableProviderStatus(status int) bool {
	return status == http.StatusTooManyRequests ||
		status == http.StatusServiceUnavailable
}

// fallbackTargetFor returns the configured on-failure fallback model for the requested
// model, or "" if no rule matches or the fallback target itself has no available provider.
func (fh *FallbackHandler) fallbackTargetFor(modelName, normalizedModel string) string {
	if fh == nil || fh.fallbackMapper == nil {
		return ""
	}
	target := strings.TrimSpace(fh.fallbackMapper.MapModel(modelName))
	if target == "" {
		target = strings.TrimSpace(fh.fallbackMapper.MapModel(normalizedModel))
	}
	return target
}

// routeFallbackModel rewrites the request to the fallback model and runs the handler. The
// conductor handles cross-provider format translation, and the response rewriter restores
// the client-facing model name so the caller still sees the model it requested.
func (fh *FallbackHandler) routeFallbackModel(c *gin.Context, handler gin.HandlerFunc, originalModel, target, requestPath string, bodyBytes []byte) {
	body := rewriteModelInRequest(bodyBytes, target)
	c.Set(MappedModelContextKey, target)
	providerName := ""
	if ps := util.GetProviderName(thinking.ParseSuffix(target).ModelName); len(ps) > 0 {
		providerName = ps[0]
	}
	logAmpRouting(RouteTypeModelMapping, originalModel, target, providerName, requestPath)
	rewriter := NewResponseRewriter(c.Writer, originalModel)
	rewriter.suppressThinking = providerName != "claude"
	c.Writer = rewriter
	filterAntropicBetaHeader(c)
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	handler(c)
	rewriter.Flush()
	log.Debugf("amp model fallback: served %s via %s", originalModel, target)
}

type neoFallbackProbeWriter struct {
	gin.ResponseWriter
	header    http.Header
	body      bytes.Buffer
	status    int
	size      int
	written   bool
	committed bool
}

func newNeoFallbackProbeWriter(w gin.ResponseWriter) *neoFallbackProbeWriter {
	return &neoFallbackProbeWriter{
		ResponseWriter: w,
		header:         make(http.Header),
		status:         http.StatusOK,
		size:           -1,
	}
}

func (w *neoFallbackProbeWriter) Header() http.Header {
	if w.committed {
		return w.ResponseWriter.Header()
	}
	return w.header
}

func (w *neoFallbackProbeWriter) WriteHeader(code int) {
	if code > 0 && !w.written {
		w.status = code
	}
}

func (w *neoFallbackProbeWriter) WriteHeaderNow() {
	if w.written {
		return
	}
	w.written = true
	if neoRetryableProviderStatus(w.status) {
		w.size = 0
		return
	}
	w.commit()
}

func (w *neoFallbackProbeWriter) Write(data []byte) (int, error) {
	w.WriteHeaderNow()
	if neoRetryableProviderStatus(w.status) && !w.committed {
		if w.body.Len()+len(data) <= maxBufferedResponseBytes {
			_, _ = w.body.Write(data)
		}
		w.size += len(data)
		return len(data), nil
	}
	n, err := w.ResponseWriter.Write(data)
	if err == nil {
		w.size += n
	}
	return n, err
}

func (w *neoFallbackProbeWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

func (w *neoFallbackProbeWriter) Status() int {
	return w.status
}

func (w *neoFallbackProbeWriter) Size() int {
	return w.size
}

func (w *neoFallbackProbeWriter) Written() bool {
	return w.written || w.committed
}

func (w *neoFallbackProbeWriter) Flush() {
	if neoRetryableProviderStatus(w.status) && !w.committed {
		w.written = true
		return
	}
	w.commit()
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *neoFallbackProbeWriter) Pusher() http.Pusher {
	return w.ResponseWriter.Pusher()
}

func (w *neoFallbackProbeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.Hijack()
}

func (w *neoFallbackProbeWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.CloseNotify()
}

func (w *neoFallbackProbeWriter) shouldFallback() bool {
	return !w.committed && neoRetryableProviderStatus(w.status)
}

func (w *neoFallbackProbeWriter) finish() {
	if w.shouldFallback() {
		return
	}
	w.commit()
	if w.body.Len() > 0 {
		if _, err := w.ResponseWriter.Write(w.body.Bytes()); err != nil {
			log.Warnf("amp model fallback: failed to write buffered response: %v", err)
		}
	}
}

func (w *neoFallbackProbeWriter) commit() {
	if w.committed {
		return
	}
	for key, values := range w.header {
		copied := make([]string, len(values))
		copy(copied, values)
		w.ResponseWriter.Header()[key] = copied
	}
	w.ResponseWriter.WriteHeader(w.status)
	w.committed = true
}
