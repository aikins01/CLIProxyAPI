package amp

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
)

func TestFallbackHandler_ModelMapping_PreservesThinkingSuffixAndRewritesResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient("test-client-amp-fallback", "codex", []*registry.ModelInfo{
		{ID: "test/gpt-5.2", OwnedBy: "openai", Type: "codex"},
	})
	defer reg.UnregisterClient("test-client-amp-fallback")

	mapper := NewModelMapper([]config.AmpModelMapping{
		{From: "gpt-5.2", To: "test/gpt-5.2"},
	})

	fallback := NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy { return nil }, mapper, nil)

	handler := func(c *gin.Context) {
		var req struct {
			Model string `json:"model"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"model":      req.Model,
			"seen_model": req.Model,
		})
	}

	r := gin.New()
	r.POST("/chat/completions", fallback.WrapHandler(handler))

	reqBody := []byte(`{"model":"gpt-5.2(xhigh)"}`)
	req := httptest.NewRequest(http.MethodPost, "/chat/completions", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var resp struct {
		Model     string `json:"model"`
		SeenModel string `json:"seen_model"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response JSON: %v", err)
	}

	if resp.Model != "gpt-5.2(xhigh)" {
		t.Errorf("Expected response model gpt-5.2(xhigh), got %s", resp.Model)
	}
	if resp.SeenModel != "test/gpt-5.2(xhigh)" {
		t.Errorf("Expected handler to see test/gpt-5.2(xhigh), got %s", resp.SeenModel)
	}
}

func TestFallbackHandler_LocalNeoInferenceFailsClosedBeforeAmpProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstreamRequests := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests <- struct{}{}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"amp-upstream"}`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("amp-secret"))
	if err != nil {
		t.Fatalf("create reverse proxy: %v", err)
	}
	fallback := NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy { return proxy }, nil, nil)

	r := gin.New()
	r.POST("/api/provider/openai/v1/chat/completions", fallback.WrapHandler(func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"unexpected": true})
	}))
	server := httptest.NewServer(r)
	defer server.Close()

	reqBody := []byte(`{"model":"definitely-not-a-local-provider-model","messages":[{"role":"user","content":"hi"}]}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/provider/openai/v1/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(localNeoInferenceHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request fallback route: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status=%d body=%s", resp.StatusCode, respBody)
	}
	if !bytes.Contains(respBody, []byte(`local_neo_provider_unavailable`)) {
		t.Fatalf("expected local Neo fail-closed response, got %s", respBody)
	}

	select {
	case <-upstreamRequests:
		t.Fatal("local Neo inference request unexpectedly fell back to amp proxy")
	default:
	}
}

func TestFallbackHandler_LocalNeoAmpProviderFallsBackToAmpProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstreamRequests := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests <- r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"amp-upstream"}`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("amp-secret"))
	if err != nil {
		t.Fatalf("create reverse proxy: %v", err)
	}
	fallback := NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy { return proxy }, nil, nil)

	r := gin.New()
	r.POST("/api/provider/:provider/v1/chat/completions", fallback.WrapHandler(func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"unexpected": true})
	}))
	server := httptest.NewServer(r)
	defer server.Close()

	reqBody := []byte(`{"model":"amp-nostromo-v1","messages":[{"role":"user","content":"hi"}]}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/provider/amp/v1/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(localNeoInferenceHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request fallback route: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, respBody)
	}
	if !bytes.Contains(respBody, []byte(`amp-upstream`)) {
		t.Fatalf("expected amp upstream response, got %s", respBody)
	}

	select {
	case headers := <-upstreamRequests:
		if headers.Get(localNeoInferenceHeader) != "" {
			t.Fatalf("%s should be stripped before upstream proxy", localNeoInferenceHeader)
		}
	case <-time.After(time.Second):
		t.Fatal("local Neo amp provider request did not reach amp proxy")
	}
}

func TestFallbackHandler_LocalNeoAmpProviderIgnoresModelFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstreamBodies := make(chan []byte, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		upstreamBodies <- body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"amp-upstream"}`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("amp-secret"))
	if err != nil {
		t.Fatalf("create reverse proxy: %v", err)
	}
	fallback := NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy { return proxy }, nil, nil)
	fallback.SetFallbackMapper(stubFallbackMapper{target: "gpt-5.5"})
	fallback.failureBreaker.trip("amp-nostromo-v1")

	r := gin.New()
	r.POST("/api/provider/:provider/v1/chat/completions", fallback.WrapHandler(func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"unexpected": true})
	}))
	server := httptest.NewServer(r)
	defer server.Close()

	reqBody := []byte(`{"model":"amp-nostromo-v1","messages":[{"role":"user","content":"hi"}]}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/provider/amp/v1/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(localNeoInferenceHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request fallback route: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, respBody)
	}
	if !bytes.Contains(respBody, []byte(`amp-upstream`)) {
		t.Fatalf("expected amp upstream response, got %s", respBody)
	}

	select {
	case body := <-upstreamBodies:
		if !bytes.Contains(body, []byte(`amp-nostromo-v1`)) || bytes.Contains(body, []byte(`gpt-5.5`)) {
			t.Fatalf("upstream body = %s", body)
		}
	case <-time.After(time.Second):
		t.Fatal("local Neo amp provider request did not reach amp proxy")
	}
}

func TestFallbackHandler_AmpProviderWithoutLocalNeoUsesModelFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"amp-upstream"}`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("amp-secret"))
	if err != nil {
		t.Fatalf("create reverse proxy: %v", err)
	}
	fallback := NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy { return proxy }, nil, nil)
	fallback.SetFallbackMapper(stubFallbackMapper{target: "gpt-5.5"})

	var handlerBody []byte
	seenMappedModel := ""
	r := gin.New()
	r.POST("/api/provider/:provider/v1/chat/completions", fallback.WrapHandler(func(c *gin.Context) {
		var errRead error
		handlerBody, errRead = io.ReadAll(c.Request.Body)
		if errRead != nil {
			t.Fatalf("read handler body: %v", errRead)
		}
		if mapped, ok := c.Get(MappedModelContextKey); ok {
			seenMappedModel, _ = mapped.(string)
		}
		c.JSON(http.StatusOK, gin.H{"id": "fallback-handler", "model": "gpt-5.5"})
	}))
	server := httptest.NewServer(r)
	defer server.Close()

	reqBody := []byte(`{"model":"amp-nostromo-v1","messages":[{"role":"user","content":"hi"}]}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/provider/amp/v1/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request fallback route: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, respBody)
	}
	if seenMappedModel != "gpt-5.5" {
		t.Fatalf("mapped model = %q, want gpt-5.5", seenMappedModel)
	}
	if !bytes.Contains(handlerBody, []byte(`gpt-5.5`)) || bytes.Contains(handlerBody, []byte(`amp-nostromo-v1`)) {
		t.Fatalf("handler body = %s", handlerBody)
	}
	if upstreamCalls != 0 {
		t.Fatalf("amp proxy calls = %d, want fallback handler path", upstreamCalls)
	}
}

func TestFallbackHandlerGeminiWildcardActionStripsLeadingSlashBeforeMapping(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient("test-client-gemini-wildcard-action", "gemini", []*registry.ModelInfo{
		{ID: "test-gemini-flash", OwnedBy: "google", Type: "gemini"},
	})
	defer reg.UnregisterClient("test-client-gemini-wildcard-action")

	mapper := NewModelMapper([]config.AmpModelMapping{
		{From: "gemini-3-flash-preview", To: "test-gemini-flash"},
	})
	fallback := NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy { return nil }, mapper, func() bool { return true })

	seenMappedModel := ""
	seenAction := ""
	r := gin.New()
	r.POST("/api/provider/google/v1beta/models/*action", fallback.WrapHandler(withMappedGeminiAction(func(c *gin.Context) {
		if mapped, ok := c.Get(MappedModelContextKey); ok {
			seenMappedModel, _ = mapped.(string)
		}
		seenAction = c.Param("action")
		c.Status(http.StatusNoContent)
	})))

	req := httptest.NewRequest(http.MethodPost, "/api/provider/google/v1beta/models/gemini-3-flash-preview:generateContent", bytes.NewReader([]byte(`{"contents":[]}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(localNeoInferenceHeader, "1")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if seenMappedModel != "test-gemini-flash" {
		t.Fatalf("mapped model = %q, want test-gemini-flash", seenMappedModel)
	}
	if seenAction != "test-gemini-flash:generateContent" {
		t.Fatalf("action = %q, want test-gemini-flash:generateContent", seenAction)
	}
}

func TestFallbackHandler_LocalAuthUnavailableDoesNotFallbackToAmpProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient("test-client-local-auth-unavailable", "codex", []*registry.ModelInfo{
		{ID: "gpt-5.5", OwnedBy: "openai", Type: "codex"},
	})
	defer reg.UnregisterClient("test-client-local-auth-unavailable")

	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"amp-upstream"}`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("amp-secret"))
	if err != nil {
		t.Fatalf("create reverse proxy: %v", err)
	}
	fallback := NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy { return proxy }, nil, nil)

	r := gin.New()
	r.POST("/api/provider/openai/v1/chat/completions", fallback.WrapHandler(func(c *gin.Context) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{
			"message": "auth_unavailable: no auth available (providers=codex, model=gpt-5.5)",
			"type":    "server_error",
			"code":    "internal_server_error",
		}})
	}))
	server := httptest.NewServer(r)
	defer server.Close()

	reqBody := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hi"}]}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/provider/openai/v1/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(localNeoInferenceHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request fallback route: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", resp.StatusCode, respBody)
	}
	if !bytes.Contains(respBody, []byte(`auth_unavailable`)) {
		t.Fatalf("expected local auth_unavailable response, got %s", respBody)
	}
	if upstreamCalls != 0 {
		t.Fatalf("upstream calls = %d, want 0", upstreamCalls)
	}
}

func TestFallbackHandlerMultipartModelFallsBackToAmpProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	type capturedRequest struct {
		path string
		body []byte
	}
	gotRequest := make(chan capturedRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotRequest <- capturedRequest{path: r.URL.Path, body: body}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"amp-image-upstream"}`))
	}))
	defer upstream.Close()

	proxy, err := createReverseProxy(upstream.URL, NewStaticSecretSource("amp-secret"))
	if err != nil {
		t.Fatalf("create reverse proxy: %v", err)
	}
	fallback := NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy { return proxy }, nil, nil)

	r := gin.New()
	r.POST("/api/provider/openai/v1/images/edits", fallback.WrapHandler(func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"unexpected": true})
	}))
	server := httptest.NewServer(r)
	defer server.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("model", "definitely-not-a-local-provider-model"); err != nil {
		t.Fatalf("write model field: %v", err)
	}
	if err := writer.WriteField("prompt", "edit this image"); err != nil {
		t.Fatalf("write prompt field: %v", err)
	}
	imagePart, err := writer.CreateFormFile("image", "input.png")
	if err != nil {
		t.Fatalf("create image part: %v", err)
	}
	if _, err := imagePart.Write([]byte("not-a-real-image")); err != nil {
		t.Fatalf("write image part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/provider/openai/v1/images/edits", bytes.NewReader(body.Bytes()))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request fallback route: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body=%s", resp.StatusCode, body)
	}

	select {
	case got := <-gotRequest:
		if got.path != "/api/provider/openai/v1/images/edits" {
			t.Fatalf("upstream path = %q", got.path)
		}
		if !bytes.Contains(got.body, []byte("definitely-not-a-local-provider-model")) {
			t.Fatalf("upstream body did not preserve multipart model field: %q", string(got.body))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upstream fallback request")
	}
}

func TestFallbackHandlerCapturesAmpCompactionCandidateRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	dir := t.TempDir()
	t.Setenv("CLIPROXYAPI_AMP_COMPACTION_CAPTURE_DIR", dir)

	fallback := NewFallbackHandlerWithMapper(func() *httputil.ReverseProxy { return nil }, nil, nil)
	r := gin.New()
	r.POST("/api/provider/openai/v1/chat/completions", fallback.WrapHandler(func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.Data(http.StatusOK, "application/json", body)
	}))

	reqBody := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"You have been working on the task described above. Write a continuation summary. Wrap your summary in <summary></summary> tags."}]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/provider/openai/v1/chat/completions", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer secret-token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}

	matches, err := filepath.Glob(filepath.Join(dir, "amp-compaction-*.json"))
	if err != nil {
		t.Fatalf("glob captures: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("capture files = %v, want exactly one", matches)
	}
	capture, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read capture: %v", err)
	}
	text := string(capture)
	if !strings.Contains(text, "continuation summary") || !strings.Contains(text, "gpt-5.4") {
		t.Fatalf("capture missing compaction body details: %s", text)
	}
	if strings.Contains(text, "secret-token") {
		t.Fatalf("capture leaked authorization header: %s", text)
	}
}

func TestLooksLikeAmpCompactionRequestRejectsStreamingSummaryTurn(t *testing.T) {
	streamingTurn := []byte(`{"model":"gpt-5.5","stream":true,"messages":[{"role":"user","content":"Wrap your summary in <summary></summary> tags."}]}`)
	if looksLikeAmpCompactionRequest(streamingTurn) {
		t.Fatal("streaming turn with summary text was classified as compaction request")
	}

	compactionTurn := []byte(`{"model":"gpt-5.4","stream":false,"messages":[{"role":"user","content":"You have been working on the task described above. Context to Preserve. Wrap your summary in <summary></summary> tags."}]}`)
	if !looksLikeAmpCompactionRequest(compactionTurn) {
		t.Fatal("non-stream final continuation prompt was not classified as compaction request")
	}
}
