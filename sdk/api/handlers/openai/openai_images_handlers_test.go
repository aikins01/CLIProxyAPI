package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"github.com/tidwall/gjson"
)

func performImagesEndpointRequest(t *testing.T, endpointPath string, contentType string, body io.Reader, handler gin.HandlerFunc, headers ...http.Header) *httptest.ResponseRecorder {
	t.Helper()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST(endpointPath, handler)

	req := httptest.NewRequest(http.MethodPost, endpointPath, body)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for _, hdr := range headers {
		for key, values := range hdr {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

type imageFeatureHeaderExecutor struct {
	mu       sync.Mutex
	features []string
}

func (e *imageFeatureHeaderExecutor) Identifier() string { return "openai" }

func (e *imageFeatureHeaderExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *imageFeatureHeaderExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, _ coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	if opts.Headers != nil {
		e.features = append(e.features, opts.Headers.Get("X-Amp-Feature"))
	} else {
		e.features = append(e.features, "")
	}
	e.mu.Unlock()

	ch := make(chan coreexecutor.StreamChunk, 1)
	ch <- coreexecutor.StreamChunk{Payload: []byte(`event: response.completed
data: {"type":"response.completed","response":{"created_at":123,"output":[{"type":"image_generation_call","result":"AA==","output_format":"png"}],"tool_usage":{"image_gen":{"input_tokens":1,"output_tokens":1}}}}

`)}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *imageFeatureHeaderExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *imageFeatureHeaderExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *imageFeatureHeaderExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{Code: "not_implemented", Message: "HttpRequest not implemented", HTTPStatus: http.StatusNotImplemented}
}

func (e *imageFeatureHeaderExecutor) Features() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.features...)
}

func ampBinaryPainterModelDefaultForTest(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	path := filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "dev", "amp-binary-parity-baseline.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Amp binary parity baseline: %v", err)
	}
	var baseline struct {
		Signals struct {
			SettingDefaults []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
				Scope string `json:"scope"`
			} `json:"setting_defaults"`
		} `json:"signals"`
	}
	if err := json.Unmarshal(raw, &baseline); err != nil {
		t.Fatalf("parse Amp binary parity baseline: %v", err)
	}
	for _, setting := range baseline.Signals.SettingDefaults {
		if setting.Name == "painter.model" {
			if setting.Scope != "local-runtime" {
				t.Fatalf("painter.model baseline scope = %q, want local-runtime", setting.Scope)
			}
			return setting.Value
		}
	}
	// gaac893 removed the painter.model setting from the Amp binary; the
	// images endpoint keeps the last binary default as its frozen choice.
	return "gpt-image-2"
}

func newImagesFeatureHeaderHandler(t *testing.T, executor *imageFeatureHeaderExecutor) *OpenAIAPIHandler {
	t.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:       "image-feature-auth",
		Provider: "openai",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "image-feature@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("manager.Register: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: defaultImagesMainModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})
	return NewOpenAIAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
}

func assertUnsupportedImagesModelResponse(t *testing.T, resp *httptest.ResponseRecorder, model string) {
	t.Helper()

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", resp.Code, http.StatusBadRequest, resp.Body.String())
	}

	message := gjson.GetBytes(resp.Body.Bytes(), "error.message").String()
	expectedMessage := "Model " + model + " is not supported on " + imagesGenerationsPath + " or " + imagesEditsPath + ". Use " + defaultImagesToolModel + "."
	if message != expectedMessage {
		t.Fatalf("error message = %q, want %q", message, expectedMessage)
	}
	if errorType := gjson.GetBytes(resp.Body.Bytes(), "error.type").String(); errorType != "invalid_request_error" {
		t.Fatalf("error type = %q, want invalid_request_error", errorType)
	}
}

func TestImagesModelValidationAllowsGPTImage2WithOptionalPrefix(t *testing.T) {
	for _, model := range []string{"gpt-image-2", "codex/gpt-image-2"} {
		if !isSupportedImagesModel(model) {
			t.Fatalf("expected %s to be supported", model)
		}
	}
	if isSupportedImagesModel("gpt-5.4-mini") {
		t.Fatal("expected gpt-5.4-mini to be rejected")
	}
}

func TestImagesToolDefaultMatchesAmpBinaryPainterModelDefault(t *testing.T) {
	if got, want := defaultImagesToolModel, ampBinaryPainterModelDefaultForTest(t); got != want {
		t.Fatalf("default image tool model = %q, want %q from Amp binary painter.model default", got, want)
	}
	if !isSupportedImagesModel(defaultImagesToolModel) {
		t.Fatalf("default image tool model %q is not accepted by image endpoints", defaultImagesToolModel)
	}
}

func TestImagesGenerationsRejectsUnsupportedModel(t *testing.T) {
	handler := &OpenAIAPIHandler{}
	body := strings.NewReader(`{"model":"gpt-5.4-mini","prompt":"draw a square"}`)

	resp := performImagesEndpointRequest(t, imagesGenerationsPath, "application/json", body, handler.ImagesGenerations)

	assertUnsupportedImagesModelResponse(t, resp, "gpt-5.4-mini")
}

func TestImagesEditsJSONRejectsUnsupportedModel(t *testing.T) {
	handler := &OpenAIAPIHandler{}
	body := strings.NewReader(`{"model":"gpt-5.4-mini","prompt":"edit this","images":[{"image_url":"data:image/png;base64,AA=="}]}`)

	resp := performImagesEndpointRequest(t, imagesEditsPath, "application/json", body, handler.ImagesEdits)

	assertUnsupportedImagesModelResponse(t, resp, "gpt-5.4-mini")
}

func TestImagesEditsMultipartRejectsUnsupportedModel(t *testing.T) {
	handler := &OpenAIAPIHandler{}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("model", "gpt-5.4-mini"); err != nil {
		t.Fatalf("write model field: %v", err)
	}
	if err := writer.WriteField("prompt", "edit this"); err != nil {
		t.Fatalf("write prompt field: %v", err)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}

	resp := performImagesEndpointRequest(t, imagesEditsPath, writer.FormDataContentType(), &body, handler.ImagesEdits)

	assertUnsupportedImagesModelResponse(t, resp, "gpt-5.4-mini")
}

func TestImagesGenerations_DisableImageGeneration_Returns404(t *testing.T) {
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{DisableImageGeneration: internalconfig.DisableImageGenerationAll}, nil)
	handler := NewOpenAIAPIHandler(base)
	body := strings.NewReader(`{"prompt":"draw a square"}`)

	resp := performImagesEndpointRequest(t, imagesGenerationsPath, "application/json", body, handler.ImagesGenerations)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d: %s", resp.Code, http.StatusNotFound, resp.Body.String())
	}
}

func TestImagesEdits_DisableImageGeneration_Returns404(t *testing.T) {
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{DisableImageGeneration: internalconfig.DisableImageGenerationAll}, nil)
	handler := NewOpenAIAPIHandler(base)
	body := strings.NewReader(`{"prompt":"edit this","images":[{"image_url":"data:image/png;base64,AA=="}]}`)

	resp := performImagesEndpointRequest(t, imagesEditsPath, "application/json", body, handler.ImagesEdits)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d: %s", resp.Code, http.StatusNotFound, resp.Body.String())
	}
}

func TestImagesGenerations_DisableImageGenerationChat_DoesNotReturn404(t *testing.T) {
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{DisableImageGeneration: internalconfig.DisableImageGenerationChat}, nil)
	handler := NewOpenAIAPIHandler(base)
	body := strings.NewReader(`{"model":"gpt-5.4-mini","prompt":"draw a square"}`)

	resp := performImagesEndpointRequest(t, imagesGenerationsPath, "application/json", body, handler.ImagesGenerations)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", resp.Code, http.StatusBadRequest, resp.Body.String())
	}
}

func TestImagesGenerationsUsesAmpImageGenerationFeatureHeader(t *testing.T) {
	executor := &imageFeatureHeaderExecutor{}
	handler := newImagesFeatureHeaderHandler(t, executor)
	body := strings.NewReader(`{"prompt":"draw a square"}`)

	resp := performImagesEndpointRequest(t, imagesGenerationsPath, "application/json", body, handler.ImagesGenerations)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", resp.Code, http.StatusOK, resp.Body.String())
	}
	features := executor.Features()
	if len(features) != 1 {
		t.Fatalf("feature captures = %#v, want one call", features)
	}
	if features[0] != ampImageGenerationFeature {
		t.Fatalf("X-Amp-Feature = %q, want %q", features[0], ampImageGenerationFeature)
	}
}

func TestImagesGenerationsPreservesExplicitAmpFeatureHeader(t *testing.T) {
	executor := &imageFeatureHeaderExecutor{}
	handler := newImagesFeatureHeaderHandler(t, executor)
	body := strings.NewReader(`{"prompt":"draw a square"}`)

	resp := performImagesEndpointRequest(t, imagesGenerationsPath, "application/json", body, handler.ImagesGenerations, http.Header{"X-Amp-Feature": {"amp.painter"}})

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", resp.Code, http.StatusOK, resp.Body.String())
	}
	features := executor.Features()
	if len(features) != 1 {
		t.Fatalf("feature captures = %#v, want one call", features)
	}
	if features[0] != "amp.painter" {
		t.Fatalf("X-Amp-Feature = %q, want explicit amp.painter", features[0])
	}
}

func TestImagesEdits_DisableImageGenerationChat_DoesNotReturn404(t *testing.T) {
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{DisableImageGeneration: internalconfig.DisableImageGenerationChat}, nil)
	handler := NewOpenAIAPIHandler(base)
	body := strings.NewReader(`{"model":"gpt-5.4-mini","prompt":"edit this","images":[{"image_url":"data:image/png;base64,AA=="}]}`)

	resp := performImagesEndpointRequest(t, imagesEditsPath, "application/json", body, handler.ImagesEdits)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", resp.Code, http.StatusBadRequest, resp.Body.String())
	}
}
