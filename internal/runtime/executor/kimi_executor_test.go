package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	kimiauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kimi"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

type kimiTestRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f kimiTestRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestKimiClaudeRequestAuthIsRequestLocal(t *testing.T) {
	tests := []struct {
		name string
		auth *cliproxyauth.Auth
	}{
		{name: "nil auth"},
		{name: "nil attributes", auth: &cliproxyauth.Auth{Metadata: map[string]any{"access_token": "test-token"}}},
		{name: "empty attributes", auth: &cliproxyauth.Auth{Attributes: map[string]string{}}},
		{name: "existing attributes", auth: &cliproxyauth.Auth{Attributes: map[string]string{"base_url": "https://original.example", "custom": "value"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var originalAttributes map[string]string
			if tc.auth != nil && tc.auth.Attributes != nil {
				originalAttributes = make(map[string]string, len(tc.auth.Attributes))
				for key, value := range tc.auth.Attributes {
					originalAttributes[key] = value
				}
			}
			requestAuth := kimiClaudeRequestAuth(tc.auth)
			if requestAuth == nil || requestAuth.Attributes["base_url"] != kimiauth.KimiAPIBaseURL {
				t.Fatalf("request auth = %#v", requestAuth)
			}
			if tc.auth == nil {
				return
			}
			if requestAuth == tc.auth {
				t.Fatal("request auth reused shared auth pointer")
			}
			if !reflect.DeepEqual(tc.auth.Attributes, originalAttributes) {
				t.Fatalf("shared attributes changed to %#v", tc.auth.Attributes)
			}
			requestAuth.Attributes["custom"] = "request-value"
			if !reflect.DeepEqual(tc.auth.Attributes, originalAttributes) {
				t.Fatalf("request mutation reached shared attributes: %#v", tc.auth.Attributes)
			}
		})
	}
}

func TestKimiExecutorClaudePathsHandleNilAuthState(t *testing.T) {
	executor := NewKimiExecutor(&config.Config{})
	request := cliproxyexecutor.Request{
		Model:   "kimi-k2",
		Payload: []byte(`{"model":"kimi-k2","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`),
	}
	options := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("claude")}
	operations := []struct {
		name string
		run  func(context.Context, *cliproxyauth.Auth) error
	}{
		{name: "execute", run: func(ctx context.Context, auth *cliproxyauth.Auth) error {
			_, err := executor.Execute(ctx, auth, request, options)
			return err
		}},
		{name: "execute stream", run: func(ctx context.Context, auth *cliproxyauth.Auth) error {
			_, err := executor.ExecuteStream(ctx, auth, request, options)
			return err
		}},
		{name: "count tokens", run: func(ctx context.Context, auth *cliproxyauth.Auth) error {
			_, err := executor.CountTokens(ctx, auth, request, options)
			return err
		}},
	}
	for _, authCase := range []struct {
		name string
		auth func() *cliproxyauth.Auth
	}{
		{name: "nil auth", auth: func() *cliproxyauth.Auth { return nil }},
		{name: "nil attributes", auth: func() *cliproxyauth.Auth {
			return &cliproxyauth.Auth{Metadata: map[string]any{"access_token": "test-token"}}
		}},
	} {
		for _, operation := range operations {
			t.Run(authCase.name+"/"+operation.name, func(t *testing.T) {
				auth := authCase.auth()
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if err := operation.run(ctx, auth); err == nil {
					t.Fatal("operation unexpectedly succeeded with canceled context")
				}
				if auth != nil && auth.Attributes != nil {
					t.Fatalf("shared auth attributes changed to %#v", auth.Attributes)
				}
			})
		}
	}
}

func TestNormalizeKimiToolMessageLinks_UsesCallIDFallback(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","tool_calls":[{"id":"list_directory:1","type":"function","function":{"name":"list_directory","arguments":"{}"}}]},
			{"role":"tool","call_id":"list_directory:1","content":"[]"}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	got := gjson.GetBytes(out, "messages.1.tool_call_id").String()
	if got != "list_directory:1" {
		t.Fatalf("messages.1.tool_call_id = %q, want %q", got, "list_directory:1")
	}
}

func TestKimiNormalizeRequestSchemasPreservesNullability(t *testing.T) {
	body := []byte(`{
		"model":"kimi-k3",
		"tools":[{"type":"function","function":{"name":"submit_review","parameters":{
			"type":"object",
			"properties":{
				"comments":{"type":"array","items":{"type":"object","properties":{
					"commentType":{"type":["string","null"],"enum":["bug","unknown",null]},
					"severity":{"type":"string","enum":["high","low",null]},
					"source":{"type":["string","null"]},
					"description":{"type":"object","properties":{"value":{"type":"string"}}}
				},"required":["commentType","severity","source"]}}
			},"required":["comments"]
		}}}],
		"response_format":{"type":"json_schema","json_schema":{"name":"run_check","schema":{
			"type":"object","properties":{
				"endLine":{"type":["integer","null"]},
				"verification":{"type":["string","null"],"enum":["root-runtime-traversal",null]}
			},"required":["endLine","verification"]
		}}}
	}`)

	out, err := normalizeKimiRequestSchemas(body)
	if err != nil {
		t.Fatalf("normalizeKimiRequestSchemas() error = %v", err)
	}
	for _, path := range []string{
		"tools.0.function.parameters.properties.comments.items.properties.commentType",
		"tools.0.function.parameters.properties.comments.items.properties.source",
	} {
		if got := gjson.GetBytes(out, path+".anyOf.0.type").String(); got != "string" {
			t.Fatalf("%s.anyOf.0.type = %q, want string", path, got)
		}
		if got := gjson.GetBytes(out, path+".anyOf.1.type").String(); got != "null" {
			t.Fatalf("%s.anyOf.1.type = %q, want null", path, got)
		}
	}
	commentType := "tools.0.function.parameters.properties.comments.items.properties.commentType"
	if got := gjson.GetBytes(out, commentType+".anyOf.0.enum").Raw; got != `["bug","unknown"]` {
		t.Fatalf("commentType non-null enum = %s", got)
	}
	severity := "tools.0.function.parameters.properties.comments.items.properties.severity"
	if got := gjson.GetBytes(out, severity+".type").String(); got != "string" {
		t.Fatalf("severity type = %q, want string", got)
	}
	if got := gjson.GetBytes(out, severity+".enum").Raw; got != `["high","low"]` {
		t.Fatalf("severity enum = %s, want non-null values", got)
	}
	if gjson.GetBytes(out, severity+".anyOf").Exists() {
		t.Fatal("null enum value widened a non-null string schema")
	}
	if got := gjson.GetBytes(out, "tools.0.function.parameters.properties.comments.items.properties.description.type").String(); got != "object" {
		t.Fatalf("parameter named description type = %q, want object", got)
	}
	if got := gjson.GetBytes(out, "tools.0.function.parameters.properties.comments.items.properties.description.properties.value.type").String(); got != "string" {
		t.Fatalf("nested description value type = %q, want string", got)
	}
	endLine := "response_format.json_schema.schema.properties.endLine"
	if got := gjson.GetBytes(out, endLine+".anyOf.0.type").String(); got != "integer" {
		t.Fatalf("structured-output endLine type = %q, want integer", got)
	}
	if got := gjson.GetBytes(out, endLine+".anyOf.1.type").String(); got != "null" {
		t.Fatalf("structured-output endLine nullable type = %q, want null", got)
	}
	verification := "response_format.json_schema.schema.properties.verification"
	if got := gjson.GetBytes(out, verification+".anyOf.0.type").String(); got != "string" {
		t.Fatalf("structured-output verification type = %q, want string", got)
	}
	if got := gjson.GetBytes(out, verification+".anyOf.0.enum").Raw; got != `["root-runtime-traversal"]` {
		t.Fatalf("structured-output verification enum = %s, want non-null values", got)
	}
	if got := gjson.GetBytes(out, verification+".anyOf.1.type").String(); got != "null" {
		t.Fatalf("structured-output verification nullable type = %q, want null", got)
	}
}

func TestKimiNormalizeRequestSchemasPreservesNullOnlyConstraints(t *testing.T) {
	body := []byte(`{
		"tools":[{"type":"function","function":{"name":"null_constraints","parameters":{
			"type":"object","properties":{
				"constOnly":{"type":["string","null"],"const":null},
				"enumOnly":{"type":["string","null"],"enum":[null]},
				"nullable":{"type":["string","null"]}
			}
		}}}]
	}`)

	out, err := normalizeKimiRequestSchemas(body)
	if err != nil {
		t.Fatalf("normalizeKimiRequestSchemas() error = %v", err)
	}
	constOnly := "tools.0.function.parameters.properties.constOnly"
	if got := gjson.GetBytes(out, constOnly+".type").String(); got != "null" {
		t.Fatalf("const-only type = %s", got)
	}
	enumOnly := "tools.0.function.parameters.properties.enumOnly"
	if got := gjson.GetBytes(out, enumOnly+".type").String(); got != "null" {
		t.Fatalf("enum-only type = %s", got)
	}
	nullable := "tools.0.function.parameters.properties.nullable"
	if got := gjson.GetBytes(out, nullable+".anyOf.0.type").String(); got != "string" {
		t.Fatalf("ordinary nullable concrete type = %q, want string", got)
	}
	if got := gjson.GetBytes(out, nullable+".anyOf.1.type").String(); got != "null" {
		t.Fatalf("ordinary nullable second type = %q, want null", got)
	}
}

func TestKimiNormalizeRequestSchemasRejectsUnrepresentableNullableSchemas(t *testing.T) {
	for name, body := range map[string]string{
		"existing anyOf":    `{"tools":[{"type":"function","function":{"parameters":{"type":"object","properties":{"value":{"type":["string","null"],"anyOf":[{"minLength":1}]}}}}}]}`,
		"MFJS boolean enum": `{"response_format":{"type":"json_schema","json_schema":{"schema":{"type":"object","properties":{"value":{"type":["boolean","null"],"enum":[true,null]}}}}}}`,
		"empty types":       `{"tools":[{"type":"function","function":{"parameters":{"type":"object","properties":{"value":{"type":[]}}}}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := normalizeKimiRequestSchemas([]byte(body))
			if err == nil || !strings.Contains(err.Error(), "unsupported") {
				t.Fatalf("normalizeKimiRequestSchemas() error = %v", err)
			}
			status, ok := err.(interface{ StatusCode() int })
			if !ok || status.StatusCode() != http.StatusBadRequest {
				t.Fatalf("normalizeKimiRequestSchemas() status = %v, want %d", err, http.StatusBadRequest)
			}
			if !strings.Contains(err.Error(), "properties.value") {
				t.Fatalf("normalizeKimiRequestSchemas() error lacks schema path: %v", err)
			}
		})
	}
}

func TestKimiExecutorNativeUpstreamStatusClassification(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantNeutral bool
	}{
		{name: "transient", body: `{"error":{"type":"server_error","message":"upstream unavailable"}}`, wantNeutral: true},
		{name: "authentication", body: `{"error":{"type":"authentication_error","message":"invalid api key"}}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", kimiTestRoundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadGateway,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(tc.body)),
				}, nil
			}))
			executor := NewKimiExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "test"}}
			_, err := executor.Execute(ctx, auth, cliproxyexecutor.Request{
				Model:   "kimi-k2",
				Payload: []byte(`{"model":"kimi-k2","messages":[{"role":"user","content":"hi"}]}`),
			}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")})
			if err == nil {
				t.Fatal("Execute() error = nil, want upstream status error")
			}
			var status interface{ StatusCode() int }
			if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
				t.Fatalf("upstream status = %v, want %d", err, http.StatusBadGateway)
			}
			var neutral interface{ AuthStateNeutral() bool }
			if !errors.As(err, &neutral) {
				t.Fatalf("error %T does not expose AuthStateNeutral", err)
			}
			if got := neutral.AuthStateNeutral(); got != tc.wantNeutral {
				t.Fatalf("AuthStateNeutral() for %v = %t, want %t", err, got, tc.wantNeutral)
			}
		})
	}
}

func TestKimiExecutorNativeTransportFailureIsAuthStateNeutral(t *testing.T) {
	wantErr := errors.New("upstream transport failed")
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", kimiTestRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, wantErr
	}))
	executor := NewKimiExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "test"}}
	_, err := executor.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "kimi-k2",
		Payload: []byte(`{"model":"kimi-k2","messages":[{"role":"user","content":"hi"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Execute() error = %v, want %v", err, wantErr)
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(err, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("transport error is not auth-state-neutral: %v", err)
	}
}

func TestKimiExecutorStreamScannerFailureDoesNotEmitCompletion(t *testing.T) {
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", kimiTestRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(strings.Repeat("x", 1_048_577))),
		}, nil
	}))
	executor := NewKimiExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "test"}}
	result, err := executor.ExecuteStream(ctx, auth, cliproxyexecutor.Request{
		Model:   "kimi-k2",
		Payload: []byte(`{"model":"kimi-k2","messages":[{"role":"user","content":"hi"}],"stream":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai"), Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	var streamErr error
	for chunk := range result.Chunks {
		if len(chunk.Payload) > 0 {
			t.Fatalf("scanner failure emitted completion payload first: %q", chunk.Payload)
		}
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil {
		t.Fatal("scanner failure did not emit an error")
	}
	var neutral interface{ AuthStateNeutral() bool }
	if !errors.As(streamErr, &neutral) || !neutral.AuthStateNeutral() {
		t.Fatalf("scanner error is not auth-state-neutral: %v", streamErr)
	}
}

func TestNormalizeKimiToolMessageLinks_InferSinglePendingID(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","tool_calls":[{"id":"call_123","type":"function","function":{"name":"read_file","arguments":"{}"}}]},
			{"role":"tool","content":"file-content"}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	got := gjson.GetBytes(out, "messages.1.tool_call_id").String()
	if got != "call_123" {
		t.Fatalf("messages.1.tool_call_id = %q, want %q", got, "call_123")
	}
}

func TestNormalizeKimiToolMessageLinks_AmbiguousMissingIDIsNotInferred(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","tool_calls":[
				{"id":"call_1","type":"function","function":{"name":"list_directory","arguments":"{}"}},
				{"id":"call_2","type":"function","function":{"name":"read_file","arguments":"{}"}}
			]},
			{"role":"tool","content":"result-without-id"}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	if gjson.GetBytes(out, "messages.1.tool_call_id").Exists() {
		t.Fatalf("messages.1.tool_call_id should be absent for ambiguous case, got %q", gjson.GetBytes(out, "messages.1.tool_call_id").String())
	}
}

func TestNormalizeKimiToolMessageLinks_PreservesExistingToolCallID(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"list_directory","arguments":"{}"}}]},
			{"role":"tool","tool_call_id":"call_1","call_id":"different-id","content":"result"}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	got := gjson.GetBytes(out, "messages.1.tool_call_id").String()
	if got != "call_1" {
		t.Fatalf("messages.1.tool_call_id = %q, want %q", got, "call_1")
	}
}

func TestNormalizeKimiToolMessageLinks_InheritsPreviousReasoningForAssistantToolCalls(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","content":"plan","reasoning_content":"previous reasoning"},
			{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"list_directory","arguments":"{}"}}]}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	got := gjson.GetBytes(out, "messages.1.reasoning_content").String()
	if got != "previous reasoning" {
		t.Fatalf("messages.1.reasoning_content = %q, want %q", got, "previous reasoning")
	}
}

func TestNormalizeKimiToolMessageLinks_InsertsFallbackReasoningWhenMissing(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"list_directory","arguments":"{}"}}]}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	reasoning := gjson.GetBytes(out, "messages.0.reasoning_content")
	if !reasoning.Exists() {
		t.Fatalf("messages.0.reasoning_content should exist")
	}
	if reasoning.String() != "[reasoning unavailable]" {
		t.Fatalf("messages.0.reasoning_content = %q, want %q", reasoning.String(), "[reasoning unavailable]")
	}
}

func TestNormalizeKimiToolMessageLinks_UsesContentAsReasoningFallback(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","content":[{"type":"text","text":"first line"},{"type":"text","text":"second line"}],"tool_calls":[{"id":"call_1","type":"function","function":{"name":"list_directory","arguments":"{}"}}]}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	got := gjson.GetBytes(out, "messages.0.reasoning_content").String()
	if got != "first line\nsecond line" {
		t.Fatalf("messages.0.reasoning_content = %q, want %q", got, "first line\nsecond line")
	}
}

func TestNormalizeKimiToolMessageLinks_ReplacesEmptyReasoningContent(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","content":"assistant summary","tool_calls":[{"id":"call_1","type":"function","function":{"name":"list_directory","arguments":"{}"}}],"reasoning_content":""}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	got := gjson.GetBytes(out, "messages.0.reasoning_content").String()
	if got != "assistant summary" {
		t.Fatalf("messages.0.reasoning_content = %q, want %q", got, "assistant summary")
	}
}

func TestNormalizeKimiToolMessageLinks_PreservesExistingAssistantReasoning(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"list_directory","arguments":"{}"}}],"reasoning_content":"keep me"}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	got := gjson.GetBytes(out, "messages.0.reasoning_content").String()
	if got != "keep me" {
		t.Fatalf("messages.0.reasoning_content = %q, want %q", got, "keep me")
	}
}

func TestNormalizeKimiToolMessageLinks_RepairsIDsAndReasoningTogether(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"list_directory","arguments":"{}"}}],"reasoning_content":"r1"},
			{"role":"tool","call_id":"call_1","content":"[]"},
			{"role":"assistant","tool_calls":[{"id":"call_2","type":"function","function":{"name":"read_file","arguments":"{}"}}]},
			{"role":"tool","call_id":"call_2","content":"file"}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	if got := gjson.GetBytes(out, "messages.1.tool_call_id").String(); got != "call_1" {
		t.Fatalf("messages.1.tool_call_id = %q, want %q", got, "call_1")
	}
	if got := gjson.GetBytes(out, "messages.3.tool_call_id").String(); got != "call_2" {
		t.Fatalf("messages.3.tool_call_id = %q, want %q", got, "call_2")
	}
	if got := gjson.GetBytes(out, "messages.2.reasoning_content").String(); got != "r1" {
		t.Fatalf("messages.2.reasoning_content = %q, want %q", got, "r1")
	}
}

func TestNormalizeKimiToolMessageLinks_DropsEmptyAssistantWithoutToolLink(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"user","content":"start"},
			{"role":"assistant","content":""},
			{"role":"assistant","content":"   "},
			{"role":"assistant","content":"","tool_calls":null},
			{"role":"assistant","content":[{"type":"text","text":"  "}]},
			{"role":"assistant"},
			{"role":"assistant","content":"keep"},
			{"role":"user","content":"next"}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	messages := gjson.GetBytes(out, "messages").Array()
	if len(messages) != 3 {
		t.Fatalf("messages length = %d, want 3, raw = %s", len(messages), gjson.GetBytes(out, "messages").Raw)
	}
	if got := messages[0].Get("content").String(); got != "start" {
		t.Fatalf("messages.0.content = %q, want %q", got, "start")
	}
	if got := messages[1].Get("content").String(); got != "keep" {
		t.Fatalf("messages.1.content = %q, want %q", got, "keep")
	}
	if got := messages[2].Get("content").String(); got != "next" {
		t.Fatalf("messages.2.content = %q, want %q", got, "next")
	}
}

func TestNormalizeKimiToolMessageLinks_PreservesAssistantWithToolLinkOrReasoning(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"list_directory","arguments":"{}"}}]},
			{"role":"assistant","content":"","function_call":{"name":"legacy_call","arguments":"{}"}},
			{"role":"assistant","content":"","reasoning_content":"thought"},
			{"role":"assistant","content":[{"type":"text","text":" visible "}]}
		]
	}`)

	out, err := normalizeKimiToolMessageLinks(body)
	if err != nil {
		t.Fatalf("normalizeKimiToolMessageLinks() error = %v", err)
	}

	messages := gjson.GetBytes(out, "messages").Array()
	if len(messages) != 4 {
		t.Fatalf("messages length = %d, want 4, raw = %s", len(messages), gjson.GetBytes(out, "messages").Raw)
	}
	if !messages[0].Get("tool_calls").Exists() {
		t.Fatalf("messages.0.tool_calls should exist")
	}
	if !messages[1].Get("function_call").Exists() {
		t.Fatalf("messages.1.function_call should exist")
	}
	if got := messages[2].Get("reasoning_content").String(); got != "thought" {
		t.Fatalf("messages.2.reasoning_content = %q, want %q", got, "thought")
	}
	if got := messages[3].Get("content.0.text").String(); got != " visible " {
		t.Fatalf("messages.3.content.0.text = %q, want %q", got, " visible ")
	}
}
