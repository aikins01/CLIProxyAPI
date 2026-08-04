package executor

import (
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"time"
	"weak"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

// RequestedModelMetadataKey stores the client-requested model name in Options.Metadata.
const RequestedModelMetadataKey = "requested_model"

// RequestPathMetadataKey stores the inbound HTTP request path (e.g. "/v1/images/generations") in Options.Metadata.
// It is optional and may be absent for non-HTTP executions.
const RequestPathMetadataKey = "request_path"

// DisallowFreeAuthMetadataKey instructs auth selection to skip known free-tier credentials.
const DisallowFreeAuthMetadataKey = "disallow_free_auth"

const (
	// PinnedAuthMetadataKey locks execution to a specific auth ID.
	PinnedAuthMetadataKey = "pinned_auth_id"
	// SelectedAuthMetadataKey stores the auth ID selected by the scheduler.
	SelectedAuthMetadataKey = "selected_auth_id"
	// SelectedAuthCallbackMetadataKey carries an optional callback invoked with the selected auth ID.
	SelectedAuthCallbackMetadataKey = "selected_auth_callback"
	// ExecutionSessionMetadataKey identifies a long-lived downstream execution session.
	ExecutionSessionMetadataKey = "execution_session_id"
)

// Request encapsulates the translated payload that will be sent to a provider executor.
type Request struct {
	// Model is the upstream model identifier after translation.
	Model string
	// Payload is the provider specific JSON payload.
	Payload []byte
	// Format represents the provider payload schema.
	Format sdktranslator.Format
	// Metadata carries optional provider specific execution hints.
	Metadata map[string]any
}

// Options controls execution behavior for both streaming and non-streaming calls.
type Options struct {
	// Stream toggles streaming mode.
	Stream bool
	// Alt carries optional alternate format hint (e.g. SSE JSON key).
	Alt string
	// Headers are forwarded to the provider request builder.
	Headers http.Header
	// Query contains optional query string parameters.
	Query url.Values
	// OriginalRequest preserves the inbound request bytes prior to translation.
	OriginalRequest []byte
	// SourceFormat identifies the inbound schema.
	SourceFormat sdktranslator.Format
	// Metadata carries extra execution hints shared across selection and executors.
	Metadata map[string]any
}

// Response wraps either a full provider response or metadata for streaming flows.
type Response struct {
	// Payload is the provider response in the executor format.
	Payload []byte
	// Metadata exposes optional structured data for translators.
	Metadata map[string]any
	// Headers carries upstream HTTP response headers for passthrough to clients.
	Headers http.Header
}

// StreamChunk represents a single streaming payload unit emitted by provider executors.
type StreamChunk struct {
	// Payload is a raw provider fragment. Framing bytes must be preserved because consumers
	// concatenate adjacent payloads without inserting separators.
	Payload []byte
	// Err reports any terminal error encountered while producing chunks.
	Err error
}

// StreamResult wraps the streaming response, providing both the chunk channel
// and the upstream HTTP response headers captured before streaming begins.
// Keepalive and bootstrap metadata set through its methods belongs to this
// pointer and is not preserved by copying the struct. Wrappers must consume
// that metadata from the original and apply it to the replacement result.
type StreamResult struct {
	// Headers carries upstream HTTP response headers from the initial connection.
	Headers http.Header
	// Chunks is the channel of streaming payload units.
	Chunks <-chan StreamChunk
}

type streamResultMetadata struct {
	keepAliveInterval    time.Duration
	hasKeepAliveInterval bool
	bootstrapCommitted   bool
}

var streamResultMetadataStore = struct {
	sync.Mutex
	values map[weak.Pointer[StreamResult]]streamResultMetadata
}{values: make(map[weak.Pointer[StreamResult]]streamResultMetadata)}

func removeStreamResultMetadata(key weak.Pointer[StreamResult]) {
	streamResultMetadataStore.Lock()
	delete(streamResultMetadataStore.values, key)
	streamResultMetadataStore.Unlock()
}

// SetKeepAliveInterval records the executor's preferred downstream keepalive interval.
func (r *StreamResult) SetKeepAliveInterval(interval time.Duration) {
	if r == nil {
		return
	}
	key := weak.Make(r)
	streamResultMetadataStore.Lock()
	metadata, exists := streamResultMetadataStore.values[key]
	metadata.keepAliveInterval = interval
	metadata.hasKeepAliveInterval = true
	streamResultMetadataStore.values[key] = metadata
	streamResultMetadataStore.Unlock()
	if !exists {
		runtime.AddCleanup(r, removeStreamResultMetadata, key)
	}
	runtime.KeepAlive(r)
}

// TakeKeepAliveInterval returns and removes the executor's downstream keepalive interval.
func (r *StreamResult) TakeKeepAliveInterval() *time.Duration {
	if r == nil {
		return nil
	}
	key := weak.Make(r)
	streamResultMetadataStore.Lock()
	metadata, exists := streamResultMetadataStore.values[key]
	hasKeepAliveInterval := exists && metadata.hasKeepAliveInterval
	interval := metadata.keepAliveInterval
	if exists {
		metadata.hasKeepAliveInterval = false
		if metadata.bootstrapCommitted {
			streamResultMetadataStore.values[key] = metadata
		} else {
			delete(streamResultMetadataStore.values, key)
		}
	}
	streamResultMetadataStore.Unlock()
	runtime.KeepAlive(r)
	if !hasKeepAliveInterval {
		return nil
	}
	return &interval
}

// SetBootstrapCommitted marks an accepted upstream request that must not be retried and whose
// downstream streaming response may be committed before the first payload is available.
func (r *StreamResult) SetBootstrapCommitted() {
	if r == nil {
		return
	}
	key := weak.Make(r)
	streamResultMetadataStore.Lock()
	metadata, exists := streamResultMetadataStore.values[key]
	metadata.bootstrapCommitted = true
	streamResultMetadataStore.values[key] = metadata
	streamResultMetadataStore.Unlock()
	if !exists {
		runtime.AddCleanup(r, removeStreamResultMetadata, key)
	}
	runtime.KeepAlive(r)
}

// BootstrapCommitted reports whether retrying could duplicate accepted upstream work and the
// downstream streaming response may be committed before the first payload is available.
func (r *StreamResult) BootstrapCommitted() bool {
	if r == nil {
		return false
	}
	key := weak.Make(r)
	streamResultMetadataStore.Lock()
	metadata := streamResultMetadataStore.values[key]
	streamResultMetadataStore.Unlock()
	runtime.KeepAlive(r)
	return metadata.bootstrapCommitted
}

// TakeBootstrapCommitted reports and removes the internal bootstrap commitment marker.
func (r *StreamResult) TakeBootstrapCommitted() bool {
	if r == nil {
		return false
	}
	key := weak.Make(r)
	streamResultMetadataStore.Lock()
	metadata, exists := streamResultMetadataStore.values[key]
	committed := exists && metadata.bootstrapCommitted
	if exists {
		metadata.bootstrapCommitted = false
		if metadata.hasKeepAliveInterval {
			streamResultMetadataStore.values[key] = metadata
		} else {
			delete(streamResultMetadataStore.values, key)
		}
	}
	streamResultMetadataStore.Unlock()
	runtime.KeepAlive(r)
	return committed
}

// StatusError represents an error that carries an HTTP-like status code.
// Provider executors should implement this when possible to enable
// better auth state updates on failures (e.g., 401/402/429).
type StatusError interface {
	error
	StatusCode() int
}
