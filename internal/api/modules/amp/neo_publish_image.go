package amp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	neoPublishImageRequestLimit = 8
	neoPublishImageResultBody   = 7 * 1024 * 1024
)

type neoPublishImageRequest struct {
	RequestID  string `json:"requestId"`
	ThreadID   string `json:"threadId"`
	RunnerID   string `json:"runnerId"`
	BrokerID   string `json:"brokerId"`
	SessionID  string `json:"sessionId"`
	Generation uint64 `json:"sessionGeneration"`
	Path       string `json:"path"`
}

type neoPublishImageDelivery struct {
	data []byte
	code string
}

type neoPendingPublishImage struct {
	request neoPublishImageRequest
	result  chan neoPublishImageDelivery
}

type neoPublishImageResult struct {
	RequestID  string `json:"requestId"`
	ThreadID   string `json:"threadId"`
	RunnerID   string `json:"runnerId"`
	BrokerID   string `json:"brokerId"`
	SessionID  string `json:"sessionId"`
	Generation uint64 `json:"sessionGeneration"`
	Data       string `json:"data,omitempty"`
	ErrorCode  string `json:"errorCode,omitempty"`
}

func validateNeoPublishImagePath(value any) (string, error) {
	relative, ok := value.(string)
	if !ok || relative == "" || len(relative) > 4096 || strings.TrimSpace(relative) != relative || strings.ContainsRune(relative, 0) || strings.Contains(relative, "\\") {
		return "", errors.New("publish_image path must be a POSIX workspace-relative path")
	}
	if strings.HasPrefix(relative, "/") || path.IsAbs(relative) || relative == "." || path.Clean(relative) != relative {
		return "", errors.New("publish_image path must be a POSIX workspace-relative path")
	}
	for _, component := range strings.Split(relative, "/") {
		if component == "" || component == "." || component == ".." {
			return "", errors.New("publish_image path must not contain traversal")
		}
	}
	return relative, nil
}

func (a *neoActor) executeLocalPublishImageTool(ctx context.Context, input map[string]any) (map[string]any, error) {
	relative, err := validateNeoPublishImagePath(input["path"])
	if err != nil {
		return nil, err
	}
	description := ""
	if raw, exists := input["description"]; exists {
		value, ok := raw.(string)
		if !ok {
			return nil, errors.New("publish_image description must be a string")
		}
		description = strings.TrimSpace(value)
	}
	if !neoLocalBrokerSafeText(description, 1024, true) {
		return nil, errors.New("publish_image description is invalid")
	}
	if a == nil || a.runtime == nil {
		return nil, errors.New("publish_image is unavailable")
	}
	a.mu.Lock()
	runnerID := strings.TrimSpace(firstNonEmptyString(a.meta["runnerId"], a.meta["runnerID"]))
	threadID := a.threadID
	sandbox := strings.EqualFold(firstNonEmptyString(a.bootstrapExecutorType, a.meta["executorType"]), "sandbox")
	environmentAmpURL := strings.TrimRight(strings.TrimSpace(stringValue(a.environment["ampURL"])), "/")
	a.mu.Unlock()
	localWorkspace := a.localThreadToolWorkingDirectory()

	var raw []byte
	orbImage := false
	switch {
	case runnerID != "":
		raw, err = a.requestBrokerPublishImage(ctx, runnerID, threadID, relative)
	case sandbox || a.runtime.orbManagerFor().live(threadID) != nil:
		orbImage = true
		raw, err = a.runtime.orbManagerFor().readWorkspaceFile(ctx, threadID, relative)
	default:
		raw, err = readNeoWorkspaceImage(localWorkspace, relative)
	}
	if err != nil {
		return nil, err
	}
	raw, mediaType, err := validateNeoAttachmentBytes(raw, http.DetectContentType(raw))
	if err != nil {
		return nil, err
	}
	if orbImage {
		origin := ""
		if orbOrigin, ok := neoPublishImageOrigin(neoOrbPortalBaseURL(a.runtime.configSnapshot())); ok {
			origin = orbOrigin
		}
		return neoStorePublishedImage(raw, mediaType, origin, description)
	}
	origin, _ := neoPublishImageOrigin(neoProxyBaseURL(a.runtime.configSnapshot()))
	if environmentOrigin, ok := neoPublishImageOrigin(environmentAmpURL); ok {
		origin = environmentOrigin
	}
	return neoStorePublishedImage(raw, mediaType, origin, description)
}

func neoStorePublishedImage(raw []byte, mediaType, origin, description string) (map[string]any, error) {
	if origin == "" {
		return nil, errors.New("publish_image attachment URL is unavailable")
	}
	attachmentID, err := writeNeoLocalAttachment(raw, mediaType, origin)
	if err != nil {
		return nil, errors.New("publish_image could not store the image")
	}
	image := map[string]any{"type": "image", "mimeType": mediaType, "url": origin + "/attachments/" + attachmentID}
	result := map[string]any{"images": []any{image}}
	if description != "" {
		result["description"] = description
	}
	return result, nil
}

func (a *neoActor) requestBrokerPublishImage(ctx context.Context, runnerID, threadID, relative string) ([]byte, error) {
	owner := a.runtime.store.userActorForOwner(a.threadToolOwnerID())
	if owner == nil {
		return nil, errors.New("publish_image runner is unavailable")
	}
	now := time.Now()
	owner.mu.Lock()
	runner, ok := owner.userRunners[runnerID]
	if !ok || runner.brokerID == "" || runner.updatedAt.Before(now.Add(-neoLocalBrokerHeartbeatTTL)) || !stringSliceContains(runner.runningThreads, threadID) {
		owner.mu.Unlock()
		return nil, errors.New("publish_image runner assignment is unavailable")
	}
	if owner.pendingPublishImages == nil {
		owner.pendingPublishImages = map[string]*neoPendingPublishImage{}
	}
	if len(owner.pendingPublishImages) >= neoPublishImageRequestLimit {
		owner.mu.Unlock()
		return nil, errors.New("publish_image runner is busy")
	}
	requestID := randomBase62(24)
	request := neoPublishImageRequest{RequestID: requestID, ThreadID: threadID, RunnerID: runnerID, BrokerID: runner.brokerID, SessionID: runner.sessionID, Generation: runner.sessionGeneration, Path: relative}
	pending := &neoPendingPublishImage{request: request, result: make(chan neoPublishImageDelivery, 1)}
	owner.pendingPublishImages[requestID] = pending
	owner.mu.Unlock()
	defer func() {
		owner.mu.Lock()
		if owner.pendingPublishImages[requestID] == pending {
			delete(owner.pendingPublishImages, requestID)
		}
		owner.mu.Unlock()
	}()
	select {
	case delivery := <-pending.result:
		if delivery.code != "" {
			return nil, neoPublishImageDeliveryError(delivery.code)
		}
		return delivery.data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func neoPublishImageDeliveryError(code string) error {
	switch code {
	case "read_failed", "too_large", "stale_assignment", "stale_session":
		return fmt.Errorf("publish_image runner could not read the image: %s", code)
	default:
		return errors.New("publish_image runner could not read the image")
	}
}

func validNeoPublishImageErrorCode(code string) bool {
	switch code {
	case "read_failed", "too_large", "stale_assignment":
		return true
	default:
		return false
	}
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (a *neoActor) cancelPendingPublishImagesLocked(code string) {
	for requestID, pending := range a.pendingPublishImages {
		delete(a.pendingPublishImages, requestID)
		pending.result <- neoPublishImageDelivery{code: code}
	}
}

func (a *neoActor) publishImageRequestsForHeartbeat(request neoLocalBrokerHeartbeatRequest) []neoPublishImageRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]neoPublishImageRequest, 0)
	for requestID, pending := range a.pendingPublishImages {
		item := pending.request
		if item.BrokerID != request.BrokerID || item.SessionID != request.SessionID || item.Generation != request.SessionGeneration {
			if item.BrokerID == request.BrokerID {
				delete(a.pendingPublishImages, requestID)
				pending.result <- neoPublishImageDelivery{code: "stale_session"}
			}
			continue
		}
		assigned := false
		for _, runner := range *request.Runners {
			if runner.RunnerID == item.RunnerID && stringSliceContains(*runner.RunningThreads, item.ThreadID) {
				assigned = true
				out = append(out, item)
			}
		}
		if !assigned {
			delete(a.pendingPublishImages, requestID)
			pending.result <- neoPublishImageDelivery{code: "stale_assignment"}
		}
		if len(out) == neoPublishImageRequestLimit {
			break
		}
	}
	return out
}

func (m *AmpModule) serveLocalBrokerPublishImageResult(c *gin.Context) {
	if strings.TrimSpace(c.GetHeader("Origin")) != "" {
		c.JSON(http.StatusForbidden, gin.H{"ok": false, "error": "browser_origin_forbidden"})
		return
	}
	if c.Request.Method != http.MethodPost {
		c.Header("Allow", http.MethodPost)
		c.JSON(http.StatusMethodNotAllowed, gin.H{"ok": false, "error": "method_not_allowed"})
		return
	}
	if m == nil || m.neoRuntime == nil || m.neoRuntime.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "runtime_unavailable"})
		return
	}
	if strings.TrimSpace(getClientAPIKeyFromContext(c.Request.Context())) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"ok": false, "error": "authentication_required"})
		return
	}
	ownerID := strings.TrimSpace(m.neoRuntime.neoRequestOwnerUserID(c.Request.Context()))
	if ownerID == "" || !neoRequestOwnerScopeResolved(c.Request.Context(), ownerID) {
		c.JSON(http.StatusUnauthorized, gin.H{"ok": false, "error": "owner_unavailable"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		c.JSON(http.StatusUnsupportedMediaType, gin.H{"ok": false, "error": "invalid_content_type"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, neoPublishImageResultBody)
	payload, err := io.ReadAll(c.Request.Body)
	if err != nil {
		status := http.StatusBadRequest
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			status = http.StatusRequestEntityTooLarge
		}
		c.JSON(status, gin.H{"ok": false, "error": "invalid_request"})
		return
	}
	if rejectNeoLocalBrokerDuplicateJSONFields(payload) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_request"})
		return
	}
	var result neoPublishImageResult
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&result) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_request"})
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_request"})
		return
	}
	actor := m.neoRuntime.store.userActorForOwner(ownerID)
	if actor == nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "unknown_request"})
		return
	}
	actor.mu.Lock()
	pending := actor.pendingPublishImages[result.RequestID]
	if pending == nil || pending.request.ThreadID != result.ThreadID || pending.request.RunnerID != result.RunnerID || pending.request.BrokerID != result.BrokerID || pending.request.SessionID != result.SessionID || pending.request.Generation != result.Generation {
		actor.mu.Unlock()
		c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "stale_or_unknown_request"})
		return
	}
	runner, ok := actor.userRunners[result.RunnerID]
	if !ok || runner.brokerID != result.BrokerID || runner.sessionID != result.SessionID || runner.sessionGeneration != result.Generation || !stringSliceContains(runner.runningThreads, result.ThreadID) {
		actor.mu.Unlock()
		c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "stale_assignment"})
		return
	}
	var delivery neoPublishImageDelivery
	if result.ErrorCode != "" {
		if result.Data != "" || !validNeoPublishImageErrorCode(result.ErrorCode) {
			actor.mu.Unlock()
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_result"})
			return
		}
		delivery.code = result.ErrorCode
	} else {
		if len(result.Data) > base64.StdEncoding.EncodedLen(neoAttachmentMaxImageBytes) {
			actor.mu.Unlock()
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"ok": false, "error": "result_too_large"})
			return
		}
		data, decodeErr := base64.StdEncoding.DecodeString(result.Data)
		if decodeErr != nil || len(data) == 0 || len(data) > neoAttachmentMaxImageBytes {
			actor.mu.Unlock()
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_result"})
			return
		}
		delivery.data = data
	}
	delete(actor.pendingPublishImages, result.RequestID)
	actor.mu.Unlock()
	pending.result <- delivery
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func neoPublishImageOrigin(raw string) (string, bool) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", false
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	return strings.TrimRight(parsed.String(), "/"), true
}
