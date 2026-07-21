package amp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/misc"
	log "github.com/sirupsen/logrus"
)

func removeQueryValuesMatching(req *http.Request, key string, match string) {
	if req == nil || req.URL == nil || match == "" {
		return
	}

	q := req.URL.Query()
	values, ok := q[key]
	if !ok || len(values) == 0 {
		return
	}

	kept := make([]string, 0, len(values))
	for _, v := range values {
		if v == match {
			continue
		}
		kept = append(kept, v)
	}

	if len(kept) == 0 {
		q.Del(key)
	} else {
		q[key] = kept
	}
	req.URL.RawQuery = q.Encode()
}

// readCloser wraps a reader and forwards Close to a separate closer.
// Used to restore peeked bytes while preserving upstream body Close behavior.
type readCloser struct {
	r io.Reader
	c io.Closer
}

func (rc *readCloser) Read(p []byte) (int, error) { return rc.r.Read(p) }
func (rc *readCloser) Close() error               { return rc.c.Close() }

type ampProxyInternalMethodContextKey struct{}
type ampProxyThreadListAugmenterContextKey struct{}
type ampProxyThreadDeleteContextKey struct{}
type ampProxyThreadSearchMergeContextKey struct{}

type ampProxyThreadDelete struct {
	apply   func() error
	restore func()
}

type ampProxyThreadListAugmenter struct {
	limit              int
	offset             int
	upstreamOverfetch  int
	includeEmpty       bool
	includeArchived    bool
	threadIDs          map[string]bool
	excludedLabelNames map[string]bool
	load               func(int) []any
	selectedLoad       func(map[string]bool) []any
}

// createReverseProxy creates a reverse proxy handler for Amp upstream
// with automatic gzip decompression via ModifyResponse
func createReverseProxy(upstreamURL string, secretSource SecretSource) (*httputil.ReverseProxy, error) {
	return createReverseProxyWithClientVersionOverride(upstreamURL, secretSource, "")
}

func createReverseProxyWithClientVersionOverride(upstreamURL string, secretSource SecretSource, clientVersionOverride string) (*httputil.ReverseProxy, error) {
	clientVersionOverride = strings.TrimSpace(clientVersionOverride)
	return createReverseProxyWithClientVersionProvider(upstreamURL, secretSource, func(context.Context) string {
		return clientVersionOverride
	})
}

func createReverseProxyWithClientVersionProvider(upstreamURL string, secretSource SecretSource, clientVersionProvider func(context.Context) string) (*httputil.ReverseProxy, error) {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return nil, fmt.Errorf("invalid amp upstream url: %w", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(parsed)
	originalDirector := proxy.Director

	// Modify outgoing requests to inject API key and fix routing
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		tagAmpProxyInternalRPCMethod(req)
		req.Host = parsed.Host
		actorRequest := actorEngineRequest(req)
		actorAuthorization := actorEngineAuthorization(req)

		// Remove client's Authorization header - it was only used for CLI Proxy API authentication
		// We will set our own Authorization using the configured upstream-api-key
		req.Header.Del("Authorization")
		req.Header.Del("X-Api-Key")
		req.Header.Del("X-Goog-Api-Key")

		// Remove proxy, client identity, and browser fingerprint headers
		misc.ScrubProxyAndFingerprintHeaders(req)
		req.Header.Del(localNeoInferenceHeader)
		if _, ok := req.Context().Value(ampProxyThreadListAugmenterContextKey{}).(ampProxyThreadListAugmenter); ok {
			req.Header.Set("Accept-Encoding", "identity")
		}
		if _, ok := req.Context().Value(ampProxyThreadDeleteContextKey{}).(ampProxyThreadDelete); ok {
			req.Header.Set("Accept-Encoding", "identity")
		}
		if _, ok := req.Context().Value(ampProxyThreadSearchMergeContextKey{}).(bool); ok {
			req.Header.Set("Accept-Encoding", "identity")
		}

		// Remove query-based credentials if they match the authenticated client API key.
		// This prevents leaking client auth material to the Amp upstream while avoiding
		// breaking unrelated upstream query parameters.
		clientKey := getClientAPIKeyFromContext(req.Context())
		removeQueryValuesMatching(req, "key", clientKey)
		removeQueryValuesMatching(req, "auth_token", clientKey)

		// Preserve correlation headers for debugging
		if req.Header.Get("X-Request-ID") == "" {
			// Could generate one here if needed
		}

		if clientVersionProvider != nil {
			if version := strings.TrimSpace(clientVersionProvider(req.Context())); version != "" {
				req.Header.Set("X-Amp-Client-Version", version)
			}
		}

		// Note: We do NOT filter Anthropic-Beta headers in the proxy path
		// Users going through ampcode.com proxy are paying for the service and should get all features
		// including 1M context window (context-1m-2025-08-07)

		if actorAuthorization != "" {
			req.Header.Set("Authorization", actorAuthorization)
			return
		}
		if actorRequest {
			return
		}

		// Inject API key from secret source (only uses upstream-api-key from config)
		if key, err := secretSource.Get(req.Context()); err == nil && key != "" {
			req.Header.Set("X-Api-Key", key)
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", key))
		} else if err != nil {
			log.Warnf("amp secret source error (continuing without auth): %v", err)
		}
	}

	// Modify incoming responses to handle gzip without Content-Encoding
	// This addresses the same issue as inline handler gzip handling, but at the proxy level
	proxy.ModifyResponse = func(resp *http.Response) error {
		deleteFinalized := false
		defer func() {
			if !deleteFinalized && resp != nil {
				restoreAmpProxyThreadDelete(resp.Request)
			}
		}()
		finalizeDelete := func() error {
			deleteFinalized = true
			return applyAmpProxyThreadDelete(resp)
		}
		// Skip if already marked as gzip (Content-Encoding set)
		if resp.Header.Get("Content-Encoding") != "" {
			return nil
		}

		// Skip streaming responses (SSE, chunked)
		if isStreamingResponse(resp) {
			return nil
		}

		// Save reference to original upstream body for proper cleanup
		originalBody := resp.Body

		// Peek at first 2 bytes to detect gzip magic bytes
		header := make([]byte, 2)
		n, _ := io.ReadFull(originalBody, header)

		// Check for gzip magic bytes (0x1f 0x8b)
		// If n < 2, we didn't get enough bytes, so it's not gzip
		if n >= 2 && header[0] == 0x1f && header[1] == 0x8b {
			// It's gzip - read the rest of the body
			rest, err := io.ReadAll(originalBody)
			if err != nil {
				// Restore what we read and return original body (preserve Close behavior)
				resp.Body = &readCloser{
					r: io.MultiReader(bytes.NewReader(header[:n]), originalBody),
					c: originalBody,
				}
				return nil
			}

			// Reconstruct complete gzipped data
			gzippedData := append(header[:n], rest...)

			// Decompress
			gzipReader, err := gzip.NewReader(bytes.NewReader(gzippedData))
			if err != nil {
				log.Warnf("amp proxy: gzip header detected but decompress failed: %v", err)
				// Close original body and return in-memory copy
				_ = originalBody.Close()
				resp.Body = io.NopCloser(bytes.NewReader(gzippedData))
				return nil
			}

			decompressed, err := io.ReadAll(gzipReader)
			_ = gzipReader.Close()
			if err != nil {
				log.Warnf("amp proxy: gzip decompress error: %v", err)
				// Close original body and return in-memory copy
				_ = originalBody.Close()
				resp.Body = io.NopCloser(bytes.NewReader(gzippedData))
				return nil
			}

			// Close original body since we're replacing with in-memory decompressed content
			_ = originalBody.Close()

			if normalized := normalizeAmpThreadListResponse(resp, decompressed); normalized != nil {
				decompressed = normalized
			}

			replaceAmpProxyResponseBody(resp, decompressed)

			log.Debugf("amp proxy: decompressed gzip response (%d -> %d bytes)", len(gzippedData), len(decompressed))
		} else {
			if ampThreadListResponse(resp) {
				rest, err := io.ReadAll(originalBody)
				if err == nil {
					body := append(header[:n], rest...)
					if normalized := normalizeAmpThreadListResponse(resp, body); normalized != nil {
						body = normalized
					}
					_ = originalBody.Close()
					replaceAmpProxyResponseBody(resp, body)
					return finalizeDelete()
				}
			}

			// Not gzip - restore peeked bytes while preserving Close behavior
			// Handle edge cases: n might be 0, 1, or 2 depending on EOF
			resp.Body = &readCloser{
				r: io.MultiReader(bytes.NewReader(header[:n]), originalBody),
				c: originalBody,
			}
		}

		return finalizeDelete()
	}

	// Error handler for proxy failures
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		restoreAmpProxyThreadDelete(req)
		// Client-side cancellations are common during polling; suppress logging in this case
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Errorf("amp upstream proxy error for %s %s: %v", req.Method, req.URL.Path, err)
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadGateway)
		_, _ = rw.Write([]byte(`{"error":"amp_upstream_proxy_error","message":"Failed to reach Amp upstream"}`))
	}

	return proxy, nil
}

func applyAmpProxyThreadDelete(resp *http.Response) error {
	if resp == nil || resp.Request == nil {
		return nil
	}
	mutation, ok := resp.Request.Context().Value(ampProxyThreadDeleteContextKey{}).(ampProxyThreadDelete)
	if !ok || mutation.apply == nil {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || resp.Body == nil {
		if mutation.restore != nil {
			mutation.restore()
		}
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		if mutation.restore != nil {
			mutation.restore()
		}
		return fmt.Errorf("amp proxy: read delete response: %w", err)
	}
	if errClose := resp.Body.Close(); errClose != nil {
		log.Debugf("amp proxy: delete response close failed: %v", errClose)
	}
	replaceAmpProxyResponseBody(resp, body)
	if !ampProxyThreadDeleteSucceeded(resp.Request, body) {
		if mutation.restore != nil {
			mutation.restore()
		}
		return nil
	}
	if err := mutation.apply(); err != nil {
		if mutation.restore != nil {
			mutation.restore()
		}
		return fmt.Errorf("amp proxy: purge local thread after upstream deletion: %w", err)
	}
	return nil
}

func restoreAmpProxyThreadDelete(req *http.Request) {
	if req == nil {
		return
	}
	mutation, ok := req.Context().Value(ampProxyThreadDeleteContextKey{}).(ampProxyThreadDelete)
	if ok && mutation.restore != nil {
		mutation.restore()
	}
}

func ampProxyThreadDeleteSucceeded(req *http.Request, body []byte) bool {
	if req == nil || req.URL == nil {
		return false
	}
	if "/"+strings.Trim(req.URL.Path, "/") != "/api/internal" {
		return ampProxySvelteRemoteCommandSucceeded(body)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	if ok, exists := payload["ok"].(bool); exists {
		if ok {
			return true
		}
		errorPayload := mapValue(payload["error"])
		return strings.EqualFold(strings.TrimSpace(stringValue(errorPayload["code"])), "thread-not-found")
	}
	return false
}

func ampProxySvelteRemoteCommandSucceeded(body []byte) bool {
	var envelope map[string]any
	if err := json.Unmarshal(body, &envelope); err != nil || stringValue(envelope["type"]) != "result" {
		return false
	}
	var values []any
	if err := json.Unmarshal([]byte(stringValue(envelope["data"])), &values); err != nil {
		return false
	}
	decoded, ok := neoDecodeSvelteKitDevalueIndex(values, 0, map[int]bool{})
	if !ok {
		return false
	}
	result := mapValue(mapValue(decoded)["_"])
	if ok, exists := result["ok"].(bool); exists {
		if ok {
			return true
		}
		errorPayload := mapValue(result["error"])
		return strings.EqualFold(strings.TrimSpace(stringValue(errorPayload["code"])), "thread-not-found")
	}
	return false
}

func replaceAmpProxyResponseBody(resp *http.Response, body []byte) {
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Del("Content-Encoding")
	resp.Header.Del("Content-Length")
	resp.Header.Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
}

func normalizeAmpThreadListResponse(resp *http.Response, body []byte) []byte {
	if !ampThreadListResponse(resp) || len(body) == 0 {
		return nil
	}
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil
	}
	changed := ensureAmpThreadRelationships(payload)
	if augmenter, ok := resp.Request.Context().Value(ampProxyThreadListAugmenterContextKey{}).(ampProxyThreadListAugmenter); ok && augmenter.load != nil {
		var loaded []any
		if len(augmenter.threadIDs) > 0 && augmenter.selectedLoad != nil {
			loaded = augmenter.selectedLoad(augmenter.threadIDs)
		} else {
			loadLimit := augmenter.limit
			if augmenter.offset > 0 {
				loadLimit += augmenter.offset
			}
			loaded = augmenter.load(loadLimit)
			if augmenter.selectedLoad != nil {
				upstreamThreadIDs := ampThreadListResponseIDs(payload)
				for _, rawThread := range loaded {
					delete(upstreamThreadIDs, ampThreadListItemID(mapValue(rawThread)))
				}
				if len(upstreamThreadIDs) > 0 {
					loaded = append(loaded, augmenter.selectedLoad(upstreamThreadIDs)...)
				}
			}
		}
		excludedLocalThreadIDs := ampThreadListExcludedLocalThreadIDs(loaded, augmenter)
		if filtered, filteredChanged := removeAmpThreadListIDs(payload, excludedLocalThreadIDs); filteredChanged {
			payload = filtered
			changed = true
		}
		localThreads := filterAmpThreadListLocalThreads(loaded, augmenter)
		windowLimit := augmenter.limit + augmenter.offset
		if augmenter.limit > 0 && len(localThreads) > windowLimit {
			localThreads = localThreads[:windowLimit]
		}
		if merged, mergedChanged := mergeAmpThreadListValue(payload, localThreads, augmenter.offset, augmenter.limit); mergedChanged {
			payload = merged
			changed = true
		}
	}
	if !changed {
		return nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return encoded
}

func ampThreadListResponseIDs(value any) map[string]bool {
	threadIDs := map[string]bool{}
	var visit func(any)
	visit = func(current any) {
		switch typed := current.(type) {
		case map[string]any:
			if threadID := ampThreadListItemID(typed); neoThreadIDExactPattern.MatchString(threadID) {
				threadIDs[threadID] = true
				return
			}
			for _, key := range []string{"result", "threads", "items", "data"} {
				visit(typed[key])
			}
		case []any:
			for _, item := range typed {
				visit(item)
			}
		}
	}
	visit(value)
	return threadIDs
}

func ampThreadListExcludedLocalThreadIDs(threads []any, augmenter ampProxyThreadListAugmenter) map[string]bool {
	excluded := make(map[string]bool)
	for _, rawThread := range threads {
		thread := mapValue(rawThread)
		threadID := ampThreadListItemID(thread)
		if len(augmenter.threadIDs) > 0 && !augmenter.threadIDs[threadID] {
			continue
		}
		if threadID != "" && ((!augmenter.includeArchived && boolValue(thread["archived"])) || (!augmenter.includeEmpty && numberFrom(thread["messageCount"]) <= 0) || ampThreadListHasExcludedLabel(thread, augmenter.excludedLabelNames)) {
			excluded[threadID] = true
		}
	}
	return excluded
}

func removeAmpThreadListIDs(value any, excluded map[string]bool) (any, bool) {
	if len(excluded) == 0 {
		return value, false
	}
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range []string{"threads", "items"} {
			items, ok := typed[key].([]any)
			if !ok {
				continue
			}
			filtered := make([]any, 0, len(items))
			for _, item := range items {
				if excluded[ampThreadListItemID(mapValue(item))] {
					continue
				}
				filtered = append(filtered, item)
			}
			if len(filtered) == len(items) {
				return typed, false
			}
			typed[key] = filtered
			return typed, true
		}
		for _, key := range []string{"result", "data"} {
			child, exists := typed[key]
			if !exists {
				continue
			}
			filtered, changed := removeAmpThreadListIDs(child, excluded)
			if changed {
				typed[key] = filtered
				return typed, true
			}
		}
	case []any:
		filtered := make([]any, 0, len(typed))
		for _, item := range typed {
			if excluded[ampThreadListItemID(mapValue(item))] {
				continue
			}
			filtered = append(filtered, item)
		}
		if len(filtered) != len(typed) {
			return filtered, true
		}
	}
	return value, false
}

func filterAmpThreadListLocalThreads(threads []any, augmenter ampProxyThreadListAugmenter) []any {
	filtered := make([]any, 0, len(threads))
	for _, rawThread := range threads {
		thread := mapValue(rawThread)
		threadID := ampThreadListItemID(thread)
		if len(augmenter.threadIDs) > 0 && !augmenter.threadIDs[threadID] {
			continue
		}
		if !augmenter.includeArchived && boolValue(thread["archived"]) {
			continue
		}
		if !augmenter.includeEmpty && numberFrom(thread["messageCount"]) <= 0 {
			continue
		}
		if ampThreadListHasExcludedLabel(thread, augmenter.excludedLabelNames) {
			continue
		}
		filtered = append(filtered, rawThread)
	}
	return filtered
}

func ampThreadListHasExcludedLabel(thread map[string]any, excluded map[string]bool) bool {
	if len(excluded) == 0 {
		return false
	}
	for _, rawLabel := range arrayValue(thread["labels"]) {
		label := strings.TrimSpace(firstNonEmptyString(rawLabel, mapValue(rawLabel)["name"]))
		if excluded[label] {
			return true
		}
	}
	return false
}

func mergeAmpThreadListValue(value any, localThreads []any, offset, limit int) (any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range []string{"threads", "items"} {
			if items, ok := typed[key].([]any); ok {
				merged, changed := mergeAmpThreadListItems(items, localThreads, offset, limit)
				if changed {
					typed[key] = merged
				}
				return typed, changed
			}
		}
		for _, key := range []string{"result", "data"} {
			child, exists := typed[key]
			if !exists {
				continue
			}
			merged, changed := mergeAmpThreadListValue(child, localThreads, offset, limit)
			if changed {
				typed[key] = merged
				return typed, true
			}
		}
	case []any:
		return mergeAmpThreadListItems(typed, localThreads, offset, limit)
	}
	return value, false
}

func mergeAmpThreadListItems(items, localThreads []any, offset, limit int) ([]any, bool) {
	merged := cloneArray(items)
	byID := make(map[string]int, len(merged)+len(localThreads))
	var itemShape []string
	for index, rawItem := range merged {
		item := mapValue(rawItem)
		if threadID := ampThreadListItemID(item); threadID != "" {
			byID[threadID] = index
			if itemShape == nil {
				_, itemShape = ampThreadListItemThread(item)
			}
		}
	}
	changed := false
	for _, rawLocal := range localThreads {
		local, _ := ampThreadListItemThread(mapValue(rawLocal))
		local = cloneMap(local)
		pinnedOverride, hasPinnedOverride := local[neoLocalPinnedOverrideKey].(bool)
		delete(local, neoLocalPinnedOverrideKey)
		threadID := ampThreadListItemID(local)
		if !neoThreadIDExactPattern.MatchString(threadID) {
			continue
		}
		local["id"] = threadID
		local["threadId"] = threadID
		if index, exists := byID[threadID]; exists {
			existingItem := mapValue(merged[index])
			existing, shape := ampThreadListItemThread(existingItem)
			updated := cloneMap(existing)
			updatedChanged := false
			if ampThreadListActivityMillis(local) >= ampThreadListActivityMillis(existing) {
				for key, value := range local {
					switch key {
					case "relationships", "pinned", "archived":
						continue
					case "title":
						if ampThreadListPlaceholderTitle(value) {
							continue
						}
					case "creator", "creatorUserID", "ownerUserId":
						if !ampThreadListMissingValue(existing[key]) {
							continue
						}
					case "meta":
						value = ampThreadListMergeMissingMap(mapValue(existing[key]), mapValue(value))
					}
					updated[key] = cloneNeoJSONValue(value)
					updatedChanged = true
				}
			}
			if hasPinnedOverride {
				if existingPinned, exists := updated["pinned"].(bool); !exists || existingPinned != pinnedOverride {
					updated["pinned"] = pinnedOverride
					updatedChanged = true
				}
			}
			for _, key := range []string{"archived", "pinned"} {
				if key == "pinned" && hasPinnedOverride {
					continue
				}
				if _, exists := updated[key]; exists {
					continue
				}
				if value, ok := local[key].(bool); ok {
					updated[key] = value
					updatedChanged = true
				}
			}
			if updatedChanged {
				merged[index] = ampThreadListWrapThread(existingItem, updated, shape)
				changed = true
			}
			continue
		}
		if _, ok := local["relationships"].([]any); !ok {
			local["relationships"] = []any{}
		}
		byID[threadID] = len(merged)
		merged = append(merged, ampThreadListWrapThread(nil, local, itemShape))
		changed = true
	}
	if changed {
		sort.SliceStable(merged, func(i, j int) bool {
			left := ampThreadListActivityMillis(mapValue(merged[i]))
			right := ampThreadListActivityMillis(mapValue(merged[j]))
			if left != right {
				return left > right
			}
			return ampThreadListItemID(mapValue(merged[i])) < ampThreadListItemID(mapValue(merged[j]))
		})
	}
	start := min(max(0, offset), len(merged))
	end := len(merged)
	if limit > 0 {
		end = min(start+limit, end)
	}
	if start > 0 || end < len(merged) {
		return merged[start:end], true
	}
	if !changed {
		return items, false
	}
	return merged, true
}

func ampThreadListPlaceholderTitle(value any) bool {
	title := strings.TrimSpace(stringValue(value))
	return title == "" || strings.EqualFold(title, "Untitled")
}

func ampThreadListMissingValue(value any) bool {
	if value == nil {
		return true
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) == ""
	case map[string]any:
		return len(typed) == 0
	default:
		return false
	}
}

func ampThreadListMergeMissingMap(existing, local map[string]any) map[string]any {
	if len(existing) == 0 {
		return cloneMap(local)
	}
	merged := cloneMap(existing)
	for key, value := range local {
		if ampThreadListMissingValue(merged[key]) {
			merged[key] = cloneNeoJSONValue(value)
		}
	}
	return merged
}

func ampThreadListItemID(item map[string]any) string {
	item, _ = ampThreadListItemThread(item)
	return strings.TrimSpace(firstNonEmptyString(item["id"], item["threadId"], item["threadID"], item["thread_id"]))
}

func ampThreadListActivityMillis(item map[string]any) int {
	item, _ = ampThreadListItemThread(item)
	for _, key := range []string{"lastUserMessageAt", "updatedAt", "userLastInteractedAt", "updated", "createdAt", "created"} {
		value := item[key]
		if timestamp := neoTimeStringMillis(stringValue(value)); timestamp > 0 {
			return timestamp
		}
		if timestamp := int(numberFrom(value)); timestamp > 0 {
			return timestamp
		}
	}
	return 0
}

func ampThreadListItemThread(item map[string]any) (map[string]any, []string) {
	current := item
	shape := make([]string, 0, 2)
	for {
		key := ""
		if len(mapValue(current["thread"])) > 0 {
			key = "thread"
		} else if len(mapValue(current["data"])) > 0 {
			key = "data"
		}
		if key == "" {
			return current, shape
		}
		shape = append(shape, key)
		current = mapValue(current[key])
	}
}

func ampThreadListWrapThread(item, thread map[string]any, shape []string) map[string]any {
	if len(shape) == 0 {
		return thread
	}
	root := cloneMap(item)
	current := root
	for index, key := range shape {
		if index == len(shape)-1 {
			current[key] = thread
			break
		}
		next := cloneMap(mapValue(current[key]))
		current[key] = next
		current = next
	}
	return root
}

func ampThreadListResponse(resp *http.Response) bool {
	if resp == nil || resp.Request == nil || resp.Request.URL == nil {
		return false
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}
	path := "/" + strings.Trim(resp.Request.URL.Path, "/")
	if path != "/api/internal" {
		return false
	}
	if method, ok := resp.Request.Context().Value(ampProxyInternalMethodContextKey{}).(string); ok && strings.EqualFold(method, "listThreads") {
		return true
	}
	if strings.EqualFold(neoInternalQueryMethod(resp.Request.URL.RawQuery), "listThreads") {
		return true
	}
	return false
}

func tagAmpProxyInternalRPCMethod(req *http.Request) {
	method := ampProxyInternalRPCMethod(req)
	if method == "" {
		return
	}
	*req = *req.WithContext(context.WithValue(req.Context(), ampProxyInternalMethodContextKey{}, method))
}

func ampProxyInternalRPCMethod(req *http.Request) string {
	if req == nil || req.URL == nil || req.Method != http.MethodPost {
		return ""
	}
	if "/"+strings.Trim(req.URL.Path, "/") != "/api/internal" {
		return ""
	}
	if method := neoInternalQueryMethod(req.URL.RawQuery); method != "" {
		return method
	}
	body, err := readAndRestoreNeoJSONBody(req)
	if err == nil {
		if method := strings.TrimSpace(stringValue(body["method"])); method != "" {
			return method
		}
	}
	return ""
}

func ampThreadListRequestAugmenter(req *http.Request) (ampProxyThreadListAugmenter, bool) {
	if req == nil || req.URL == nil {
		return ampProxyThreadListAugmenter{}, false
	}
	augmenter := ampProxyThreadListAugmenter{}
	query := req.URL.Query()
	if strings.TrimSpace(query.Get("installationID")) != "" {
		return ampProxyThreadListAugmenter{}, false
	}
	if value, err := strconv.Atoi(strings.TrimSpace(query.Get("limit"))); err == nil && value > 0 {
		augmenter.limit = value
	}
	if offset, err := strconv.Atoi(strings.TrimSpace(query.Get("offset"))); err == nil && offset > 0 {
		augmenter.offset = offset
	}
	if includeArchived, err := strconv.ParseBool(strings.TrimSpace(query.Get("includeArchived"))); err == nil {
		augmenter.includeArchived = includeArchived
	}
	body, err := readAndRestoreNeoJSONBody(req)
	if err != nil {
		augmenter.includeEmpty, _ = strconv.ParseBool(strings.TrimSpace(query.Get("includeEmpty")))
		augmenter.threadIDs = ampThreadListStringSet(query["threadIDs"])
		augmenter.excludedLabelNames = ampThreadListStringSet(query["excludeLabelNames"])
		return augmenter, true
	}
	params := mapValue(body["params"])
	if strings.TrimSpace(stringValue(params["installationID"])) != "" {
		return ampProxyThreadListAugmenter{}, false
	}
	if augmenter.limit == 0 {
		augmenter.limit = max(0, int(numberFrom(params["limit"])))
	}
	if !query.Has("offset") {
		augmenter.offset = max(0, int(numberFrom(params["offset"])))
	}
	if augmenter.offset > 0 && augmenter.limit == 0 {
		return ampProxyThreadListAugmenter{}, false
	}
	if !query.Has("includeArchived") {
		augmenter.includeArchived = boolValue(params["includeArchived"])
	}
	augmenter.includeEmpty = boolValue(params["includeEmpty"])
	if query.Has("includeEmpty") {
		augmenter.includeEmpty, _ = strconv.ParseBool(strings.TrimSpace(query.Get("includeEmpty")))
	}
	augmenter.threadIDs = ampThreadListStringSet(params["threadIDs"])
	if query.Has("threadIDs") {
		augmenter.threadIDs = ampThreadListStringSet(query["threadIDs"])
	}
	augmenter.excludedLabelNames = ampThreadListStringSet(params["excludeLabelNames"])
	if query.Has("excludeLabelNames") {
		augmenter.excludedLabelNames = ampThreadListStringSet(query["excludeLabelNames"])
	}
	return augmenter, true
}

func rewriteAmpThreadListRequestWindow(req *http.Request, augmenter ampProxyThreadListAugmenter) bool {
	if augmenter.offset <= 0 && augmenter.upstreamOverfetch <= 0 {
		return true
	}
	if req == nil || req.URL == nil || augmenter.limit <= 0 || strings.TrimSpace(req.Header.Get("Content-Encoding")) != "" {
		return false
	}
	upstreamLimit := augmenter.offset + augmenter.limit + augmenter.upstreamOverfetch
	if upstreamLimit < augmenter.limit || upstreamLimit < augmenter.upstreamOverfetch {
		return false
	}
	body, err := readAndRestoreNeoJSONBody(req)
	if err != nil {
		return false
	}
	query := req.URL.Query()
	if query.Has("offset") {
		query.Set("offset", "0")
	}
	if query.Has("limit") {
		query.Set("limit", strconv.Itoa(upstreamLimit))
	}
	params := mapValue(body["params"])
	if len(params) == 0 {
		if !query.Has("limit") {
			return false
		}
		query.Set("offset", "0")
		req.URL.RawQuery = query.Encode()
		return true
	}
	params["offset"] = 0
	params["limit"] = upstreamLimit
	body["params"] = params
	encoded, err := json.Marshal(body)
	if err != nil {
		return false
	}
	req.URL.RawQuery = query.Encode()
	req.Body = io.NopCloser(bytes.NewReader(encoded))
	req.ContentLength = int64(len(encoded))
	req.Header.Set("Content-Length", strconv.FormatInt(req.ContentLength, 10))
	return true
}

func ampThreadListStringSet(raw any) map[string]bool {
	values := stringArrayValue(raw)
	if value := strings.TrimSpace(stringValue(raw)); value != "" {
		values = []any{value}
	}
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]bool, len(values))
	for _, rawValue := range values {
		if value := strings.TrimSpace(stringValue(rawValue)); value != "" {
			set[value] = true
		}
	}
	return set
}

func ensureAmpThreadRelationships(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		changed := false
		for _, key := range []string{"threads", "items", "data"} {
			if ensureAmpThreadListRelationships(typed[key]) {
				changed = true
			}
		}
		if result, ok := typed["result"]; ok {
			if ensureAmpThreadRelationships(result) {
				changed = true
			}
		}
		return changed
	default:
		return ensureAmpThreadListRelationships(value)
	}
}

func ensureAmpThreadListRelationships(value any) bool {
	items, ok := value.([]any)
	if !ok {
		return false
	}
	changed := false
	for _, item := range items {
		thread, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if _, ok := thread["relationships"].([]any); !ok {
			thread["relationships"] = []any{}
			changed = true
		}
	}
	return changed
}

func actorEngineAuthorization(req *http.Request) string {
	if req == nil || req.URL == nil {
		return ""
	}
	if !actorEnginePath(req.URL.Path) {
		return ""
	}
	authorization := strings.TrimSpace(req.Header.Get("Authorization"))
	scheme, _, ok := strings.Cut(authorization, " ")
	if !ok {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "basic", "bearer":
	default:
		return ""
	}
	return authorization
}

// actorEngineMetadataRequest matches RivetKit's pre-connection metadata discovery.
// The client fetches this WITHOUT a token before it has an actor or connection, so it
// cannot satisfy the authenticated engine-routing checks below. It must still reach the
// local engine; otherwise the client's retry-forever metadata lookup never resolves and
// the thread transport loops on connect_failed. Newer binaries probe /actors/metadata
// in manager mode and /metadata once they adopt the discovered engine endpoint; both
// only expose a non-sensitive engine descriptor, so routing them unauthenticated is safe.
func actorEngineMetadataRequest(req *http.Request) bool {
	if req == nil || req.Method != http.MethodGet || req.URL == nil {
		return false
	}
	switch "/" + strings.Trim(req.URL.Path, "/") {
	case "/metadata", "/actors/metadata":
		return true
	default:
		return false
	}
}

func actorEngineRequest(req *http.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}
	if actorEngineMetadataRequest(req) {
		return true
	}
	if !actorEnginePath(req.URL.Path) {
		return false
	}
	if actorEngineAuthorization(req) != "" {
		return true
	}
	if strings.TrimSpace(req.URL.Query().Get("rvt-token")) != "" {
		return true
	}
	for _, protocol := range req.Header.Values("Sec-WebSocket-Protocol") {
		if strings.Contains(strings.ToLower(protocol), "rivet_token.") {
			return true
		}
	}
	return false
}

func actorEngineRivetCredentialRequest(req *http.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}
	if strings.TrimSpace(req.URL.Query().Get("rvt-token")) != "" {
		return true
	}
	for _, protocol := range req.Header.Values("Sec-WebSocket-Protocol") {
		if strings.Contains(strings.ToLower(protocol), "rivet_token.") {
			return true
		}
	}
	return false
}

func actorEnginePath(path string) bool {
	normalized := "/" + strings.Trim(path, "/")
	// /gateway is the rivetkit engine transport the client uses once it adopts the
	// discovered engine endpoint (engine mode); /actors is the manager-mode prefix.
	return normalized == "/actors" || strings.HasPrefix(normalized, "/actors/") ||
		normalized == "/gateway" || strings.HasPrefix(normalized, "/gateway/")
}

// isStreamingResponse detects if the response is streaming (SSE only)
// Note: We only treat text/event-stream as streaming. Chunked transfer encoding
// is a transport-level detail and doesn't mean we can't decompress the full response.
// Many JSON APIs use chunked encoding for normal responses.
func isStreamingResponse(resp *http.Response) bool {
	contentType := resp.Header.Get("Content-Type")

	// Only Server-Sent Events are true streaming responses
	if strings.Contains(contentType, "text/event-stream") {
		return true
	}

	return false
}

// proxyHandler converts httputil.ReverseProxy to gin.HandlerFunc
func proxyHandler(proxy *httputil.ReverseProxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		proxy.ServeHTTP(c.Writer, c.Request)
	}
}

// filterBetaFeatures removes a specific beta feature from comma-separated list
func filterBetaFeatures(header, featureToRemove string) string {
	features := strings.Split(header, ",")
	filtered := make([]string, 0, len(features))

	for _, feature := range features {
		trimmed := strings.TrimSpace(feature)
		if trimmed != "" && trimmed != featureToRemove {
			filtered = append(filtered, trimmed)
		}
	}

	return strings.Join(filtered, ",")
}
