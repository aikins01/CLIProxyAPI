package logging

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func TestGinLogrusRecoveryRepanicsErrAbortHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(GinLogrusRecovery())
	engine.GET("/abort", func(c *gin.Context) {
		panic(http.ErrAbortHandler)
	})

	req := httptest.NewRequest(http.MethodGet, "/abort", nil)
	recorder := httptest.NewRecorder()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("expected panic, got nil")
		}
		err, ok := recovered.(error)
		if !ok {
			t.Fatalf("expected error panic, got %T", recovered)
		}
		if !errors.Is(err, http.ErrAbortHandler) {
			t.Fatalf("expected ErrAbortHandler, got %v", err)
		}
		if err != http.ErrAbortHandler {
			t.Fatalf("expected exact ErrAbortHandler sentinel, got %v", err)
		}
	}()

	engine.ServeHTTP(recorder, req)
}

func TestGinLogrusRecoveryHandlesRegularPanic(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(GinLogrusRecovery())
	engine.GET("/panic", func(c *gin.Context) {
		panic("boom")
	})

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	recorder := httptest.NewRecorder()

	engine.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", recorder.Code)
	}
}

func TestIsAIAPIPathIncludesImages(t *testing.T) {
	if !isAIAPIPath("/v1/images/generations") {
		t.Fatalf("expected /v1/images/generations to be treated as AI API path")
	}
	if !isAIAPIPath("/v1/images/edits") {
		t.Fatalf("expected /v1/images/edits to be treated as AI API path")
	}
}

func TestLogFormatterIncludesThreadSearchShadowMetrics(t *testing.T) {
	metrics := log.Fields{
		"files_considered":            uint64(1),
		"valid_indexes":               uint64(2),
		"unknown_indexes":             uint64(3),
		"corrupt_indexes":             uint64(4),
		"stale_indexes":               uint64(5),
		"predicted_negatives_checked": uint64(6),
		"mismatches":                  uint64(7),
		"rebuild_successes":           uint64(8),
		"rebuild_failures":            uint64(9),
		"sidecar_bytes_read":          uint64(10),
		"sidecar_bytes_written":       uint64(11),
		"scanner_bytes_read":          uint64(12),
		"potentially_pruned_bytes":    uint64(13),
		"orphan_cleanups":             uint64(14),
		"orphan_cleanup_failures":     uint64(15),
		"ranked_searches":             uint64(16),
		"ranked_candidates":           uint64(17),
		"ranked_decodes":              uint64(18),
		"rank_fallbacks":              uint64(19),
		"unapproved_field":            "not logged",
	}
	entry := &log.Entry{
		Logger:  log.New(),
		Data:    metrics,
		Time:    time.Unix(0, 0),
		Level:   log.DebugLevel,
		Message: "amp neo local thread search index shadow metrics",
	}

	formatted, err := (&LogFormatter{}).Format(entry)
	if err != nil {
		t.Fatalf("format log entry: %v", err)
	}
	line := string(formatted)
	for key, value := range metrics {
		if key == "unapproved_field" {
			continue
		}
		want := key + "=" + fmt.Sprint(value)
		if !strings.Contains(line, want) {
			t.Fatalf("formatted log = %q, missing %q", line, want)
		}
	}
	if strings.Contains(line, "unapproved_field") || strings.Contains(line, "not logged") {
		t.Fatalf("formatted log included unapproved field: %q", line)
	}
}
