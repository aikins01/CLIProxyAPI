package amp

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestNeoScheduleToolsLifecyclePersists(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
	rt := newNeoRuntime(cfg)
	threadID := "T-019f7000-0000-7000-8000-000000000091"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	actor.mu.Lock()
	actor.messages = []neoMessage{{
		ThreadID:  threadID,
		MessageID: "M-0000000000000000000091",
		Role:      "user",
		Content:   []any{map[string]any{"type": "text", "text": "Schedule this."}},
		UserState: map[string]any{"puckContext": map[string]any{"currentURL": "https://ampcode.com/feed", "timeZone": "America/New_York"}},
	}}
	actor.rebuildHistoryLocked()
	actor.mu.Unlock()

	setResult, err := actor.executeLocalScheduleTool("set_schedule", map[string]any{
		"title":         "Morning check",
		"prompt":        "Check the overnight agent results and report blockers.",
		"schedule":      "FREQ=DAILY;BYHOUR=9;BYMINUTE=30",
		"scheduleLabel": "Every morning",
	})
	if err != nil {
		t.Fatal(err)
	}
	schedule := mapValue(setResult["schedule"])
	if stringValue(mapValue(schedule["trigger"])["schedule"]) != "RRULE:FREQ=DAILY;BYHOUR=9;BYMINUTE=30" {
		t.Fatalf("set schedule = %#v", schedule)
	}
	if stringValue(schedule["nextRunAt"]) == "" || !boolValue(schedule["enabled"]) {
		t.Fatalf("set schedule timing = %#v", schedule)
	}
	if _, err := os.Stat(rt.schedulePath); err != nil {
		t.Fatalf("schedule store: %v", err)
	}

	reloaded := newNeoRuntime(cfg)
	reloadedActor := reloaded.store.ensureThreadActor(threadID)
	reloadedActor.updateSettings(map[string]any{"agentMode": "puck"})
	getResult, err := reloadedActor.executeLocalScheduleTool("get_schedule", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if got := stringValue(mapValue(getResult["schedule"])["title"]); got != "Morning check" {
		t.Fatalf("reloaded title = %q", got)
	}

	updateResult, err := reloadedActor.executeLocalScheduleTool("update_schedule", map[string]any{"enabled": false})
	if err != nil {
		t.Fatal(err)
	}
	paused := mapValue(updateResult["schedule"])
	if boolValue(paused["enabled"]) || stringValue(paused["disabledReason"]) != "paused" || paused["nextRunAt"] != nil {
		t.Fatalf("paused schedule = %#v", paused)
	}

	clearResult, err := reloadedActor.executeLocalScheduleTool("clear_schedule", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if !boolValue(clearResult["cleared"]) {
		t.Fatalf("clear result = %#v", clearResult)
	}
	if stringValue(clearResult["threadId"]) != threadID {
		t.Fatalf("clear thread IDs = %#v", clearResult)
	}
	getResult, err = reloadedActor.executeLocalScheduleTool("get_schedule", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if getResult["schedule"] != nil {
		t.Fatalf("schedule remained after clear: %#v", getResult)
	}
}

func TestNeoScheduleToolReportsPublishedDurabilityPending(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000119"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	rt.syncScheduleStoreDir = func(string) error { return errors.New("injected directory sync failure") }
	result, err := actor.executeLocalScheduleTool("set_schedule", map[string]any{
		"title":    "Durability check",
		"prompt":   "Verify the deferred directory sync.",
		"schedule": "FREQ=DAILY",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !boolValue(result["durabilityPending"]) || stringValue(result["durabilityMessage"]) == "" {
		t.Fatalf("set result = %#v", result)
	}
	if _, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID); !ok {
		t.Fatal("published schedule was rolled back")
	}
	rt.syncScheduleStoreDir = nil
	rt.scheduleMu.Lock()
	rt.schedulePersistRetryAt = time.Time{}
	rt.scheduleMu.Unlock()
	if !rt.retryLocalScheduleDirectorySync(time.Now().UTC()) {
		t.Fatal("directory sync retry did not complete")
	}
	result, err = actor.executeLocalScheduleTool("get_schedule", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if result["durabilityPending"] != nil || result["durabilityMessage"] != nil {
		t.Fatalf("recovered result = %#v", result)
	}
}

func TestNeoScheduleCreationIsBlockedThroughoutThreadDeletion(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000120"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	if !actor.syncLocalThreadSnapshotForShutdownNow() {
		t.Fatal("initial thread snapshot failed")
	}
	guard := rt.localScheduleGuard(threadID)
	guard.Lock()
	purgeDone := make(chan error, 1)
	go func() {
		purgeDone <- rt.purgeNeoLocalThread(threadID)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !rt.neoCloudThreadSyncBlocked(threadID) {
		if time.Now().After(deadline) {
			guard.Unlock()
			t.Fatal("thread deletion did not enter admission state")
		}
		time.Sleep(time.Millisecond)
	}
	setDone := make(chan error, 1)
	go func() {
		_, err := rt.setLocalSchedule(threadID, neoLocalOwnerUserID, map[string]any{
			"title":    "Must not survive deletion",
			"prompt":   "Do not create this schedule.",
			"schedule": "FREQ=DAILY",
		}, "UTC")
		setDone <- err
	}()
	guard.Unlock()
	if err := <-purgeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-setDone; err == nil || !strings.Contains(err.Error(), "being deleted") {
		t.Fatalf("set during deletion error = %v", err)
	}
	if _, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID); ok {
		t.Fatal("schedule survived thread deletion")
	}
}

func TestNeoScheduleOwnerMigratesWithLegacyThreadClaim(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
	rt := newNeoRuntime(cfg)
	threadID := "T-019f7000-0000-7000-8000-000000000121"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	schedule := testNeoLocalSchedule(threadID)
	rt.schedules[threadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		t.Fatal(err)
	}
	ownerUserID := "user-aikins01"
	rt.legacyOwnerMigrationMu.Lock()
	rt.legacyOwnerMigrationUser = ownerUserID
	rt.legacyOwnerMigrationMu.Unlock()
	if !rt.claimNeoLegacyThreadActorOwner(actor, ownerUserID) {
		t.Fatal("legacy thread owner claim failed")
	}
	stored, ok := rt.getLocalSchedule(threadID, ownerUserID)
	if !ok || stored.OwnerUserID != ownerUserID {
		t.Fatalf("migrated schedule = %#v", stored)
	}
	reloaded := newNeoRuntime(cfg)
	stored, ok = reloaded.getLocalSchedule(threadID, ownerUserID)
	if !ok || stored.OwnerUserID != ownerUserID {
		t.Fatalf("reloaded migrated schedule = %#v", stored)
	}
}

func TestNeoScheduleOwnerMigrationRejectsForeignThreadOwner(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000122"
	actor := rt.store.ensureThreadActor(threadID)
	actor.mu.Lock()
	actor.meta["creatorUserID"] = "user-b"
	actor.meta["ownerUserId"] = "user-b"
	actor.mu.Unlock()
	schedule := testNeoLocalSchedule(threadID)
	rt.schedules[threadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		t.Fatal(err)
	}
	rt.legacyOwnerMigrationMu.Lock()
	rt.legacyOwnerMigrationUser = "user-a"
	rt.legacyOwnerMigrationMu.Unlock()
	if rt.claimNeoLegacyThreadActorOwner(actor, "user-a") {
		t.Fatal("foreign-owned thread was claimed")
	}
	stored, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || stored.OwnerUserID != neoLocalOwnerUserID {
		t.Fatalf("foreign-owner rejection changed schedule = %#v", stored)
	}
}

func TestNeoScheduleOwnerMigrationRollsBackOnThreadSyncFailure(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000123"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	schedule := testNeoLocalSchedule(threadID)
	rt.schedules[threadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		t.Fatal(err)
	}
	ownerUserID := "user-aikins01"
	rt.legacyOwnerMigrationMu.Lock()
	rt.legacyOwnerMigrationUser = ownerUserID
	rt.legacyOwnerMigrationMu.Unlock()
	syncCalls := 0
	rt.syncScheduledThreadPath = func(path string, syncFile bool) error {
		syncCalls++
		if syncCalls == 1 {
			return errors.New("injected owner sync failure")
		}
		return syncNeoDurablePath(path, syncFile)
	}
	if rt.claimNeoLegacyThreadActorOwner(actor, ownerUserID) {
		t.Fatal("owner claim succeeded despite thread sync failure")
	}
	if actor.threadToolOwnerID() != neoLocalOwnerUserID {
		t.Fatalf("actor owner = %q", actor.threadToolOwnerID())
	}
	stored, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || stored.OwnerUserID != neoLocalOwnerUserID {
		t.Fatalf("rolled-back schedule = %#v", stored)
	}
}

func TestNeoScheduleThreadDeletionRollbackPreservesActorWork(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000124"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	actor.mu.Lock()
	actor.currentInference = &neoInferenceInflight{messageID: "M-active", agentMode: "puck"}
	actor.pendingTools["TU-active"] = neoPendingTool{ID: "TU-active", Name: "shell_command", MessageID: "M-active"}
	actor.queue = []neoQueuedMessage{{MessageID: "M-queued", Content: []any{map[string]any{"type": "text", "text": "Keep this queued work."}}}}
	actor.mu.Unlock()
	actor.closeLocalSnapshotSyncs()
	threadPath := filepath.Join(rt.threadDir, threadID+".json")
	if err := os.Remove(threadPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := os.MkdirAll(threadPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(threadPath, "keep"), []byte("force removal failure"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := rt.purgeNeoLocalThread(threadID); err == nil {
		t.Fatal("thread deletion succeeded despite non-empty thread path")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.currentInference == nil || actor.currentInference.messageID != "M-active" {
		t.Fatalf("current inference after rollback = %#v", actor.currentInference)
	}
	if _, ok := actor.pendingTools["TU-active"]; !ok {
		t.Fatalf("pending tools after rollback = %#v", actor.pendingTools)
	}
	if len(actor.queue) != 1 || actor.queue[0].MessageID != "M-queued" {
		t.Fatalf("queue after rollback = %#v", actor.queue)
	}
}

func TestNeoScheduleUsesBrowserTimeZoneAndRejectsSecondly(t *testing.T) {
	anchor := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rule, zone, next, err := neoParseLocalSchedule("FREQ=DAILY;BYHOUR=9;BYMINUTE=0", "America/New_York", anchor, anchor)
	if err != nil {
		t.Fatal(err)
	}
	if rule != "RRULE:FREQ=DAILY;BYHOUR=9;BYMINUTE=0" || zone != "America/New_York" {
		t.Fatalf("normalized rule=%q zone=%q", rule, zone)
	}
	want := time.Date(2026, time.July, 22, 13, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next run = %s, want %s", next, want)
	}
	if _, _, _, err := neoParseLocalSchedule("RRULE:FREQ=SECONDLY", "UTC", anchor, anchor); err == nil || !strings.Contains(err.Error(), "SECONDLY") {
		t.Fatalf("SECONDLY error = %v", err)
	}
	if _, _, _, err := neoParseLocalSchedule("DTSTART\nRRULE:FREQ=DAILY", "UTC", anchor, anchor); err == nil || !strings.Contains(err.Error(), "after ':'") {
		t.Fatalf("malformed DTSTART error = %v", err)
	}
	lowercaseRule, _, _, err := neoParseLocalSchedule("freq=daily;byhour=9", "UTC", anchor, anchor)
	if err != nil || lowercaseRule != "RRULE:FREQ=DAILY;BYHOUR=9" {
		t.Fatalf("lowercase rule = %q err=%v", lowercaseRule, err)
	}
}

func TestNeoScheduleUpdatePreservesTimeZone(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000098"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	actor.mu.Lock()
	actor.messages = []neoMessage{{
		ThreadID:  threadID,
		MessageID: "M-0000000000000000000098",
		Role:      "user",
		Content:   []any{map[string]any{"type": "text", "text": "Schedule this."}},
		UserState: map[string]any{"puckContext": map[string]any{"timeZone": "America/New_York"}},
	}}
	actor.rebuildHistoryLocked()
	actor.mu.Unlock()

	setResult, err := actor.executeLocalScheduleTool("set_schedule", map[string]any{
		"title":    "Daily check",
		"prompt":   "Run the daily check.",
		"schedule": "FREQ=DAILY;BYHOUR=9",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := stringValue(mapValue(setResult["schedule"])["timeZone"]); got != "America/New_York" {
		t.Fatalf("set time zone = %q", got)
	}
	updated, err := actor.executeLocalScheduleTool("update_schedule", map[string]any{
		"schedule": "FREQ=WEEKLY;BYDAY=MO;BYHOUR=10",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := stringValue(mapValue(updated["schedule"])["timeZone"]); got != "America/New_York" {
		t.Fatalf("updated time zone = %q", got)
	}
	updated, err = actor.executeLocalScheduleTool("update_schedule", map[string]any{"timeZone": "America/Chicago"})
	if err != nil {
		t.Fatal(err)
	}
	if got := stringValue(mapValue(updated["schedule"])["timeZone"]); got != "America/Chicago" {
		t.Fatalf("time-zone-only update = %q", got)
	}
	updated, err = actor.executeLocalScheduleTool("update_schedule", map[string]any{"timeZone": "Asia/Tokyo", "enabled": false})
	if err != nil {
		t.Fatal(err)
	}
	paused := mapValue(updated["schedule"])
	if got := stringValue(paused["timeZone"]); got != "Asia/Tokyo" || boolValue(paused["enabled"]) {
		t.Fatalf("time zone plus enabled update = %#v", paused)
	}
	_, zone, _, err := neoParseLocalSchedule("DTSTART;TZID=Europe/London:20260723T090000\nRRULE:FREQ=DAILY", "America/New_York", time.Now().UTC(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if zone != "Europe/London" {
		t.Fatalf("DTSTART time zone = %q", zone)
	}
}

func TestNeoScheduleCharacterLimitsUseRunes(t *testing.T) {
	if _, err := neoRequiredScheduleText(map[string]any{"title": strings.Repeat("界", neoLocalScheduleTitleMax)}, "title", neoLocalScheduleTitleMax); err != nil {
		t.Fatalf("256-rune title rejected: %v", err)
	}
	if _, err := neoRequiredScheduleText(map[string]any{"title": strings.Repeat("界", neoLocalScheduleTitleMax+1)}, "title", neoLocalScheduleTitleMax); err == nil {
		t.Fatal("257-rune title accepted")
	}
}

func TestNeoValidateLocalScheduleAllowsPersistedInvalidDisabledRule(t *testing.T) {
	schedule := testNeoLocalSchedule("T-019f7000-0000-7000-8000-000000000121")
	schedule.Enabled = false
	schedule.DisabledReason = "invalid_schedule"
	schedule.NextRunAt = ""
	schedule.Trigger.Schedule = "RRULE:FREQ=INVALID"
	if err := neoValidateLocalSchedule(schedule); err != nil {
		t.Fatalf("disabled invalid schedule rejected: %v", err)
	}
	schedule.Enabled = true
	schedule.NextRunAt = time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	if err := neoValidateLocalSchedule(schedule); err == nil {
		t.Fatal("enabled invalid schedule accepted")
	}
}

func TestNeoScheduleDispatchQueuesSavedPrompt(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000092"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	schedule := neoLocalSchedule{
		ID:             "AT-test",
		TargetThreadID: threadID,
		Prompt:         "Review the scheduled status now.",
		OwnerUserID:    neoLocalOwnerUserID,
		LastRunAt:      "2026-07-22T12:00:00Z",
		Pending: &neoLocalSchedulePending{
			ID:          "SO-test",
			MessageID:   "M-0000000000000000000092",
			ScheduledAt: "2026-07-22T12:00:00Z",
			CreatedAt:   "2026-07-22T12:00:01Z",
			Prompt:      "Review the scheduled status now.",
		},
	}
	if _, err := rt.dispatchLocalSchedule(schedule); err != nil {
		t.Fatal(err)
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 1 {
		t.Fatalf("queued messages = %#v", actor.queue)
	}
	queued := actor.queue[0]
	if got := stringValue(mapValue(queued.Content[0])["text"]); got != schedule.Prompt {
		t.Fatalf("queued prompt = %q", got)
	}
	if stringValue(queued.Meta["automationId"]) != schedule.ID || stringValue(queued.Meta["automationProvider"]) != "schedule" {
		t.Fatalf("queued metadata = %#v", queued.Meta)
	}
	if queued.MessageID != schedule.Pending.MessageID {
		t.Fatalf("queued message ID = %q", queued.MessageID)
	}
}

func TestNeoScheduleCommittedQueuePrunesAndRehydrates(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000122"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "deep", "reasoning.effort": "xhigh"})
	actor.mu.Lock()
	actor.bootstrapExecutorType = "test-client"
	actor.meta["executorType"] = "test-client"
	actor.mu.Unlock()
	schedule := testNeoLocalSchedule(threadID)
	schedule.Pending = &neoLocalSchedulePending{
		ID:          "SO-prunable",
		MessageID:   "M-0000000000000000000122",
		ScheduledAt: "2026-07-22T12:00:00Z",
		CreatedAt:   "2026-07-22T12:00:01Z",
		Prompt:      "Preserve this scheduled prompt across eviction.",
	}
	rt.scheduleMu.Lock()
	rt.schedules[threadID] = schedule
	err := rt.persistLocalSchedulesLocked()
	rt.scheduleMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	dispatched, err := rt.dispatchCurrentLocalSchedule(schedule)
	if err != nil || !dispatched {
		t.Fatalf("dispatch result dispatched=%v err=%v", dispatched, err)
	}
	storedSchedule, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || storedSchedule.Pending != nil {
		t.Fatalf("committed schedule = %#v", storedSchedule)
	}
	waitForNeoActorSyncIdle(t, actor)
	now := time.Now()
	actor.mu.Lock()
	actor.lastUsed = now.Add(-time.Minute)
	actor.mu.Unlock()
	if pruned := rt.store.pruneIdle(now, neoActorIdleTTL); pruned != 1 {
		t.Fatalf("pruned = %d, want 1", pruned)
	}
	if rt.store.lookupThreadActor(threadID) != nil {
		t.Fatal("committed scheduled queue retained its actor")
	}
	storedThread, ok := loadNeoThreadFromDir(threadID, rt.threadDir)
	if !ok {
		t.Fatal("scheduled thread snapshot missing after pruning")
	}
	queuedMessages := arrayValue(storedThread["queuedMessages"])
	if len(queuedMessages) != 1 {
		t.Fatalf("persisted queued messages = %#v", queuedMessages)
	}
	persisted := mapValue(mapValue(queuedMessages[0])["queuedMessage"])
	persistedMeta := mapValue(persisted["meta"])
	if stringValue(persisted["messageId"]) != schedule.Pending.MessageID || textFromBlocks(arrayValue(persisted["content"])) != schedule.Pending.Prompt || stringValue(persisted["agentMode"]) != "deep" || stringValue(persisted["reasoningEffort"]) != "xhigh" || stringValue(persistedMeta["automationId"]) != schedule.ID || stringValue(persistedMeta["automationOccurrenceId"]) != schedule.Pending.ID || stringValue(persistedMeta["automationProvider"]) != "schedule" {
		t.Fatalf("persisted scheduled queue = %#v", persisted)
	}
	rehydrated := rt.store.ensureThreadActor(threadID)
	rehydrated.mu.Lock()
	if len(rehydrated.queue) != 1 {
		rehydrated.mu.Unlock()
		t.Fatalf("rehydrated queue = %#v", rehydrated.queue)
	}
	queued := rehydrated.queue[0]
	rehydrated.mu.Unlock()
	if queued.MessageID != schedule.Pending.MessageID || queued.AgentMode != "deep" || queued.ReasoningEffort != "xhigh" || stringValue(queued.Meta["automationOccurrenceId"]) != schedule.Pending.ID {
		t.Fatalf("rehydrated scheduled message = %#v", queued)
	}
	rehydrated.mu.Lock()
	rehydrated.lastUsed = now.Add(-time.Minute)
	rehydrated.mu.Unlock()
	if pruned := rt.store.pruneIdle(now, neoActorIdleTTL); pruned != 1 {
		t.Fatalf("second prune = %d, want 1", pruned)
	}
	type updateResult struct {
		schedule neoLocalSchedule
		err      error
	}
	updated := make(chan updateResult, 1)
	go func() {
		paused, updateErr := rt.updateLocalSchedule(threadID, neoLocalOwnerUserID, map[string]any{"enabled": false}, "UTC", false)
		updated <- updateResult{schedule: paused, err: updateErr}
	}()
	var paused neoLocalSchedule
	select {
	case result := <-updated:
		if result.err != nil {
			t.Fatalf("pause after eviction: %v", result.err)
		}
		paused = result.schedule
	case <-time.After(2 * time.Second):
		t.Fatal("pause after eviction deadlocked")
	}
	if paused.Enabled || paused.DisabledReason != "paused" {
		t.Fatalf("paused schedule = %#v", paused)
	}
	cancelled := rt.store.lookupThreadActor(threadID)
	if cancelled == nil {
		t.Fatal("pause did not rehydrate the scheduled thread")
	}
	cancelled.mu.Lock()
	remaining := len(cancelled.queue)
	cancelled.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("queue after pause = %d", remaining)
	}
	waitForNeoActorSyncIdle(t, cancelled)
	storedThread, ok = loadNeoThreadFromDir(threadID, rt.threadDir)
	if !ok || len(arrayValue(storedThread["queuedMessages"])) != 0 {
		t.Fatalf("persisted queue after pause = %#v", storedThread["queuedMessages"])
	}
}

func TestNeoScheduleCancellationRetainsPrehydratedActor(t *testing.T) {
	tests := []struct {
		name     string
		threadID string
		run      func(*neoRuntime, string) error
		assert   func(*testing.T, *neoRuntime, string)
	}{
		{
			name:     "pause",
			threadID: "T-019f7000-0000-7000-8000-000000000123",
			run: func(rt *neoRuntime, threadID string) error {
				_, err := rt.updateLocalSchedule(threadID, neoLocalOwnerUserID, map[string]any{"enabled": false}, "UTC", false)
				return err
			},
			assert: func(t *testing.T, rt *neoRuntime, threadID string) {
				t.Helper()
				schedule, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
				if !ok || schedule.Enabled || schedule.DisabledReason != "paused" {
					t.Fatalf("paused schedule = %#v, exists=%v", schedule, ok)
				}
			},
		},
		{
			name:     "clear",
			threadID: "T-019f7000-0000-7000-8000-000000000124",
			run: func(rt *neoRuntime, threadID string) error {
				_, cleared, err := rt.takeLocalSchedule(threadID, neoLocalOwnerUserID)
				if err == nil && !cleared {
					return errors.New("schedule was not cleared")
				}
				return err
			},
			assert: func(t *testing.T, rt *neoRuntime, threadID string) {
				t.Helper()
				if schedule, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID); ok {
					t.Fatalf("cleared schedule = %#v", schedule)
				}
			},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useTempNeoThreadStore(t)
			enabled := true
			rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
			actor := rt.store.ensureThreadActor(test.threadID)
			actor.updateSettings(map[string]any{"agentMode": "deep", "reasoning.effort": "xhigh"})
			actor.mu.Lock()
			actor.bootstrapExecutorType = "test-client"
			actor.meta["executorType"] = "test-client"
			actor.mu.Unlock()
			schedule := testNeoLocalSchedule(test.threadID)
			schedule.Pending = &neoLocalSchedulePending{
				ID:          fmt.Sprintf("SO-retained-%d", index),
				MessageID:   fmt.Sprintf("M-000000000000000000012%d", index+3),
				ScheduledAt: "2026-07-22T12:00:00Z",
				CreatedAt:   "2026-07-22T12:00:01Z",
				Prompt:      "Cancel this retained scheduled prompt.",
			}
			rt.scheduleMu.Lock()
			rt.schedules[test.threadID] = schedule
			err := rt.persistLocalSchedulesLocked()
			rt.scheduleMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if dispatched, dispatchErr := rt.dispatchCurrentLocalSchedule(schedule); dispatchErr != nil || !dispatched {
				t.Fatalf("dispatch result dispatched=%v err=%v", dispatched, dispatchErr)
			}
			waitForNeoActorSyncIdle(t, actor)
			now := time.Now()
			actor.mu.Lock()
			actor.lastUsed = now.Add(-time.Minute)
			actor.mu.Unlock()
			if pruned := rt.store.pruneIdle(now, neoActorIdleTTL); pruned != 1 {
				t.Fatalf("initial prune = %d, want 1", pruned)
			}

			guard := rt.localScheduleGuard(test.threadID)
			guard.Lock()
			guardLocked := true
			defer func() {
				if guardLocked {
					guard.Unlock()
				}
			}()
			finished := make(chan error, 1)
			go func() {
				finished <- test.run(rt, test.threadID)
			}()

			var retained *neoActor
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				retained = rt.store.lookupThreadActor(test.threadID)
				if retained != nil {
					retained.mu.Lock()
					ready := retained.pruneLeases == 1 && len(retained.queue) == 1
					retained.mu.Unlock()
					if ready {
						break
					}
				}
				time.Sleep(time.Millisecond)
			}
			if retained == nil {
				t.Fatal("schedule control did not prehydrate the actor")
			}
			retained.mu.Lock()
			ready := retained.pruneLeases == 1 && len(retained.queue) == 1
			retained.lastUsed = now.Add(-time.Minute)
			retained.mu.Unlock()
			if !ready {
				t.Fatal("schedule control did not retain the hydrated queue")
			}
			if pruned := rt.store.pruneIdle(now, neoActorIdleTTL); pruned != 0 {
				t.Fatalf("retained actor prune = %d, want 0", pruned)
			}
			if current := rt.store.lookupThreadActor(test.threadID); current != retained {
				t.Fatal("retained actor was detached during schedule control")
			}

			guard.Unlock()
			guardLocked = false
			select {
			case controlErr := <-finished:
				if controlErr != nil {
					t.Fatal(controlErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("schedule control deadlocked after pruning attempt")
			}
			test.assert(t, rt, test.threadID)
			retained.mu.Lock()
			remaining := len(retained.queue)
			delivered := retained.messageIndexLocked(schedule.Pending.MessageID) >= 0
			spawned := len(retained.spawnedExecutors)
			leases := retained.pruneLeases
			retained.mu.Unlock()
			if remaining != 0 || delivered || spawned != 0 || leases != 0 {
				t.Fatalf("actor after schedule control: queued=%d delivered=%v spawned=%d leases=%d", remaining, delivered, spawned, leases)
			}
			waitForNeoActorSyncIdle(t, retained)
			storedThread, ok := loadNeoThreadFromDir(test.threadID, rt.threadDir)
			if !ok || len(arrayValue(storedThread["queuedMessages"])) != 0 {
				t.Fatalf("persisted queue after schedule control = %#v", storedThread["queuedMessages"])
			}
		})
	}
}

func TestNeoScheduleUnsafeQueuesRemainNonPrunable(t *testing.T) {
	useTempNeoThreadStore(t)
	tests := []struct {
		name   string
		mutate func(*neoRuntime, *neoActor, neoLocalSchedule)
	}{
		{name: "schedule pending", mutate: func(rt *neoRuntime, _ *neoActor, schedule neoLocalSchedule) {
			schedule.Pending = &neoLocalSchedulePending{ID: "SO-pending", MessageID: "M-pending"}
			rt.schedules[schedule.TargetThreadID] = schedule
		}},
		{name: "schedule cancellation", mutate: func(rt *neoRuntime, _ *neoActor, schedule neoLocalSchedule) {
			schedule.Cancellation = &neoLocalScheduleCancellation{}
			rt.schedules[schedule.TargetThreadID] = schedule
		}},
		{name: "schedule deletion", mutate: func(rt *neoRuntime, _ *neoActor, schedule neoLocalSchedule) {
			rt.scheduleDeletions[schedule.TargetThreadID] = neoLocalScheduleDeletion{}
		}},
		{name: "schedule cancelling", mutate: func(rt *neoRuntime, _ *neoActor, schedule neoLocalSchedule) {
			rt.scheduleCancelling[schedule.TargetThreadID] = schedule.ID
		}},
		{name: "directory sync pending", mutate: func(rt *neoRuntime, _ *neoActor, _ neoLocalSchedule) {
			rt.scheduleDirectorySyncPending = true
		}},
		{name: "store unavailable", mutate: func(rt *neoRuntime, _ *neoActor, _ neoLocalSchedule) {
			rt.scheduleStoreErr = errors.New("unavailable")
		}},
		{name: "mixed ordinary queue", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.queue = append(actor.queue, neoQueuedMessage{MessageID: "M-ordinary"})
		}},
		{name: "steer", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.queue[0].Steer = true
		}},
		{name: "client api key", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.queue[0].ClientAPIKey = "secret"
		}},
		{name: "missing prompt", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.queue[0].Content = nil
		}},
		{name: "invalid scheduled time", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.queue[0].Meta["scheduledAt"] = "invalid"
		}},
		{name: "missing created time", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.queue[0].CreatedAt = ""
		}},
		{name: "missing agent mode", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.queue[0].AgentMode = ""
		}},
		{name: "duplicate message id", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			duplicate := actor.queue[0]
			duplicate.Meta = cloneMap(duplicate.Meta)
			duplicate.Meta["automationOccurrenceId"] = "SO-other"
			actor.queue = append(actor.queue, duplicate)
		}},
		{name: "duplicate occurrence id", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			duplicate := actor.queue[0]
			duplicate.MessageID = "M-0000000000000000000999"
			actor.queue = append(actor.queue, duplicate)
		}},
		{name: "pending inference", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.pendingInference = &neoInferenceInflight{agentMode: "deep"}
		}},
		{name: "pending tool", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.pendingTools["TU-pending"] = neoPendingTool{ID: "TU-pending"}
		}},
		{name: "socket", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.sockets[&neoSocket{}] = struct{}{}
		}},
		{name: "executor", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.executorID = "executor-active"
		}},
		{name: "recovered executor", mutate: func(_ *neoRuntime, actor *neoActor, _ neoLocalSchedule) {
			actor.recoveredExecutorPID = 42
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enabled := true
			rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
			threadID := "T-019f7000-0000-7000-8000-000000000123"
			actor := rt.store.ensureThreadActorWithLocalImport(threadID, false)
			schedule := testNeoLocalSchedule(threadID)
			rt.schedules[threadID] = schedule
			actor.mu.Lock()
			actor.lastUsed = time.Now().Add(-time.Hour)
			actor.queue = []neoQueuedMessage{{
				MessageID: "M-0000000000000000000123",
				Content:   []any{map[string]any{"type": "text", "text": "Committed scheduled prompt."}},
				CreatedAt: "2026-07-22T12:00:01Z",
				AgentMode: "deep",
				Meta: map[string]any{
					"automationId":           schedule.ID,
					"automationOccurrenceId": "SO-committed",
					"automationProvider":     "schedule",
					"scheduledAt":            "2026-07-22T12:00:00Z",
				},
			}}
			test.mutate(rt, actor, schedule)
			actor.mu.Unlock()
			if actor.prunable(time.Now(), neoActorIdleTTL, true) {
				t.Fatal("unsafe scheduled queue was prunable")
			}
		})
	}
}

func TestNeoScheduleCorruptStoreBlocksMutation(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
	initial := newNeoRuntime(cfg)
	if err := os.MkdirAll(filepath.Dir(initial.schedulePath), 0o700); err != nil {
		t.Fatal(err)
	}
	corrupt := []byte("{not-json\n")
	if err := os.WriteFile(initial.schedulePath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}

	rt := newNeoRuntime(cfg)
	actor := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000093")
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	_, err := actor.executeLocalScheduleTool("set_schedule", map[string]any{
		"title":    "Blocked write",
		"prompt":   "Do not overwrite the corrupt store.",
		"schedule": "FREQ=DAILY",
	})
	if err == nil || !strings.Contains(err.Error(), "store is unavailable") {
		t.Fatalf("set error = %v", err)
	}
	stored, err := os.ReadFile(initial.schedulePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored) != string(corrupt) {
		t.Fatalf("corrupt store was overwritten: %q", stored)
	}
}

func TestNeoScheduleInvalidStoreBlocksMutation(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "unknown version", raw: `{"version":2,"schedules":[]}`},
		{name: "invalid entry", raw: `{"version":1,"schedules":[{"targetThreadID":"T-019f7000-0000-7000-8000-000000000099"}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useTempNeoThreadStore(t)
			enabled := true
			cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
			initial := newNeoRuntime(cfg)
			if err := os.MkdirAll(filepath.Dir(initial.schedulePath), 0o700); err != nil {
				t.Fatal(err)
			}
			raw := []byte(test.raw + "\n")
			if err := os.WriteFile(initial.schedulePath, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			rt := newNeoRuntime(cfg)
			actor := rt.store.ensureThreadActor("T-019f7000-0000-7000-8000-000000000100")
			actor.updateSettings(map[string]any{"agentMode": "puck"})
			_, err := actor.executeLocalScheduleTool("set_schedule", map[string]any{
				"title":    "Blocked write",
				"prompt":   "Do not overwrite the invalid store.",
				"schedule": "FREQ=DAILY",
			})
			if err == nil || !strings.Contains(err.Error(), "store is unavailable") {
				t.Fatalf("set error = %v", err)
			}
			stored, err := os.ReadFile(initial.schedulePath)
			if err != nil {
				t.Fatal(err)
			}
			if string(stored) != string(raw) {
				t.Fatalf("invalid store was overwritten: %q", stored)
			}
		})
	}
}

func TestNeoSchedulePendingOccurrenceReplaysIdempotently(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
	rt := newNeoRuntime(cfg)
	threadID := "T-019f7000-0000-7000-8000-000000000101"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	if !actor.syncLocalThreadSnapshotForShutdownNow() {
		t.Fatal("initial thread snapshot failed")
	}
	schedule := testNeoLocalSchedule(threadID)
	scheduledAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	schedule.LastRunAt = scheduledAt
	schedule.Pending = &neoLocalSchedulePending{
		ID:          "SO-replay",
		MessageID:   "M-0000000000000000000101",
		ScheduledAt: scheduledAt,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Prompt:      schedule.Prompt,
	}
	rt.schedules[threadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		t.Fatal(err)
	}

	reloaded := newNeoRuntime(cfg)
	loaded, ok := reloaded.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || loaded.Pending == nil {
		t.Fatalf("pending schedule was not reloaded: %#v", loaded)
	}
	dispatched, err := reloaded.dispatchCurrentLocalSchedule(loaded)
	if err != nil || !dispatched {
		t.Fatalf("dispatch result dispatched=%v err=%v", dispatched, err)
	}
	restoredActor := reloaded.store.ensureThreadActor(threadID)
	restoredActor.mu.Lock()
	if len(restoredActor.queue) != 1 || restoredActor.queue[0].MessageID != schedule.Pending.MessageID {
		restoredActor.mu.Unlock()
		t.Fatalf("first delivery queue = %#v", restoredActor.queue)
	}
	restoredActor.mu.Unlock()

	reloaded.schedules[threadID] = loaded
	if err := reloaded.persistLocalSchedulesLocked(); err != nil {
		t.Fatal(err)
	}
	dispatched, err = reloaded.dispatchCurrentLocalSchedule(loaded)
	if err != nil || !dispatched {
		t.Fatalf("replay result dispatched=%v err=%v", dispatched, err)
	}
	restoredActor.mu.Lock()
	defer restoredActor.mu.Unlock()
	if len(restoredActor.queue) != 1 {
		t.Fatalf("replayed queue = %#v", restoredActor.queue)
	}
}

func TestNeoScheduleExecutorReadyWaitsForDurableQueue(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000104"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	actor.mu.Lock()
	actor.executorReady = true
	actor.mu.Unlock()
	inferenceStarted := make(chan struct{}, 1)
	rt.inferStream = func(*neoRuntime, neoInferenceRequest, neoStreamCallback) (neoInferenceResult, error) {
		inferenceStarted <- struct{}{}
		return neoInferenceResult{Text: "done", StopReason: "end_turn"}, nil
	}
	rt.writeLocalSnapshot = func(neoCloudThreadSnapshot, string) (int64, error) {
		return 0, errors.New("injected snapshot failure")
	}
	schedule := testNeoLocalSchedule(threadID)
	scheduledAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	schedule.LastRunAt = scheduledAt
	schedule.Pending = &neoLocalSchedulePending{
		ID:          "SO-durable-first",
		MessageID:   "M-0000000000000000000104",
		ScheduledAt: scheduledAt,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Prompt:      schedule.Prompt,
	}
	rt.schedules[threadID] = schedule
	dispatched, err := rt.dispatchCurrentLocalSchedule(schedule)
	if !dispatched || err == nil || !strings.Contains(err.Error(), "persist scheduled message") {
		t.Fatalf("dispatch result dispatched=%v err=%v", dispatched, err)
	}
	select {
	case <-inferenceStarted:
		t.Fatal("inference started before the scheduled message was durable")
	default:
	}
	actor.mu.Lock()
	if len(actor.queue) != 1 || actor.queue[0].MessageID != schedule.Pending.MessageID {
		actor.mu.Unlock()
		t.Fatalf("durability retry queue = %#v", actor.queue)
	}
	actor.mu.Unlock()
	stored, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || stored.Pending == nil || stored.LastRunError == "" || rt.schedulePersistRetryAt.IsZero() {
		t.Fatalf("dispatch retry state = %#v retryAt=%v", stored, rt.schedulePersistRetryAt)
	}
}

func TestNeoSchedulePauseAndClearCancelDurablePendingMessage(t *testing.T) {
	tests := []struct {
		name       string
		clear      bool
		wantStored bool
		wantQueued int
	}{
		{name: "pause", wantStored: true},
		{name: "clear", clear: true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useTempNeoThreadStore(t)
			enabled := true
			cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
			rt := newNeoRuntime(cfg)
			threadID := []string{
				"T-019f7000-0000-7000-8000-000000000106",
				"T-019f7000-0000-7000-8000-000000000107",
			}[index]
			actor := rt.store.ensureThreadActor(threadID)
			actor.updateSettings(map[string]any{"agentMode": "puck"})
			actor.mu.Lock()
			actor.executorReady = true
			actor.mu.Unlock()
			inferenceStarted := make(chan struct{}, 1)
			rt.inferStream = func(*neoRuntime, neoInferenceRequest, neoStreamCallback) (neoInferenceResult, error) {
				inferenceStarted <- struct{}{}
				return neoInferenceResult{Text: "done", StopReason: "end_turn"}, nil
			}
			schedule := testNeoLocalSchedule(threadID)
			scheduledAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
			schedule.LastRunAt = scheduledAt
			schedule.Pending = &neoLocalSchedulePending{
				ID:          "SO-cancel-" + test.name,
				MessageID:   []string{"M-0000000000000000000106", "M-0000000000000000000107"}[index],
				ScheduledAt: scheduledAt,
				CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
				Prompt:      schedule.Prompt,
			}
			rt.schedules[threadID] = schedule
			originalSchedulePath := rt.schedulePath
			rt.schedulePath = t.TempDir()
			dispatched, err := rt.dispatchCurrentLocalSchedule(schedule)
			if !dispatched || err == nil || !strings.Contains(err.Error(), "persist schedule delivery") {
				t.Fatalf("dispatch result dispatched=%v err=%v", dispatched, err)
			}
			rt.schedulePath = originalSchedulePath
			if test.clear {
				cleared, clearErr := rt.clearLocalScheduleForOwner(threadID, neoLocalOwnerUserID)
				if clearErr != nil || !cleared {
					t.Fatalf("clear result cleared=%v err=%v", cleared, clearErr)
				}
			} else {
				paused, pauseErr := rt.updateLocalSchedule(threadID, neoLocalOwnerUserID, map[string]any{"enabled": false}, "", false)
				if pauseErr != nil || paused.Enabled || paused.Pending != nil {
					t.Fatalf("pause result schedule=%#v err=%v", paused, pauseErr)
				}
			}
			select {
			case <-inferenceStarted:
				t.Fatal("cancelled scheduled occurrence started")
			default:
			}
			actor.mu.Lock()
			if len(actor.queue) != test.wantQueued {
				actor.mu.Unlock()
				t.Fatalf("queue after %s = %#v, want %d messages", test.name, actor.queue, test.wantQueued)
			}
			actor.mu.Unlock()
			reloaded := newNeoRuntime(cfg)
			_, stored := reloaded.getLocalSchedule(threadID, neoLocalOwnerUserID)
			if stored != test.wantStored {
				t.Fatalf("stored schedule present=%v want=%v", stored, test.wantStored)
			}
			reloadedActor := reloaded.store.ensureThreadActor(threadID)
			reloadedActor.mu.Lock()
			defer reloadedActor.mu.Unlock()
			if len(reloadedActor.queue) != test.wantQueued {
				t.Fatalf("reloaded queue after %s = %#v, want %d messages", test.name, reloadedActor.queue, test.wantQueued)
			}
		})
	}
}

func TestNeoSchedulePauseAndClearCancelCommittedQueuedMessage(t *testing.T) {
	tests := []struct {
		name       string
		clear      bool
		wantQueued int
	}{
		{name: "pause"},
		{name: "clear", clear: true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useTempNeoThreadStore(t)
			enabled := true
			cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
			rt := newNeoRuntime(cfg)
			threadID := []string{
				"T-019f7000-0000-7000-8000-000000000109",
				"T-019f7000-0000-7000-8000-000000000110",
			}[index]
			actor := rt.store.ensureThreadActor(threadID)
			actor.updateSettings(map[string]any{"agentMode": "puck"})
			actor.mu.Lock()
			actor.executorReady = true
			actor.agentState = "working"
			actor.currentInference = &neoInferenceInflight{messageID: "M-busy", agentMode: "puck"}
			actor.mu.Unlock()
			schedule := testNeoLocalSchedule(threadID)
			scheduledAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
			schedule.LastRunAt = scheduledAt
			schedule.Pending = &neoLocalSchedulePending{
				ID:          "SO-committed-" + test.name,
				MessageID:   []string{"M-0000000000000000000109", "M-0000000000000000000110"}[index],
				ScheduledAt: scheduledAt,
				CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
				Prompt:      schedule.Prompt,
			}
			rt.schedules[threadID] = schedule
			dispatched, err := rt.dispatchCurrentLocalSchedule(schedule)
			if err != nil || !dispatched {
				t.Fatalf("dispatch result dispatched=%v err=%v", dispatched, err)
			}
			actor.mu.Lock()
			if len(actor.queue) != 1 {
				actor.mu.Unlock()
				t.Fatalf("committed queue = %#v", actor.queue)
			}
			actor.mu.Unlock()
			if stored, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID); !ok || stored.Pending != nil {
				t.Fatalf("committed schedule = %#v", stored)
			}
			if test.clear {
				cleared, clearErr := rt.clearLocalScheduleForOwner(threadID, neoLocalOwnerUserID)
				if clearErr != nil || !cleared {
					t.Fatalf("clear result cleared=%v err=%v", cleared, clearErr)
				}
			} else if _, pauseErr := rt.updateLocalSchedule(threadID, neoLocalOwnerUserID, map[string]any{"enabled": false}, "", false); pauseErr != nil {
				t.Fatal(pauseErr)
			}
			actor.mu.Lock()
			if len(actor.queue) != test.wantQueued {
				actor.mu.Unlock()
				t.Fatalf("queue after %s = %#v, want %d messages", test.name, actor.queue, test.wantQueued)
			}
			actor.mu.Unlock()
			reloaded := newNeoRuntime(cfg)
			reloadedActor := reloaded.store.ensureThreadActor(threadID)
			reloadedActor.mu.Lock()
			defer reloadedActor.mu.Unlock()
			if len(reloadedActor.queue) != test.wantQueued {
				t.Fatalf("reloaded queue after %s = %#v, want %d messages", test.name, reloadedActor.queue, test.wantQueued)
			}
		})
	}
}

func TestNeoScheduleCancellationRollsBackWhenStoreWriteFails(t *testing.T) {
	tests := []struct {
		name  string
		clear bool
	}{
		{name: "pause"},
		{name: "clear", clear: true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useTempNeoThreadStore(t)
			enabled := true
			cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
			rt := newNeoRuntime(cfg)
			threadID := []string{
				"T-019f7000-0000-7000-8000-000000000112",
				"T-019f7000-0000-7000-8000-000000000113",
			}[index]
			actor := rt.store.ensureThreadActor(threadID)
			actor.updateSettings(map[string]any{"agentMode": "puck"})
			actor.mu.Lock()
			actor.executorReady = true
			actor.agentState = "working"
			actor.currentInference = &neoInferenceInflight{messageID: "M-busy", agentMode: "puck"}
			actor.mu.Unlock()
			schedule := testNeoLocalSchedule(threadID)
			scheduledAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
			schedule.LastRunAt = scheduledAt
			schedule.Pending = &neoLocalSchedulePending{
				ID:          "SO-rollback-" + test.name,
				MessageID:   []string{"M-0000000000000000000112", "M-0000000000000000000113"}[index],
				ScheduledAt: scheduledAt,
				CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
				Prompt:      schedule.Prompt,
			}
			rt.schedules[threadID] = schedule
			if dispatched, err := rt.dispatchCurrentLocalSchedule(schedule); err != nil || !dispatched {
				t.Fatalf("dispatch result dispatched=%v err=%v", dispatched, err)
			}
			originalSchedulePath := rt.schedulePath
			rt.schedulePath = t.TempDir()
			if test.clear {
				if cleared, err := rt.clearLocalScheduleForOwner(threadID, neoLocalOwnerUserID); err == nil || cleared {
					t.Fatalf("clear result cleared=%v err=%v", cleared, err)
				}
			} else if _, err := rt.updateLocalSchedule(threadID, neoLocalOwnerUserID, map[string]any{"enabled": false}, "", false); err == nil {
				t.Fatal("pause succeeded despite schedule-store failure")
			}
			rt.schedulePath = originalSchedulePath
			stored, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
			if !ok || !stored.Enabled || stored.Pending != nil {
				t.Fatalf("rolled-back schedule = %#v", stored)
			}
			actor.mu.Lock()
			if len(actor.queue) != 1 || actor.queue[0].MessageID != schedule.Pending.MessageID {
				actor.mu.Unlock()
				t.Fatalf("rolled-back queue = %#v", actor.queue)
			}
			actor.mu.Unlock()
			reloaded := newNeoRuntime(cfg)
			reloadedActor := reloaded.store.ensureThreadActor(threadID)
			reloadedActor.mu.Lock()
			defer reloadedActor.mu.Unlock()
			if len(reloadedActor.queue) != 1 || reloadedActor.queue[0].MessageID != schedule.Pending.MessageID {
				t.Fatalf("reloaded rolled-back queue = %#v", reloadedActor.queue)
			}
		})
	}
}

func TestNeoScheduleCancellationRecoveryRestoresQueuedMessage(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
	rt := newNeoRuntime(cfg)
	threadID := "T-019f7000-0000-7000-8000-000000000114"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	schedule := testNeoLocalSchedule(threadID)
	pending := neoLocalSchedulePending{
		ID:          "SO-recover-cancellation",
		MessageID:   "M-0000000000000000000114",
		ScheduledAt: time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Prompt:      schedule.Prompt,
	}
	schedule.Cancellation = &neoLocalScheduleCancellation{Messages: []neoLocalSchedulePending{pending}}
	rt.schedules[threadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		t.Fatal(err)
	}

	rt.recoverLocalScheduleCancellations()
	stored, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || stored.Cancellation != nil {
		t.Fatalf("recovered schedule = %#v", stored)
	}
	actor.mu.Lock()
	if len(actor.queue) != 1 || actor.queue[0].MessageID != pending.MessageID {
		actor.mu.Unlock()
		t.Fatalf("recovered queue = %#v", actor.queue)
	}
	actor.mu.Unlock()

	reloaded := newNeoRuntime(cfg)
	reloadedActor := reloaded.store.ensureThreadActor(threadID)
	reloadedActor.mu.Lock()
	defer reloadedActor.mu.Unlock()
	if len(reloadedActor.queue) != 1 || reloadedActor.queue[0].MessageID != pending.MessageID {
		t.Fatalf("reloaded recovered queue = %#v", reloadedActor.queue)
	}
}

func TestNeoScheduleCancellationRecoveryWaitsForQueueCapacity(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000120"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	actor.mu.Lock()
	actor.messages = []neoMessage{{ThreadID: threadID, MessageID: "M-bootstrap", Role: "user", Content: []any{map[string]any{"type": "text", "text": "Keep this task available."}}}}
	for index := 0; index < neoMaxQueuedMessages; index++ {
		actor.queue = append(actor.queue, neoQueuedMessage{MessageID: fmt.Sprintf("M-existing-%d", index)})
	}
	actor.rebuildHistoryLocked()
	actor.mu.Unlock()
	schedule := testNeoLocalSchedule(threadID)
	schedule.Cancellation = &neoLocalScheduleCancellation{Messages: []neoLocalSchedulePending{{
		ID:          "SO-capacity",
		MessageID:   "M-0000000000000000000120",
		ScheduledAt: time.Now().UTC().Format(time.RFC3339Nano),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Prompt:      schedule.Prompt,
	}}}
	if err := rt.restoreLocalScheduleCancellationMessages(schedule); err == nil || !strings.Contains(err.Error(), "queue is full") {
		t.Fatalf("restore error = %v, want queue capacity failure", err)
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != neoMaxQueuedMessages {
		t.Fatalf("queue length = %d, want unchanged %d", len(actor.queue), neoMaxQueuedMessages)
	}
}

func TestNeoScheduleCancellationWaitsForScheduleDirectorySync(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000115"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	actor.mu.Lock()
	actor.agentState = "working"
	actor.currentInference = &neoInferenceInflight{messageID: "M-busy", agentMode: "puck"}
	actor.mu.Unlock()
	schedule := testNeoLocalSchedule(threadID)
	schedule.Pending = &neoLocalSchedulePending{
		ID:          "SO-directory-sync",
		MessageID:   "M-0000000000000000000115",
		ScheduledAt: time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Prompt:      schedule.Prompt,
	}
	rt.schedules[threadID] = schedule
	if dispatched, err := rt.dispatchCurrentLocalSchedule(schedule); err != nil || !dispatched {
		t.Fatalf("dispatch result dispatched=%v err=%v", dispatched, err)
	}
	rt.syncScheduleStoreDir = func(string) error { return errors.New("injected directory sync failure") }
	if cleared, err := rt.clearLocalScheduleForOwner(threadID, neoLocalOwnerUserID); err == nil || cleared {
		t.Fatalf("clear result cleared=%v err=%v", cleared, err)
	}
	stored, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || stored.Cancellation == nil {
		t.Fatalf("cancellation intent = %#v", stored)
	}
	actor.mu.Lock()
	if len(actor.queue) != 1 || actor.queue[0].MessageID != schedule.Pending.MessageID {
		actor.mu.Unlock()
		t.Fatalf("queue changed before intent became durable: %#v", actor.queue)
	}
	actor.mu.Unlock()
	rt.syncScheduleStoreDir = nil
	rt.scheduleMu.Lock()
	rt.schedulePersistRetryAt = time.Time{}
	rt.scheduleMu.Unlock()
	rt.fireDueLocalSchedules(time.Now().UTC())
	stored, ok = rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || stored.Cancellation != nil {
		t.Fatalf("recovered schedule = %#v", stored)
	}
}

func TestNeoScheduleCancellationRecoversAfterThreadDirectorySyncFailure(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
	rt := newNeoRuntime(cfg)
	threadID := "T-019f7000-0000-7000-8000-000000000116"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	actor.mu.Lock()
	actor.agentState = "working"
	actor.currentInference = &neoInferenceInflight{messageID: "M-busy", agentMode: "puck"}
	actor.mu.Unlock()
	schedule := testNeoLocalSchedule(threadID)
	schedule.Pending = &neoLocalSchedulePending{
		ID:          "SO-thread-directory-sync",
		MessageID:   "M-0000000000000000000116",
		ScheduledAt: time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Prompt:      schedule.Prompt,
	}
	rt.schedules[threadID] = schedule
	if dispatched, err := rt.dispatchCurrentLocalSchedule(schedule); err != nil || !dispatched {
		t.Fatalf("dispatch result dispatched=%v err=%v", dispatched, err)
	}
	rt.syncScheduledThreadPath = func(string, bool) error { return errors.New("injected thread sync failure") }
	if cleared, err := rt.clearLocalScheduleForOwner(threadID, neoLocalOwnerUserID); err == nil || cleared {
		t.Fatalf("clear result cleared=%v err=%v", cleared, err)
	}
	stored, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || stored.Cancellation == nil {
		t.Fatalf("cancellation intent = %#v", stored)
	}

	reloaded := newNeoRuntime(cfg)
	reloadedSchedule, ok := reloaded.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || reloadedSchedule.Cancellation != nil {
		t.Fatalf("reloaded schedule = %#v", reloadedSchedule)
	}
	reloadedActor := reloaded.store.ensureThreadActor(threadID)
	reloadedActor.mu.Lock()
	defer reloadedActor.mu.Unlock()
	if len(reloadedActor.queue) != 1 || reloadedActor.queue[0].MessageID != schedule.Pending.MessageID {
		t.Fatalf("recovered queue = %#v", reloadedActor.queue)
	}
}

func TestNeoScheduleClearWaitsForQueuedOccurrenceStart(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	inferenceStarted := make(chan struct{}, 1)
	rt.inferStream = func(*neoRuntime, neoInferenceRequest, neoStreamCallback) (neoInferenceResult, error) {
		inferenceStarted <- struct{}{}
		return neoInferenceResult{Text: "done", StopReason: "end_turn"}, nil
	}
	threadID := "T-019f7000-0000-7000-8000-000000000117"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	actor.mu.Lock()
	actor.agentState = "working"
	actor.currentInference = &neoInferenceInflight{messageID: "M-busy", agentMode: "puck"}
	actor.executorReady = true
	actor.mu.Unlock()
	schedule := testNeoLocalSchedule(threadID)
	schedule.Pending = &neoLocalSchedulePending{
		ID:          "SO-start-race",
		MessageID:   "M-0000000000000000000117",
		ScheduledAt: time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Prompt:      schedule.Prompt,
	}
	rt.schedules[threadID] = schedule
	if dispatched, err := rt.dispatchCurrentLocalSchedule(schedule); err != nil || !dispatched {
		t.Fatalf("dispatch result dispatched=%v err=%v", dispatched, err)
	}
	actor.mu.Lock()
	actor.agentState = "idle"
	actor.currentInference = nil
	processDone := make(chan struct{})
	go func() {
		actor.processQueue()
		close(processDone)
	}()
	guard := rt.localScheduleGuard(threadID)
	deadline := time.Now().Add(5 * time.Second)
	for guard.TryLock() {
		guard.Unlock()
		if time.Now().After(deadline) {
			actor.mu.Unlock()
			t.Fatal("queue processor did not acquire the schedule guard")
		}
		time.Sleep(time.Millisecond)
	}
	clearDone := make(chan error, 1)
	go func() {
		_, err := rt.clearLocalScheduleForOwner(threadID, neoLocalOwnerUserID)
		clearDone <- err
	}()
	actor.mu.Unlock()
	select {
	case <-processDone:
	case <-time.After(5 * time.Second):
		t.Fatal("queue processor did not finish")
	}
	select {
	case err := <-clearDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("clear did not finish")
	}
	select {
	case <-inferenceStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduled inference did not start")
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		actor.mu.Lock()
		idle := actor.currentInference == nil && actor.agentState == "idle" && !actor.localSyncRunning && !actor.localSyncPending
		actor.mu.Unlock()
		if idle {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scheduled inference did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	actor.closeLocalSnapshotSyncs()
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.messageIndexLocked(schedule.Pending.MessageID) < 0 {
		t.Fatalf("scheduled occurrence did not start before clear: %#v", actor.messages)
	}
	if len(actor.queue) != 0 {
		t.Fatalf("queue after start and clear = %#v", actor.queue)
	}
}

func TestNeoScheduleOrphanedQueuedOccurrenceStaysBlocked(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000118"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	item := neoQueuedMessage{
		MessageID: "M-0000000000000000000118",
		Content:   []any{map[string]any{"type": "text", "text": "Do not run this cleared occurrence."}},
		Meta: map[string]any{
			"automationId":           "AT-cleared",
			"automationOccurrenceId": "SO-cleared",
			"automationProvider":     "schedule",
			"scheduledAt":            time.Now().UTC().Format(time.RFC3339Nano),
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	actor.mu.Lock()
	actor.executorReady = true
	actor.queue = []neoQueuedMessage{item}
	actor.mu.Unlock()
	rt.scheduleDeletions[threadID] = neoLocalScheduleDeletion{
		ThreadID: threadID,
		Schedule: neoLocalSchedule{ID: "AT-different"},
	}
	actor.processQueue()
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 1 || actor.queue[0].MessageID != item.MessageID {
		t.Fatalf("orphaned scheduled queue = %#v", actor.queue)
	}
	if actor.messageIndexLocked(item.MessageID) >= 0 {
		t.Fatalf("orphaned scheduled occurrence started: %#v", actor.messages)
	}
}

func TestNeoScheduleDeletionWaitsForDispatchAdmission(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000105"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	actor.mu.Lock()
	actor.currentInference = &neoInferenceInflight{messageID: "M-existing", agentMode: "puck"}
	actor.mu.Unlock()
	schedule := testNeoLocalSchedule(threadID)
	scheduledAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	schedule.LastRunAt = scheduledAt
	schedule.Pending = &neoLocalSchedulePending{
		ID:          "SO-delete-race",
		MessageID:   "M-0000000000000000000105",
		ScheduledAt: scheduledAt,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Prompt:      schedule.Prompt,
	}
	rt.schedules[threadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		t.Fatal(err)
	}
	enteredSnapshot := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	var blockOnce sync.Once
	var releaseOnce sync.Once
	releaseSnapshotWrite := func() { releaseOnce.Do(func() { close(releaseSnapshot) }) }
	t.Cleanup(releaseSnapshotWrite)
	rt.writeLocalSnapshot = func(snapshot neoCloudThreadSnapshot, dir string) (int64, error) {
		blockOnce.Do(func() {
			close(enteredSnapshot)
			<-releaseSnapshot
		})
		_, _, size, err := writeNeoLocalThreadSnapshotFileMeasured(snapshot, dir)
		return size, err
	}
	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.dispatchCurrentLocalSchedule(schedule)
		dispatchDone <- err
	}()
	select {
	case <-enteredSnapshot:
	case err := <-dispatchDone:
		t.Fatalf("schedule dispatch completed before snapshot admission: %v", err)
	case <-time.After(time.Second):
		t.Fatal("schedule dispatch did not enter snapshot admission")
	}
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- rt.purgeNeoLocalThread(threadID)
	}()
	select {
	case err := <-deleteDone:
		t.Fatalf("deletion passed dispatch admission guard: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseSnapshotWrite()
	select {
	case err := <-dispatchDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("schedule dispatch did not finish")
	}
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("schedule deletion did not finish")
	}
	if _, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID); ok {
		t.Fatal("schedule remained after deletion")
	}
	if _, err := os.Stat(filepath.Join(rt.threadDir, threadID+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("thread snapshot was recreated: %v", err)
	}
}

func TestNeoScheduleDeletionKeepsIntentUntilDirectoriesSync(t *testing.T) {
	tests := []struct {
		name       string
		failThread bool
	}{
		{name: "summary directory"},
		{name: "thread directory", failThread: true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useTempNeoThreadStore(t)
			enabled := true
			cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
			rt := newNeoRuntime(cfg)
			threadID := []string{
				"T-019f7000-0000-7000-8000-000000000108",
				"T-019f7000-0000-7000-8000-000000000111",
			}[index]
			schedule := testNeoLocalSchedule(threadID)
			rt.schedules[threadID] = schedule
			if err := rt.persistLocalSchedulesLocked(); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(rt.threadDir, 0o700); err != nil {
				t.Fatal(err)
			}
			threadPath := filepath.Join(rt.threadDir, threadID+".json")
			if err := os.WriteFile(threadPath, []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			summaryPath := neoWebLocalThreadSummaryPath(rt.threadDir, threadID)
			if err := os.MkdirAll(filepath.Dir(summaryPath), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(summaryPath, []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			failedDirectory := filepath.Dir(summaryPath)
			if test.failThread {
				failedDirectory = rt.threadDir
			}
			rt.syncScheduleDeletionDir = func(path string) error {
				if path == failedDirectory {
					return errors.New("injected directory sync failure")
				}
				return nil
			}
			if err := rt.purgeNeoLocalThread(threadID); err == nil || !strings.Contains(err.Error(), "directory sync failure") {
				t.Fatalf("purge error = %v", err)
			}
			if _, err := os.Stat(threadPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("thread file remains: %v", err)
			}
			if len(rt.scheduleDeletions) != 1 {
				t.Fatalf("deletion intent = %#v", rt.scheduleDeletions)
			}
			raw, err := os.ReadFile(rt.schedulePath)
			if err != nil {
				t.Fatal(err)
			}
			var stored neoLocalScheduleStore
			if err := json.Unmarshal(raw, &stored); err != nil {
				t.Fatal(err)
			}
			if len(stored.Deletions) != 1 || stored.Deletions[0].ThreadID != threadID {
				t.Fatalf("durable deletion intent = %#v", stored.Deletions)
			}
			reloaded := newNeoRuntime(cfg)
			if _, ok := reloaded.getLocalSchedule(threadID, neoLocalOwnerUserID); ok || len(reloaded.scheduleDeletions) != 0 {
				t.Fatalf("recovered deletion state schedules=%#v deletions=%#v", reloaded.schedules, reloaded.scheduleDeletions)
			}
		})
	}
}

func TestNeoScheduleDeletionReturnsCompletionFailure(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000112"
	rt.schedules[threadID] = testNeoLocalSchedule(threadID)
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rt.threadDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rt.threadDir, threadID+".json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	actor := rt.store.ensureThreadActor(threadID)
	rt.syncScheduleDeletionDir = func(string) error {
		rt.scheduleStoreErr = errors.New("injected schedule completion failure")
		return nil
	}
	err := rt.purgeNeoLocalThread(threadID)
	if err == nil || !strings.Contains(err.Error(), "schedule completion failure") {
		t.Fatalf("purge error = %v", err)
	}
	if rt.store.lookupThreadActor(threadID) == actor || !rt.neoThreadDeleted(threadID) {
		t.Fatal("failed schedule completion did not finish thread deletion")
	}
	if len(rt.scheduleDeletions) != 1 {
		t.Fatalf("schedule deletion intent = %#v", rt.scheduleDeletions)
	}
}

func TestNeoScheduleDeletionIntentRecovers(t *testing.T) {
	tests := []struct {
		name         string
		threadExists bool
		wantSchedule bool
	}{
		{name: "restore before thread removal", threadExists: true, wantSchedule: true},
		{name: "finish after thread removal", threadExists: false, wantSchedule: false},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useTempNeoThreadStore(t)
			enabled := true
			cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
			rt := newNeoRuntime(cfg)
			threadID := []string{
				"T-019f7000-0000-7000-8000-000000000102",
				"T-019f7000-0000-7000-8000-000000000103",
			}[index]
			schedule := testNeoLocalSchedule(threadID)
			rt.scheduleDeletions[threadID] = neoLocalScheduleDeletion{
				ThreadID:  threadID,
				Schedule:  schedule,
				CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
			}
			if err := rt.persistLocalSchedulesLocked(); err != nil {
				t.Fatal(err)
			}
			if test.threadExists {
				if err := os.MkdirAll(rt.threadDir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(rt.threadDir, threadID+".json"), []byte("{}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			reloaded := newNeoRuntime(cfg)
			_, gotSchedule := reloaded.getLocalSchedule(threadID, neoLocalOwnerUserID)
			if gotSchedule != test.wantSchedule {
				t.Fatalf("schedule present = %v, want %v", gotSchedule, test.wantSchedule)
			}
			if len(reloaded.scheduleDeletions) != 0 {
				t.Fatalf("deletion intent remained: %#v", reloaded.scheduleDeletions)
			}
		})
	}
}

func TestNeoScheduleReloadSkipsMissedOccurrence(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	cfg := &config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}}
	rt := newNeoRuntime(cfg)
	now := time.Now().UTC()
	threadID := "T-019f7000-0000-7000-8000-000000000094"
	rt.schedules[threadID] = neoLocalSchedule{
		ID:             "AT-missed",
		Title:          "Hourly check",
		TargetThreadID: threadID,
		Trigger:        neoLocalScheduleTrigger{Provider: "schedule", Schedule: "RRULE:FREQ=HOURLY"},
		Prompt:         "Check now.",
		Enabled:        true,
		NextRunAt:      now.Add(-time.Hour).Format(time.RFC3339Nano),
		CreatedAt:      now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
		UpdatedAt:      now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
		OwnerUserID:    neoLocalOwnerUserID,
		AnchorAt:       now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
		AnchorTimeZone: "UTC",
	}
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		t.Fatal(err)
	}

	reloaded := newNeoRuntime(cfg)
	schedule, ok := reloaded.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok {
		t.Fatal("reloaded schedule missing")
	}
	nextRunAt, err := time.Parse(time.RFC3339Nano, schedule.NextRunAt)
	if err != nil {
		t.Fatal(err)
	}
	if !nextRunAt.After(now) {
		t.Fatalf("missed occurrence retained as next run: %s", nextRunAt)
	}
}

func TestNeoSchedulePersistenceFailureDoesNotDispatch(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000095"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	now := time.Now().UTC()
	original := neoLocalSchedule{
		ID:             "AT-persist-failure",
		Title:          "Hourly check",
		TargetThreadID: threadID,
		Trigger:        neoLocalScheduleTrigger{Provider: "schedule", Schedule: "RRULE:FREQ=HOURLY"},
		Prompt:         "This must not run without durable advancement.",
		Enabled:        true,
		NextRunAt:      now.Add(-time.Minute).Format(time.RFC3339Nano),
		CreatedAt:      now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		UpdatedAt:      now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		OwnerUserID:    neoLocalOwnerUserID,
		AnchorAt:       now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		AnchorTimeZone: "UTC",
	}
	rt.schedules[threadID] = original
	rt.schedulePath = t.TempDir()

	rt.fireDueLocalSchedules(now)
	actor.mu.Lock()
	queued := len(actor.queue)
	actor.mu.Unlock()
	if queued != 0 {
		t.Fatalf("queued messages = %d", queued)
	}
	stored, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID)
	if !ok || stored.LastRunAt != original.LastRunAt || stored.NextRunAt != original.NextRunAt {
		t.Fatalf("schedule advancement was not rolled back: %#v", stored)
	}
	if rt.schedulePersistRetryAt.IsZero() {
		t.Fatal("persistence retry was not scheduled")
	}
}

func TestNeoScheduleDispatchSkipsPausedOccurrence(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000096"
	actor := rt.store.ensureThreadActor(threadID)
	actor.updateSettings(map[string]any{"agentMode": "puck"})
	schedule := neoLocalSchedule{
		ID:             "AT-paused-race",
		TargetThreadID: threadID,
		Prompt:         "Do not queue after pause.",
		OwnerUserID:    neoLocalOwnerUserID,
		LastRunAt:      "2026-07-22T12:00:00Z",
		UpdatedAt:      "2026-07-22T12:00:00Z",
		Pending: &neoLocalSchedulePending{
			ID:          "SO-paused-race",
			MessageID:   "M-0000000000000000000096",
			ScheduledAt: "2026-07-22T12:00:00Z",
			CreatedAt:   "2026-07-22T12:00:00Z",
			Prompt:      "Do not queue after pause.",
		},
	}
	paused := schedule
	paused.Enabled = false
	paused.DisabledReason = "paused"
	paused.UpdatedAt = "2026-07-22T12:00:01Z"
	paused.Pending = nil
	rt.schedules[threadID] = paused
	dispatched, err := rt.dispatchCurrentLocalSchedule(schedule)
	if err != nil {
		t.Fatal(err)
	}
	if dispatched {
		t.Fatal("paused occurrence was dispatched")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if len(actor.queue) != 0 {
		t.Fatalf("queued messages = %#v", actor.queue)
	}
}

func TestNeoStopLocalScheduleTimerDoesNotBlockAfterDelivery(t *testing.T) {
	timer := time.NewTimer(0)
	<-timer.C
	done := make(chan struct{})
	go func() {
		neoStopLocalScheduleTimer(timer)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stopping a delivered schedule timer blocked")
	}
}

func TestNeoScheduleRemovalFailureAbortsThreadDeletion(t *testing.T) {
	useTempNeoThreadStore(t)
	enabled := true
	rt := newNeoRuntime(&config.Config{AmpCode: config.AmpCode{NeoLocalRuntime: config.AmpNeoLocalRuntime{Enabled: &enabled}}})
	threadID := "T-019f7000-0000-7000-8000-000000000097"
	if err := os.MkdirAll(rt.threadDir, 0o700); err != nil {
		t.Fatal(err)
	}
	threadPath := filepath.Join(rt.threadDir, threadID+".json")
	if err := os.WriteFile(threadPath, []byte("thread remains"), 0o600); err != nil {
		t.Fatal(err)
	}
	rt.schedules[threadID] = neoLocalSchedule{ID: "AT-delete", TargetThreadID: threadID, OwnerUserID: neoLocalOwnerUserID}
	rt.schedulePath = t.TempDir()

	if err := rt.purgeNeoLocalThread(threadID); err == nil {
		t.Fatal("thread deletion succeeded despite schedule persistence failure")
	}
	if _, err := os.Stat(threadPath); err != nil {
		t.Fatalf("thread file was removed: %v", err)
	}
	if _, ok := rt.getLocalSchedule(threadID, neoLocalOwnerUserID); !ok {
		t.Fatal("schedule was not restored after failed deletion")
	}
}

func testNeoLocalSchedule(threadID string) neoLocalSchedule {
	now := time.Now().UTC()
	return neoLocalSchedule{
		ID:             "AT-" + randomBase62(22),
		Title:          "Test schedule",
		TargetThreadID: threadID,
		Trigger:        neoLocalScheduleTrigger{Provider: "schedule", Schedule: "RRULE:FREQ=HOURLY"},
		Prompt:         "Run the test schedule.",
		Enabled:        true,
		NextRunAt:      now.Add(time.Hour).Format(time.RFC3339Nano),
		CreatedAt:      now.Format(time.RFC3339Nano),
		UpdatedAt:      now.Format(time.RFC3339Nano),
		OwnerUserID:    neoLocalOwnerUserID,
		AnchorAt:       now.Format(time.RFC3339Nano),
		AnchorTimeZone: "UTC",
	}
}
