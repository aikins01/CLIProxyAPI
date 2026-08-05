package amp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	log "github.com/sirupsen/logrus"
	"github.com/teambition/rrule-go"
)

const (
	neoLocalScheduleStoreFileName = ".cliproxyapi-schedules.json"
	neoLocalScheduleStoreVersion  = 1
	neoLocalScheduleTitleMax      = 256
	neoLocalScheduleLabelMax      = 64
	neoLocalSchedulePromptMax     = 64 * 1024
	neoLocalScheduleRuleMax       = 8 * 1024
	neoLocalScheduleRetryInterval = 5 * time.Second
)

type neoLocalScheduleTrigger struct {
	Provider      string  `json:"provider"`
	Schedule      string  `json:"schedule"`
	ScheduleLabel *string `json:"scheduleLabel"`
}

type neoLocalSchedule struct {
	ID             string                        `json:"id"`
	Title          string                        `json:"title"`
	TargetThreadID string                        `json:"targetThreadID"`
	Trigger        neoLocalScheduleTrigger       `json:"trigger"`
	Prompt         string                        `json:"prompt"`
	Enabled        bool                          `json:"enabled"`
	DisabledReason string                        `json:"disabledReason,omitempty"`
	NextRunAt      string                        `json:"nextRunAt,omitempty"`
	LastRunAt      string                        `json:"lastRunAt,omitempty"`
	LastRunError   string                        `json:"lastRunError,omitempty"`
	CreatedAt      string                        `json:"createdAt"`
	UpdatedAt      string                        `json:"updatedAt"`
	OwnerUserID    string                        `json:"ownerUserID"`
	AnchorAt       string                        `json:"anchorAt"`
	AnchorTimeZone string                        `json:"anchorTimeZone"`
	Pending        *neoLocalSchedulePending      `json:"pending,omitempty"`
	Cancellation   *neoLocalScheduleCancellation `json:"cancellation,omitempty"`
}

type neoLocalSchedulePending struct {
	ID          string `json:"id"`
	MessageID   string `json:"messageID"`
	ScheduledAt string `json:"scheduledAt"`
	CreatedAt   string `json:"createdAt"`
	Prompt      string `json:"prompt"`
}

type neoLocalScheduleDeletion struct {
	ThreadID  string           `json:"threadID"`
	Schedule  neoLocalSchedule `json:"schedule"`
	CreatedAt string           `json:"createdAt"`
}

type neoLocalScheduleCancellation struct {
	Messages []neoLocalSchedulePending `json:"messages"`
}

type neoLocalScheduleStore struct {
	Version   int                        `json:"version"`
	Schedules []neoLocalSchedule         `json:"schedules"`
	Deletions []neoLocalScheduleDeletion `json:"deletions,omitempty"`
}

type neoLocalScheduleDurabilityError struct {
	err error
}

func (e *neoLocalScheduleDurabilityError) Error() string {
	return fmt.Sprintf("schedule store was published but its directory sync is pending: %v", e.err)
}

func (e *neoLocalScheduleDurabilityError) Unwrap() error {
	return e.err
}

func neoLocalSchedulePersistencePublished(err error) bool {
	var durabilityErr *neoLocalScheduleDurabilityError
	return errors.As(err, &durabilityErr)
}

func neoScheduleToolSpec(name string) neoToolSpec {
	properties := map[string]any{}
	required := []any{}
	description := "Inspect the recurring schedule on the current Amp thread. A thread can have at most one schedule."
	switch name {
	case "set_schedule":
		description = "Set one recurring schedule on the current Amp thread. Use only when the user explicitly asks for recurring or future work. Every occurrence adds the saved prompt as a new user message to this thread. Use update_schedule if a schedule already exists."
		properties = neoScheduleWritableProperties(true)
		required = []any{"title", "prompt", "schedule"}
	case "update_schedule":
		description = "Update or pause the recurring schedule on the current Amp thread. Include only fields the user explicitly asked to change. Set enabled to false to pause it and true to resume it."
		properties = neoScheduleWritableProperties(false)
	case "clear_schedule":
		description = "Permanently clear the recurring schedule from the current Amp thread. Use only when the user explicitly asks to remove it."
	}
	return neoToolSpec{
		Name:        name,
		Description: description,
		InputSchema: neoThreadToolSchema(properties, required),
		Meta:        map[string]any{"source": "server"},
	}
}

func neoScheduleWritableProperties(includeAll bool) map[string]any {
	properties := map[string]any{
		"title":         map[string]any{"type": "string", "description": "Short name for the scheduled work, up to 256 characters."},
		"prompt":        map[string]any{"type": "string", "description": "The user message to add to this thread at every occurrence."},
		"schedule":      map[string]any{"type": "string", "description": "RFC 5545 recurrence rule, such as RRULE:FREQ=DAILY;BYHOUR=9;BYMINUTE=0. FREQ=SECONDLY is not supported. DTSTART is optional."},
		"scheduleLabel": map[string]any{"type": []any{"string", "null"}, "description": "Optional human-readable cadence label, up to 64 characters. Use null to clear it."},
		"timeZone":      map[string]any{"type": "string", "description": "Optional IANA time zone used when the rule omits DTSTART, such as America/New_York. Puck normally derives this from the browser."},
		"timezone":      map[string]any{"type": "string", "description": "Alias for timeZone."},
	}
	if !includeAll {
		properties["enabled"] = map[string]any{"type": "boolean", "description": "False pauses future occurrences; true resumes them."}
	}
	return properties
}

func (a *neoActor) executeLocalScheduleTool(name string, input map[string]any) (map[string]any, error) {
	if a == nil || a.runtime == nil {
		return nil, errors.New("schedule tool missing local runtime")
	}
	threadID := firstNonEmptyString(a.threadID, a.key)
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return nil, errors.New("schedule tool requires a valid current thread")
	}
	if err := a.runtime.localScheduleStoreError(); err != nil {
		return nil, err
	}
	ownerUserID := a.threadToolOwnerID()
	switch strings.TrimSpace(name) {
	case "get_schedule":
		schedule, ok := a.runtime.getLocalSchedule(threadID, ownerUserID)
		if !ok {
			return a.runtime.localScheduleToolResult(map[string]any{"schedule": nil}), nil
		}
		return a.runtime.localScheduleToolResult(map[string]any{"schedule": schedule.response()}), nil
	case "set_schedule":
		schedule, err := a.runtime.setLocalSchedule(threadID, ownerUserID, input, a.localScheduleTimeZone(input))
		if err != nil {
			return nil, err
		}
		return a.runtime.localScheduleToolResult(map[string]any{"schedule": schedule.response()}), nil
	case "update_schedule":
		timeZone, timeZoneExplicit := neoExplicitLocalScheduleTimeZone(input)
		schedule, err := a.runtime.updateLocalSchedule(threadID, ownerUserID, input, timeZone, timeZoneExplicit)
		if err != nil {
			return nil, err
		}
		return a.runtime.localScheduleToolResult(map[string]any{"schedule": schedule.response()}), nil
	case "clear_schedule":
		cleared, err := a.runtime.clearLocalScheduleForOwner(threadID, ownerUserID)
		if err != nil {
			return nil, err
		}
		return a.runtime.localScheduleToolResult(map[string]any{"cleared": cleared, "threadId": threadID}), nil
	default:
		return nil, errors.New("unsupported schedule tool")
	}
}

func (rt *neoRuntime) localScheduleToolResult(result map[string]any) map[string]any {
	if rt == nil {
		return result
	}
	rt.scheduleMu.Lock()
	pending := rt.scheduleDirectorySyncPending
	rt.scheduleMu.Unlock()
	if pending {
		result["durabilityPending"] = true
		result["durabilityMessage"] = "The schedule change is active, but its directory sync is still being retried. Scheduled work will remain blocked until that sync completes."
	}
	return result
}

func neoExplicitLocalScheduleTimeZone(input map[string]any) (string, bool) {
	for _, key := range []string{"timeZone", "timezone"} {
		raw, exists := input[key]
		if !exists {
			continue
		}
		value, ok := raw.(string)
		if !ok || strings.TrimSpace(value) == "" {
			return "", true
		}
		return strings.TrimSpace(value), true
	}
	return "", false
}

func (a *neoActor) localScheduleTimeZone(input map[string]any) string {
	if value := strings.TrimSpace(firstNonEmptyString(input["timeZone"], input["timezone"])); value != "" {
		return value
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for index := len(a.messages) - 1; index >= 0; index-- {
		if value := neoScheduleTimeZoneFromUserState(a.messages[index].UserState); value != "" {
			return value
		}
	}
	for index := len(a.queue) - 1; index >= 0; index-- {
		if value := neoScheduleTimeZoneFromUserState(a.queue[index].UserState); value != "" {
			return value
		}
	}
	return neoAmpTimezone()
}

func neoScheduleTimeZoneFromUserState(userState any) string {
	state := mapValue(userState)
	if puckContext := mapValue(state["puckContext"]); len(puckContext) > 0 {
		return strings.TrimSpace(firstNonEmptyString(puckContext["timeZone"], puckContext["timezone"]))
	}
	return strings.TrimSpace(firstNonEmptyString(state["timeZone"], state["timezone"]))
}

func neoLocalScheduleStorePath(threadDir string) string {
	threadDir = strings.TrimSpace(threadDir)
	if threadDir == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(threadDir), neoLocalScheduleStoreFileName)
}

func (rt *neoRuntime) loadLocalSchedules() {
	if rt == nil || strings.TrimSpace(rt.schedulePath) == "" {
		return
	}
	raw, err := os.ReadFile(rt.schedulePath)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		rt.scheduleStoreErr = fmt.Errorf("read schedule store: %w", err)
		log.Warnf("amp neo local runtime schedule store read failed: %v", err)
		return
	}
	var stored neoLocalScheduleStore
	if err := json.Unmarshal(raw, &stored); err != nil {
		rt.scheduleStoreErr = fmt.Errorf("decode schedule store: %w", err)
		log.Warnf("amp neo local runtime schedule store decode failed: %v", err)
		return
	}
	if stored.Version != neoLocalScheduleStoreVersion {
		rt.scheduleStoreErr = fmt.Errorf("unsupported schedule store version %d", stored.Version)
		log.Warnf("amp neo local runtime schedule store version unsupported: %d", stored.Version)
		return
	}
	now := time.Now().UTC()
	schedules := make(map[string]neoLocalSchedule, len(stored.Schedules))
	deletions := make(map[string]neoLocalScheduleDeletion, len(stored.Deletions))
	changed := false
	for _, schedule := range stored.Schedules {
		if err := neoValidateLocalSchedule(schedule); err != nil {
			rt.scheduleStoreErr = fmt.Errorf("invalid schedule for thread %q: %w", schedule.TargetThreadID, err)
			log.Warnf("amp neo local runtime schedule store validation failed: %v", rt.scheduleStoreErr)
			return
		}
		if _, exists := schedules[schedule.TargetThreadID]; exists {
			rt.scheduleStoreErr = fmt.Errorf("duplicate schedule for thread %q", schedule.TargetThreadID)
			log.Warnf("amp neo local runtime schedule store validation failed: %v", rt.scheduleStoreErr)
			return
		}
		if schedule.Enabled {
			parsedNextRunAt, nextRunErr := time.Parse(time.RFC3339Nano, schedule.NextRunAt)
			if nextRunErr != nil || !parsedNextRunAt.After(now) {
				next, nextErr := neoLocalScheduleNext(schedule, now)
				if nextErr != nil {
					rt.scheduleStoreErr = fmt.Errorf("invalid schedule for thread %q: %w", schedule.TargetThreadID, nextErr)
					log.Warnf("amp neo local runtime schedule store validation failed: %v", rt.scheduleStoreErr)
					return
				}
				if next.IsZero() {
					schedule.Enabled = false
					schedule.DisabledReason = "schedule_exhausted"
					schedule.NextRunAt = ""
				} else {
					schedule.NextRunAt = next.Format(time.RFC3339Nano)
				}
				changed = true
			}
		}
		schedules[schedule.TargetThreadID] = schedule
	}
	for _, deletion := range stored.Deletions {
		if deletion.ThreadID != deletion.Schedule.TargetThreadID || !neoThreadIDExactPattern.MatchString(deletion.ThreadID) {
			rt.scheduleStoreErr = fmt.Errorf("invalid schedule deletion target %q", deletion.ThreadID)
			log.Warnf("amp neo local runtime schedule store validation failed: %v", rt.scheduleStoreErr)
			return
		}
		if err := neoValidateLocalSchedule(deletion.Schedule); err != nil {
			rt.scheduleStoreErr = fmt.Errorf("invalid schedule deletion for thread %q: %w", deletion.ThreadID, err)
			log.Warnf("amp neo local runtime schedule store validation failed: %v", rt.scheduleStoreErr)
			return
		}
		if _, err := time.Parse(time.RFC3339Nano, deletion.CreatedAt); err != nil {
			rt.scheduleStoreErr = fmt.Errorf("invalid schedule deletion timestamp for thread %q", deletion.ThreadID)
			log.Warnf("amp neo local runtime schedule store validation failed: %v", rt.scheduleStoreErr)
			return
		}
		if _, exists := schedules[deletion.ThreadID]; exists {
			rt.scheduleStoreErr = fmt.Errorf("schedule and deletion overlap for thread %q", deletion.ThreadID)
			log.Warnf("amp neo local runtime schedule store validation failed: %v", rt.scheduleStoreErr)
			return
		}
		if _, exists := deletions[deletion.ThreadID]; exists {
			rt.scheduleStoreErr = fmt.Errorf("duplicate schedule deletion for thread %q", deletion.ThreadID)
			log.Warnf("amp neo local runtime schedule store validation failed: %v", rt.scheduleStoreErr)
			return
		}
		deletions[deletion.ThreadID] = deletion
	}
	rt.schedules = schedules
	rt.scheduleDeletions = deletions
	for threadID, deletion := range rt.scheduleDeletions {
		_, statErr := os.Stat(filepath.Join(rt.threadDir, threadID+".json"))
		switch {
		case statErr == nil:
			schedule := deletion.Schedule
			if schedule.Enabled {
				nextRunAt, nextRunErr := time.Parse(time.RFC3339Nano, schedule.NextRunAt)
				if nextRunErr != nil || !nextRunAt.After(now) {
					next, recurrenceErr := neoLocalScheduleNext(schedule, now)
					if recurrenceErr != nil {
						rt.scheduleStoreErr = fmt.Errorf("recover schedule deletion for thread %q: %w", threadID, recurrenceErr)
						log.Warnf("amp neo local runtime schedule deletion recovery failed: %v", rt.scheduleStoreErr)
						return
					}
					if next.IsZero() {
						schedule.Enabled = false
						schedule.DisabledReason = "schedule_exhausted"
						schedule.NextRunAt = ""
					} else {
						schedule.NextRunAt = next.Format(time.RFC3339Nano)
					}
				}
			}
			rt.schedules[threadID] = schedule
		case errors.Is(statErr, os.ErrNotExist):
		default:
			rt.scheduleStoreErr = fmt.Errorf("inspect thread for schedule deletion recovery: %w", statErr)
			log.Warnf("amp neo local runtime schedule deletion recovery failed: %v", statErr)
			return
		}
		delete(rt.scheduleDeletions, threadID)
		changed = true
	}
	if changed {
		if err := rt.persistLocalSchedulesLocked(); err != nil {
			if !neoLocalSchedulePersistencePublished(err) {
				rt.scheduleStoreErr = fmt.Errorf("persist recovered schedule store: %w", err)
			}
			log.Warnf("amp neo local runtime schedule recovery persistence incomplete: %v", err)
		}
	}
}

func neoValidateLocalSchedule(schedule neoLocalSchedule) error {
	if strings.TrimSpace(schedule.ID) == "" {
		return errors.New("schedule ID is required")
	}
	if strings.TrimSpace(schedule.Title) == "" || utf8.RuneCountInString(schedule.Title) > neoLocalScheduleTitleMax {
		return errors.New("schedule title is invalid")
	}
	if !neoThreadIDExactPattern.MatchString(schedule.TargetThreadID) {
		return errors.New("target thread ID is invalid")
	}
	if schedule.Trigger.Provider != "schedule" {
		return errors.New("schedule provider is invalid")
	}
	if strings.TrimSpace(schedule.Trigger.Schedule) == "" || utf8.RuneCountInString(schedule.Trigger.Schedule) > neoLocalScheduleRuleMax {
		return errors.New("recurrence rule is invalid")
	}
	if schedule.Trigger.ScheduleLabel != nil && utf8.RuneCountInString(*schedule.Trigger.ScheduleLabel) > neoLocalScheduleLabelMax {
		return errors.New("schedule label is invalid")
	}
	if strings.TrimSpace(schedule.Prompt) == "" || utf8.RuneCountInString(schedule.Prompt) > neoLocalSchedulePromptMax {
		return errors.New("schedule prompt is invalid")
	}
	if strings.TrimSpace(schedule.OwnerUserID) == "" {
		return errors.New("schedule owner is required")
	}
	for name, value := range map[string]string{
		"createdAt": schedule.CreatedAt,
		"updatedAt": schedule.UpdatedAt,
		"anchorAt":  schedule.AnchorAt,
	} {
		if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
			return fmt.Errorf("%s is invalid", name)
		}
	}
	for name, value := range map[string]string{
		"nextRunAt": schedule.NextRunAt,
		"lastRunAt": schedule.LastRunAt,
	} {
		if value != "" {
			if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
				return fmt.Errorf("%s is invalid", name)
			}
		}
	}
	if schedule.Enabled && schedule.NextRunAt == "" {
		return errors.New("enabled schedule is missing nextRunAt")
	}
	if _, _, err := neoLocalScheduleLocation(schedule.AnchorTimeZone); err != nil {
		return err
	}
	if schedule.Enabled || schedule.DisabledReason != "invalid_schedule" {
		if _, err := neoLocalScheduleNext(schedule, time.Now().UTC()); err != nil {
			return err
		}
	}
	if schedule.Pending != nil {
		if err := neoValidateLocalSchedulePending(*schedule.Pending); err != nil {
			return err
		}
		pending := schedule.Pending
		if schedule.LastRunAt == "" || schedule.LastRunAt != pending.ScheduledAt {
			return errors.New("pending occurrence does not match lastRunAt")
		}
	}
	if schedule.Cancellation != nil {
		if len(schedule.Cancellation.Messages) == 0 {
			return errors.New("schedule cancellation has no messages")
		}
		seen := map[string]struct{}{}
		for _, pending := range schedule.Cancellation.Messages {
			if err := neoValidateLocalSchedulePending(pending); err != nil {
				return fmt.Errorf("invalid cancellation message: %w", err)
			}
			if _, exists := seen[pending.MessageID]; exists {
				return errors.New("schedule cancellation has duplicate messages")
			}
			seen[pending.MessageID] = struct{}{}
		}
	}
	return nil
}

func neoValidateLocalSchedulePending(pending neoLocalSchedulePending) error {
	if strings.TrimSpace(pending.ID) == "" || strings.TrimSpace(pending.MessageID) == "" {
		return errors.New("pending occurrence ID is invalid")
	}
	if strings.TrimSpace(pending.Prompt) == "" || utf8.RuneCountInString(pending.Prompt) > neoLocalSchedulePromptMax {
		return errors.New("pending occurrence prompt is invalid")
	}
	if _, err := time.Parse(time.RFC3339Nano, pending.ScheduledAt); err != nil {
		return errors.New("pending occurrence scheduledAt is invalid")
	}
	if _, err := time.Parse(time.RFC3339Nano, pending.CreatedAt); err != nil {
		return errors.New("pending occurrence createdAt is invalid")
	}
	return nil
}

func (rt *neoRuntime) localScheduleStoreError() error {
	if rt == nil {
		return errors.New("local schedule runtime is unavailable")
	}
	rt.scheduleMu.Lock()
	defer rt.scheduleMu.Unlock()
	if rt.scheduleStoreErr == nil {
		return nil
	}
	return fmt.Errorf("local schedule store is unavailable: %w", rt.scheduleStoreErr)
}

func (rt *neoRuntime) getLocalSchedule(threadID, ownerUserID string) (neoLocalSchedule, bool) {
	if rt == nil {
		return neoLocalSchedule{}, false
	}
	rt.scheduleMu.Lock()
	defer rt.scheduleMu.Unlock()
	schedule, ok := rt.schedules[threadID]
	if !ok || ownerUserID != "" && schedule.OwnerUserID != ownerUserID {
		return neoLocalSchedule{}, false
	}
	return schedule, true
}

func (rt *neoRuntime) localScheduleGuard(threadID string) *sync.Mutex {
	hash := uint32(2166136261)
	for index := 0; index < len(threadID); index++ {
		hash ^= uint32(threadID[index])
		hash *= 16777619
	}
	return &rt.scheduleGuards[int(hash%uint32(len(rt.scheduleGuards)))]
}

func (rt *neoRuntime) setLocalSchedule(threadID, ownerUserID string, input map[string]any, timeZone string) (neoLocalSchedule, error) {
	if rt == nil || strings.TrimSpace(rt.schedulePath) == "" {
		return neoLocalSchedule{}, errors.New("local schedule persistence is unavailable")
	}
	title, err := neoRequiredScheduleText(input, "title", neoLocalScheduleTitleMax)
	if err != nil {
		return neoLocalSchedule{}, err
	}
	prompt, err := neoRequiredScheduleText(input, "prompt", neoLocalSchedulePromptMax)
	if err != nil {
		return neoLocalSchedule{}, err
	}
	rule, err := neoRequiredScheduleText(input, "schedule", neoLocalScheduleRuleMax)
	if err != nil {
		return neoLocalSchedule{}, err
	}
	label, err := neoLocalScheduleLabel(input)
	if err != nil {
		return neoLocalSchedule{}, err
	}
	now := time.Now().UTC()
	canonicalRule, anchorTimeZone, next, err := neoParseLocalSchedule(rule, timeZone, now, now)
	if err != nil {
		return neoLocalSchedule{}, err
	}
	if next.IsZero() {
		return neoLocalSchedule{}, errors.New("schedule has no future occurrences")
	}
	schedule := neoLocalSchedule{
		ID:             "AT-" + randomBase62(22),
		Title:          title,
		TargetThreadID: threadID,
		Trigger: neoLocalScheduleTrigger{
			Provider:      "schedule",
			Schedule:      canonicalRule,
			ScheduleLabel: label,
		},
		Prompt:         prompt,
		Enabled:        true,
		NextRunAt:      next.Format(time.RFC3339Nano),
		CreatedAt:      now.Format(time.RFC3339Nano),
		UpdatedAt:      now.Format(time.RFC3339Nano),
		OwnerUserID:    ownerUserID,
		AnchorAt:       now.Format(time.RFC3339Nano),
		AnchorTimeZone: anchorTimeZone,
	}
	guard := rt.localScheduleGuard(threadID)
	guard.Lock()
	defer guard.Unlock()
	rt.scheduleMu.Lock()
	defer rt.scheduleMu.Unlock()
	if _, exists := rt.schedules[threadID]; exists {
		return neoLocalSchedule{}, errors.New("current thread already has a schedule; use update_schedule or clear_schedule")
	}
	if _, deleting := rt.scheduleDeletions[threadID]; deleting || rt.neoCloudThreadSyncBlocked(threadID) {
		return neoLocalSchedule{}, errors.New("current thread is being deleted")
	}
	rt.schedules[threadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		if neoLocalSchedulePersistencePublished(err) {
			rt.wakeLocalScheduleLoop()
			return schedule, nil
		}
		delete(rt.schedules, threadID)
		return neoLocalSchedule{}, fmt.Errorf("persist schedule: %w", err)
	}
	if !rt.scheduleDirectorySyncPending {
		rt.schedulePersistRetryAt = time.Time{}
	}
	rt.wakeLocalScheduleLoop()
	return schedule, nil
}

func (rt *neoRuntime) migrateLocalScheduleOwner(threadID, ownerUserID string) error {
	ownerUserID = strings.TrimSpace(ownerUserID)
	if rt == nil || ownerUserID == "" || ownerUserID == neoLocalOwnerUserID {
		return nil
	}
	rt.scheduleMu.Lock()
	defer rt.scheduleMu.Unlock()
	schedule, exists := rt.schedules[threadID]
	if !exists || schedule.OwnerUserID == ownerUserID {
		return nil
	}
	if schedule.OwnerUserID != neoLocalOwnerUserID {
		return errors.New("schedule belongs to a different owner")
	}
	previous := schedule
	schedule.OwnerUserID = ownerUserID
	schedule.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	rt.schedules[threadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		if neoLocalSchedulePersistencePublished(err) {
			rt.wakeLocalScheduleLoop()
			return nil
		}
		rt.schedules[threadID] = previous
		return fmt.Errorf("persist schedule owner migration: %w", err)
	}
	if !rt.scheduleDirectorySyncPending {
		rt.schedulePersistRetryAt = time.Time{}
	}
	rt.wakeLocalScheduleLoop()
	return nil
}

func (rt *neoRuntime) updateLocalSchedule(threadID, ownerUserID string, input map[string]any, timeZone string, timeZoneExplicit bool) (neoLocalSchedule, error) {
	if rt == nil {
		return neoLocalSchedule{}, errors.New("local schedule runtime is unavailable")
	}
	if enabled, ok := input["enabled"].(bool); ok && !enabled && rt.store != nil {
		if actor, release := rt.store.retainThreadActorWithoutReadyWork(threadID); actor != nil {
			defer release()
			defer actor.processQueue()
		}
	}
	guard := rt.localScheduleGuard(threadID)
	guard.Lock()
	defer guard.Unlock()
	rt.scheduleMu.Lock()
	defer rt.scheduleMu.Unlock()
	schedule, exists := rt.schedules[threadID]
	if !exists || schedule.OwnerUserID != ownerUserID {
		return neoLocalSchedule{}, errors.New("current thread does not have a schedule")
	}
	if schedule.Cancellation != nil {
		return neoLocalSchedule{}, errors.New("schedule cancellation recovery is still pending")
	}
	previous := schedule
	changed := false
	pauseRequested := false
	if _, exists := input["title"]; exists {
		value, err := neoRequiredScheduleText(input, "title", neoLocalScheduleTitleMax)
		if err != nil {
			return neoLocalSchedule{}, err
		}
		schedule.Title = value
		changed = true
	}
	if _, exists := input["prompt"]; exists {
		value, err := neoRequiredScheduleText(input, "prompt", neoLocalSchedulePromptMax)
		if err != nil {
			return neoLocalSchedule{}, err
		}
		schedule.Prompt = value
		changed = true
	}
	if _, exists := input["scheduleLabel"]; exists {
		label, err := neoLocalScheduleLabel(input)
		if err != nil {
			return neoLocalSchedule{}, err
		}
		schedule.Trigger.ScheduleLabel = label
		changed = true
	}
	now := time.Now().UTC()
	_, scheduleExplicit := input["schedule"]
	if scheduleExplicit || timeZoneExplicit {
		rule := schedule.Trigger.Schedule
		if scheduleExplicit {
			var err error
			rule, err = neoRequiredScheduleText(input, "schedule", neoLocalScheduleRuleMax)
			if err != nil {
				return neoLocalSchedule{}, err
			}
		}
		if !timeZoneExplicit {
			timeZone = schedule.AnchorTimeZone
		} else if strings.TrimSpace(timeZone) == "" {
			return neoLocalSchedule{}, errors.New("timeZone must be a non-empty IANA time zone")
		}
		canonicalRule, anchorTimeZone, next, err := neoParseLocalSchedule(rule, timeZone, now, now)
		if err != nil {
			return neoLocalSchedule{}, err
		}
		if next.IsZero() {
			return neoLocalSchedule{}, errors.New("schedule has no future occurrences")
		}
		schedule.Trigger.Schedule = canonicalRule
		schedule.AnchorAt = now.Format(time.RFC3339Nano)
		schedule.AnchorTimeZone = anchorTimeZone
		if schedule.Enabled {
			schedule.NextRunAt = next.Format(time.RFC3339Nano)
			schedule.DisabledReason = ""
		} else {
			schedule.NextRunAt = ""
			if schedule.DisabledReason == "" || schedule.DisabledReason == "schedule_exhausted" || schedule.DisabledReason == "invalid_schedule" {
				schedule.DisabledReason = "paused"
			}
		}
		changed = true
	}
	if rawEnabled, exists := input["enabled"]; exists {
		enabled, ok := rawEnabled.(bool)
		if !ok {
			return neoLocalSchedule{}, errors.New("enabled must be a boolean")
		}
		schedule.Enabled = enabled
		if enabled {
			next, err := neoLocalScheduleNext(schedule, now)
			if err != nil {
				return neoLocalSchedule{}, err
			}
			if next.IsZero() {
				return neoLocalSchedule{}, errors.New("schedule has no future occurrences")
			}
			schedule.NextRunAt = next.Format(time.RFC3339Nano)
			schedule.DisabledReason = ""
		} else {
			pauseRequested = true
			schedule.NextRunAt = ""
			schedule.DisabledReason = "paused"
			schedule.Pending = nil
			schedule.LastRunError = ""
		}
		changed = true
	}
	if !changed {
		return neoLocalSchedule{}, errors.New("update_schedule requires at least one field to change")
	}
	cancellationIntent := previous
	hasCancellationIntent := false
	if pauseRequested {
		var cancelErr error
		cancellationIntent, hasCancellationIntent, cancelErr = rt.prepareLocalScheduleCancellationLocked(previous)
		if cancelErr != nil {
			return neoLocalSchedule{}, cancelErr
		}
		schedule.Cancellation = nil
	}
	schedule.UpdatedAt = now.Format(time.RFC3339Nano)
	rt.schedules[threadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		if neoLocalSchedulePersistencePublished(err) {
			rt.wakeLocalScheduleLoop()
			return schedule, nil
		}
		if hasCancellationIntent {
			rt.schedules[threadID] = cancellationIntent
			rt.schedulePersistRetryAt = time.Now().UTC().Add(neoLocalScheduleRetryInterval)
			rt.wakeLocalScheduleLoop()
		} else {
			rt.schedules[threadID] = previous
		}
		return neoLocalSchedule{}, fmt.Errorf("persist schedule: %w", err)
	}
	if !rt.scheduleDirectorySyncPending {
		rt.schedulePersistRetryAt = time.Time{}
	}
	rt.wakeLocalScheduleLoop()
	return schedule, nil
}

func (rt *neoRuntime) clearLocalScheduleForOwner(threadID, ownerUserID string) (bool, error) {
	_, cleared, err := rt.takeLocalSchedule(threadID, ownerUserID)
	return cleared, err
}

func (rt *neoRuntime) clearLocalSchedule(threadID, ownerUserID string) error {
	_, _, err := rt.takeLocalSchedule(threadID, ownerUserID)
	return err
}

func (rt *neoRuntime) takeLocalSchedule(threadID, ownerUserID string) (neoLocalSchedule, bool, error) {
	if rt == nil {
		return neoLocalSchedule{}, false, nil
	}
	if rt.store != nil {
		if actor, release := rt.store.retainThreadActorWithoutReadyWork(threadID); actor != nil {
			defer release()
			defer actor.processQueue()
		}
	}
	guard := rt.localScheduleGuard(threadID)
	guard.Lock()
	defer guard.Unlock()
	rt.scheduleMu.Lock()
	defer rt.scheduleMu.Unlock()
	if rt.scheduleStoreErr != nil {
		return neoLocalSchedule{}, false, fmt.Errorf("schedule store was not loaded safely: %w", rt.scheduleStoreErr)
	}
	previous, exists := rt.schedules[threadID]
	if !exists || ownerUserID != "" && previous.OwnerUserID != ownerUserID {
		return neoLocalSchedule{}, false, nil
	}
	cancellationIntent, hasCancellationIntent, cancelErr := rt.prepareLocalScheduleCancellationLocked(previous)
	if cancelErr != nil {
		return neoLocalSchedule{}, false, cancelErr
	}
	delete(rt.schedules, threadID)
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		if neoLocalSchedulePersistencePublished(err) {
			rt.wakeLocalScheduleLoop()
			return previous, true, nil
		}
		if hasCancellationIntent {
			rt.schedules[threadID] = cancellationIntent
			rt.schedulePersistRetryAt = time.Now().UTC().Add(neoLocalScheduleRetryInterval)
			rt.wakeLocalScheduleLoop()
		} else {
			rt.schedules[threadID] = previous
		}
		return neoLocalSchedule{}, false, fmt.Errorf("persist schedule removal: %w", err)
	}
	if !rt.scheduleDirectorySyncPending {
		rt.schedulePersistRetryAt = time.Time{}
	}
	rt.wakeLocalScheduleLoop()
	return previous, true, nil
}

func (rt *neoRuntime) beginLocalScheduleDeletion(threadID string) (bool, error) {
	if rt == nil {
		return false, nil
	}
	guard := rt.localScheduleGuard(threadID)
	guard.Lock()
	defer guard.Unlock()
	rt.scheduleMu.Lock()
	defer rt.scheduleMu.Unlock()
	if rt.scheduleStoreErr != nil {
		return false, fmt.Errorf("schedule store was not loaded safely: %w", rt.scheduleStoreErr)
	}
	schedule, exists := rt.schedules[threadID]
	if !exists {
		if _, deleting := rt.scheduleDeletions[threadID]; deleting {
			return true, nil
		}
		return false, nil
	}
	deletion := neoLocalScheduleDeletion{
		ThreadID:  threadID,
		Schedule:  schedule,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	delete(rt.schedules, threadID)
	rt.scheduleDeletions[threadID] = deletion
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		if neoLocalSchedulePersistencePublished(err) {
			rt.wakeLocalScheduleLoop()
			return true, fmt.Errorf("persist schedule deletion intent: %w", err)
		}
		delete(rt.scheduleDeletions, threadID)
		rt.schedules[threadID] = schedule
		return false, fmt.Errorf("persist schedule deletion intent: %w", err)
	}
	if !rt.scheduleDirectorySyncPending {
		rt.schedulePersistRetryAt = time.Time{}
	}
	rt.wakeLocalScheduleLoop()
	return true, nil
}

func (rt *neoRuntime) rollbackLocalScheduleDeletion(threadID string) error {
	if rt == nil {
		return nil
	}
	guard := rt.localScheduleGuard(threadID)
	guard.Lock()
	defer guard.Unlock()
	rt.scheduleMu.Lock()
	defer rt.scheduleMu.Unlock()
	deletion, exists := rt.scheduleDeletions[threadID]
	if !exists {
		return nil
	}
	delete(rt.scheduleDeletions, threadID)
	rt.schedules[threadID] = deletion.Schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		if neoLocalSchedulePersistencePublished(err) {
			rt.wakeLocalScheduleLoop()
			return fmt.Errorf("persist schedule deletion rollback: %w", err)
		}
		delete(rt.schedules, threadID)
		rt.scheduleDeletions[threadID] = deletion
		return fmt.Errorf("persist schedule deletion rollback: %w", err)
	}
	if !rt.scheduleDirectorySyncPending {
		rt.schedulePersistRetryAt = time.Time{}
	}
	rt.wakeLocalScheduleLoop()
	return nil
}

func (rt *neoRuntime) completeLocalScheduleDeletion(threadID string) error {
	if rt == nil {
		return nil
	}
	guard := rt.localScheduleGuard(threadID)
	guard.Lock()
	defer guard.Unlock()
	rt.scheduleMu.Lock()
	defer rt.scheduleMu.Unlock()
	deletion, exists := rt.scheduleDeletions[threadID]
	if !exists {
		return nil
	}
	delete(rt.scheduleDeletions, threadID)
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		if neoLocalSchedulePersistencePublished(err) {
			rt.wakeLocalScheduleLoop()
			return fmt.Errorf("persist schedule deletion completion: %w", err)
		}
		rt.scheduleDeletions[threadID] = deletion
		return fmt.Errorf("persist schedule deletion completion: %w", err)
	}
	return nil
}

func (rt *neoRuntime) persistLocalSchedulesLocked() error {
	if rt.scheduleStoreErr != nil {
		return fmt.Errorf("schedule store was not loaded safely: %w", rt.scheduleStoreErr)
	}
	if strings.TrimSpace(rt.schedulePath) == "" {
		return errors.New("schedule store path is unavailable")
	}
	schedules := make([]neoLocalSchedule, 0, len(rt.schedules))
	for _, schedule := range rt.schedules {
		schedules = append(schedules, schedule)
	}
	sort.Slice(schedules, func(i, j int) bool {
		return schedules[i].TargetThreadID < schedules[j].TargetThreadID
	})
	deletions := make([]neoLocalScheduleDeletion, 0, len(rt.scheduleDeletions))
	for _, deletion := range rt.scheduleDeletions {
		deletions = append(deletions, deletion)
	}
	sort.Slice(deletions, func(i, j int) bool {
		return deletions[i].ThreadID < deletions[j].ThreadID
	})
	raw, err := json.MarshalIndent(neoLocalScheduleStore{Version: neoLocalScheduleStoreVersion, Schedules: schedules, Deletions: deletions}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(rt.schedulePath), 0o700); err != nil {
		return err
	}
	if err := writeNeoDurableAtomicFile(rt.schedulePath, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	var syncErr error
	if rt.syncScheduleStoreDir != nil {
		syncErr = rt.syncScheduleStoreDir(filepath.Dir(rt.schedulePath))
	} else {
		syncErr = syncNeoDurableDirectory(filepath.Dir(rt.schedulePath))
	}
	if syncErr != nil {
		rt.scheduleDirectorySyncPending = true
		rt.schedulePersistRetryAt = time.Now().UTC().Add(neoLocalScheduleRetryInterval)
		rt.wakeLocalScheduleLoop()
		return &neoLocalScheduleDurabilityError{err: syncErr}
	} else {
		rt.scheduleDirectorySyncPending = false
	}
	return nil
}

func writeNeoDurableAtomicFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	closed := false
	removeTemp := true
	defer func() {
		if !closed {
			if closeErr := temp.Close(); closeErr != nil {
				log.Errorf("amp neo durable file close failed: %v", closeErr)
			}
		}
		if removeTemp {
			if removeErr := os.Remove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				log.Errorf("amp neo durable file cleanup failed: %v", removeErr)
			}
		}
	}()
	if _, err := temp.Write(data); err != nil {
		return err
	}
	if err := temp.Chmod(perm); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	removeTemp = false
	return nil
}

func syncNeoDurablePath(path string, syncFile bool) error {
	if syncFile {
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			if closeErr := file.Close(); closeErr != nil {
				return errors.Join(err, closeErr)
			}
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	return syncNeoDurableDirectory(filepath.Dir(path))
}

func syncNeoDurableDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		if closeErr := directory.Close(); closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	}
	return directory.Close()
}

func (rt *neoRuntime) wakeLocalScheduleLoop() {
	if rt == nil || rt.scheduleWake == nil {
		return
	}
	select {
	case rt.scheduleWake <- struct{}{}:
	default:
	}
}

func (rt *neoRuntime) localScheduleLoop(ctx context.Context) {
	for {
		next, ok := rt.nextLocalScheduleRun()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-rt.scheduleWake:
				continue
			}
		}
		delay := time.Until(next)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			neoStopLocalScheduleTimer(timer)
			return
		case <-rt.scheduleWake:
			neoStopLocalScheduleTimer(timer)
		case <-timer.C:
			rt.fireDueLocalSchedules(time.Now().UTC())
		}
	}
}

func neoStopLocalScheduleTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func (rt *neoRuntime) nextLocalScheduleRun() (time.Time, bool) {
	if rt == nil {
		return time.Time{}, false
	}
	rt.scheduleMu.Lock()
	defer rt.scheduleMu.Unlock()
	if rt.scheduleStoreErr != nil {
		return time.Time{}, false
	}
	var earliest time.Time
	if rt.scheduleDirectorySyncPending {
		earliest = rt.schedulePersistRetryAt
		if earliest.IsZero() {
			earliest = time.Now().UTC()
		}
	}
	for _, schedule := range rt.schedules {
		if schedule.Cancellation != nil {
			now := time.Now().UTC()
			if earliest.IsZero() || now.Before(earliest) {
				earliest = now
			}
			continue
		}
		if schedule.Pending != nil {
			earliest = time.Now().UTC()
			break
		}
		if !schedule.Enabled {
			continue
		}
		next, err := time.Parse(time.RFC3339Nano, schedule.NextRunAt)
		if err != nil {
			continue
		}
		if earliest.IsZero() || next.Before(earliest) {
			earliest = next
		}
	}
	if !earliest.IsZero() && !rt.schedulePersistRetryAt.IsZero() && !earliest.After(rt.schedulePersistRetryAt) {
		earliest = rt.schedulePersistRetryAt
	}
	return earliest, !earliest.IsZero()
}

func (rt *neoRuntime) fireDueLocalSchedules(now time.Time) {
	if rt == nil {
		return
	}
	now = now.UTC()
	if !rt.retryLocalScheduleDirectorySync(now) {
		return
	}
	rt.recoverLocalScheduleCancellations()
	due := make([]neoLocalSchedule, 0)
	previous := map[string]neoLocalSchedule{}
	rt.scheduleMu.Lock()
	if rt.scheduleStoreErr != nil {
		rt.scheduleMu.Unlock()
		return
	}
	for threadID, schedule := range rt.schedules {
		if schedule.Cancellation != nil {
			continue
		}
		if schedule.Pending != nil {
			due = append(due, schedule)
			continue
		}
		if !schedule.Enabled {
			continue
		}
		nextRunAt, err := time.Parse(time.RFC3339Nano, schedule.NextRunAt)
		if err != nil || nextRunAt.After(now) {
			continue
		}
		previous[threadID] = schedule
		scheduledAt := nextRunAt.UTC().Format(time.RFC3339Nano)
		schedule.Pending = &neoLocalSchedulePending{
			ID:          "SO-" + randomBase62(22),
			MessageID:   newNeoMessageID(),
			ScheduledAt: scheduledAt,
			CreatedAt:   now.Format(time.RFC3339Nano),
			Prompt:      schedule.Prompt,
		}
		schedule.LastRunAt = scheduledAt
		schedule.LastRunError = ""
		schedule.UpdatedAt = now.Format(time.RFC3339Nano)
		next, nextErr := neoLocalScheduleNext(schedule, now)
		if nextErr != nil {
			schedule.Enabled = false
			schedule.DisabledReason = "invalid_schedule"
			schedule.NextRunAt = ""
			schedule.LastRunError = nextErr.Error()
		} else if next.IsZero() {
			schedule.Enabled = false
			schedule.DisabledReason = "schedule_exhausted"
			schedule.NextRunAt = ""
		} else {
			schedule.NextRunAt = next.Format(time.RFC3339Nano)
		}
		rt.schedules[threadID] = schedule
		due = append(due, schedule)
	}
	if len(previous) > 0 {
		if err := rt.persistLocalSchedulesLocked(); err != nil {
			if !neoLocalSchedulePersistencePublished(err) {
				for threadID, schedule := range previous {
					rt.schedules[threadID] = schedule
				}
			}
			rt.schedulePersistRetryAt = time.Now().UTC().Add(neoLocalScheduleRetryInterval)
			log.Warnf("amp neo local runtime schedule store update failed: %v", err)
			rt.scheduleMu.Unlock()
			return
		}
		if !rt.scheduleDirectorySyncPending {
			rt.schedulePersistRetryAt = time.Time{}
		}
		if rt.scheduleDirectorySyncPending {
			rt.scheduleMu.Unlock()
			return
		}
	}
	rt.scheduleMu.Unlock()
	for _, schedule := range due {
		dispatched, err := rt.dispatchCurrentLocalSchedule(schedule)
		if dispatched && err != nil {
			log.Warnf("amp neo local runtime schedule dispatch failed thread=%s: %v", schedule.TargetThreadID, err)
		}
	}
}

func (rt *neoRuntime) retryLocalScheduleDirectorySync(now time.Time) bool {
	rt.scheduleMu.Lock()
	if !rt.scheduleDirectorySyncPending {
		rt.scheduleMu.Unlock()
		return true
	}
	if !rt.schedulePersistRetryAt.IsZero() && now.Before(rt.schedulePersistRetryAt) {
		rt.scheduleMu.Unlock()
		return false
	}
	var err error
	if rt.syncScheduleStoreDir != nil {
		err = rt.syncScheduleStoreDir(filepath.Dir(rt.schedulePath))
	} else {
		err = syncNeoDurableDirectory(filepath.Dir(rt.schedulePath))
	}
	if err != nil {
		rt.schedulePersistRetryAt = now.Add(neoLocalScheduleRetryInterval)
		rt.scheduleMu.Unlock()
		log.Warnf("amp neo local runtime schedule directory sync retry failed: %v", err)
		return false
	}
	rt.scheduleDirectorySyncPending = false
	rt.schedulePersistRetryAt = time.Time{}
	rt.scheduleMu.Unlock()
	rt.processLocalScheduleQueues()
	return true
}

func (rt *neoRuntime) processLocalScheduleQueues() {
	if rt == nil || rt.store == nil {
		return
	}
	rt.store.mu.RLock()
	actors := make([]*neoActor, 0, len(rt.store.actors))
	for _, actor := range rt.store.actors {
		actors = append(actors, actor)
	}
	rt.store.mu.RUnlock()
	for _, actor := range actors {
		actor.processQueue()
	}
}

func (rt *neoRuntime) dispatchCurrentLocalSchedule(schedule neoLocalSchedule) (bool, error) {
	if rt == nil {
		return false, nil
	}
	if rt.store != nil {
		rt.store.ensureThreadActor(schedule.TargetThreadID)
	}
	guard := rt.localScheduleGuard(schedule.TargetThreadID)
	guard.Lock()
	defer guard.Unlock()
	rt.scheduleMu.Lock()
	current, ok := rt.schedules[schedule.TargetThreadID]
	currentOccurrence := ok &&
		current.ID == schedule.ID &&
		current.Pending != nil &&
		schedule.Pending != nil &&
		current.Pending.ID == schedule.Pending.ID
	_, deleting := rt.scheduleDeletions[schedule.TargetThreadID]
	rt.scheduleMu.Unlock()
	if !currentOccurrence || deleting {
		return false, nil
	}
	actor, dispatchErr := rt.dispatchLocalSchedule(current)
	rt.scheduleMu.Lock()
	current, ok = rt.schedules[schedule.TargetThreadID]
	currentOccurrence = ok && current.ID == schedule.ID && current.Pending != nil && current.Pending.ID == schedule.Pending.ID
	if !currentOccurrence {
		rt.scheduleMu.Unlock()
		return false, nil
	}
	if dispatchErr != nil {
		current.LastRunError = dispatchErr.Error()
		current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		rt.schedules[current.TargetThreadID] = current
		persistErr := rt.persistLocalSchedulesLocked()
		rt.schedulePersistRetryAt = time.Now().UTC().Add(neoLocalScheduleRetryInterval)
		rt.scheduleMu.Unlock()
		rt.wakeLocalScheduleLoop()
		if persistErr != nil {
			return true, errors.Join(dispatchErr, fmt.Errorf("persist schedule dispatch error: %w", persistErr))
		}
		return true, dispatchErr
	}
	previous := current
	current.Pending = nil
	current.LastRunError = ""
	current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	rt.schedules[current.TargetThreadID] = current
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		if !neoLocalSchedulePersistencePublished(err) {
			rt.schedules[current.TargetThreadID] = previous
		}
		rt.schedulePersistRetryAt = time.Now().UTC().Add(neoLocalScheduleRetryInterval)
		rt.scheduleMu.Unlock()
		rt.wakeLocalScheduleLoop()
		return true, fmt.Errorf("persist schedule delivery: %w", err)
	}
	if !rt.hasPendingLocalScheduleErrorLocked() {
		if !rt.scheduleDirectorySyncPending {
			rt.schedulePersistRetryAt = time.Time{}
		}
	}
	rt.scheduleMu.Unlock()
	rt.wakeLocalScheduleLoop()
	actor.processQueueWithScheduleGuardHeld()
	return true, nil
}

func (rt *neoRuntime) dispatchLocalSchedule(schedule neoLocalSchedule) (*neoActor, error) {
	if rt == nil || rt.store == nil {
		return nil, errors.New("local schedule actor store is unavailable")
	}
	if !neoThreadIDExactPattern.MatchString(schedule.TargetThreadID) {
		return nil, errors.New("scheduled thread ID is invalid")
	}
	if schedule.Pending == nil {
		return nil, errors.New("scheduled occurrence is unavailable")
	}
	actor := rt.store.ensureThreadActor(schedule.TargetThreadID)
	if actor == nil || !actor.hasLocalThreadBootstrapState() {
		return nil, errors.New("scheduled thread is no longer available locally")
	}
	if actor.threadToolOwnerID() != schedule.OwnerUserID {
		return nil, errors.New("scheduled thread owner does not match")
	}
	if err := actor.persistScheduledUserMessage(neoQueuedMessage{
		MessageID: schedule.Pending.MessageID,
		Content:   []any{map[string]any{"type": "text", "text": schedule.Pending.Prompt}},
		Meta:      map[string]any{"automationId": schedule.ID, "automationOccurrenceId": schedule.Pending.ID, "automationProvider": "schedule", "scheduledAt": schedule.Pending.ScheduledAt},
		CreatedAt: schedule.Pending.CreatedAt,
	}); err != nil {
		return nil, err
	}
	return actor, nil
}

func (a *neoActor) persistScheduledUserMessage(user neoQueuedMessage) error {
	if a == nil {
		return errors.New("scheduled thread actor is unavailable")
	}
	a.mu.Lock()
	user.AgentMode = a.agentModeLocked()
	user.ReasoningEffort = a.reasoningEffortForModeLocked(user.AgentMode)
	if user.AgentMode == "" {
		a.mu.Unlock()
		return errors.New("scheduled thread has no agent mode")
	}
	alreadyPresent := a.messageIndexLocked(user.MessageID) >= 0
	queued := false
	for _, item := range a.queue {
		if item.MessageID == user.MessageID {
			alreadyPresent = true
			queued = true
			break
		}
	}
	seq := 0
	if !alreadyPresent {
		if len(a.queue) >= neoMaxQueuedMessages {
			a.mu.Unlock()
			return errors.New("scheduled thread message queue is full")
		}
		a.touchLocked()
		a.draft = nil
		a.queue = append(a.queue, user)
		a.executorIdleGeneration++
		seq = a.nextSeqLocked()
		queued = true
	}
	a.mu.Unlock()
	if seq > 0 {
		a.broadcast(map[string]any{"type": "queued_message_added", "message": user.queueProtocol(), "seq": seq})
	}
	if !a.syncLocalThreadSnapshotForShutdownNow() {
		return errors.New("persist scheduled message to thread")
	}
	if err := a.syncScheduledThreadPath(); err != nil {
		return fmt.Errorf("sync scheduled message to thread: %w", err)
	}
	if seq > 0 || queued {
		a.syncCloudAsync()
	}
	return nil
}

func (rt *neoRuntime) prepareLocalScheduleCancellationLocked(schedule neoLocalSchedule) (neoLocalSchedule, bool, error) {
	if rt == nil {
		return schedule, false, nil
	}
	if rt.scheduleCancelling == nil {
		rt.scheduleCancelling = map[string]string{}
	}
	rt.scheduleCancelling[schedule.TargetThreadID] = schedule.ID
	rt.scheduleMu.Unlock()
	messages, err := rt.localScheduleQueuedMessages(schedule)
	rt.scheduleMu.Lock()
	if err != nil || len(messages) == 0 {
		delete(rt.scheduleCancelling, schedule.TargetThreadID)
		return schedule, false, err
	}
	previous := schedule
	schedule.Cancellation = &neoLocalScheduleCancellation{Messages: messages}
	rt.schedules[schedule.TargetThreadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		if neoLocalSchedulePersistencePublished(err) {
			delete(rt.scheduleCancelling, schedule.TargetThreadID)
			return schedule, true, fmt.Errorf("persist schedule cancellation intent: %w", err)
		}
		rt.schedules[schedule.TargetThreadID] = previous
		delete(rt.scheduleCancelling, schedule.TargetThreadID)
		return previous, false, fmt.Errorf("persist schedule cancellation intent: %w", err)
	}
	if rt.scheduleDirectorySyncPending {
		delete(rt.scheduleCancelling, schedule.TargetThreadID)
		return schedule, true, errors.New("schedule cancellation intent durability is pending")
	}
	rt.scheduleMu.Unlock()
	err = rt.removeLocalScheduleQueuedMessages(schedule)
	rt.scheduleMu.Lock()
	delete(rt.scheduleCancelling, schedule.TargetThreadID)
	if err != nil {
		rt.schedulePersistRetryAt = time.Now().UTC().Add(neoLocalScheduleRetryInterval)
		rt.wakeLocalScheduleLoop()
		return schedule, true, err
	}
	return schedule, true, nil
}

func (rt *neoRuntime) localScheduleQueuedMessages(schedule neoLocalSchedule) ([]neoLocalSchedulePending, error) {
	if rt == nil || rt.store == nil {
		return nil, nil
	}
	actor := rt.store.ensureThreadActor(schedule.TargetThreadID)
	if actor == nil || !actor.hasLocalThreadBootstrapState() {
		return nil, nil
	}
	return actor.scheduledQueuedMessages(schedule.ID)
}

func (a *neoActor) scheduledQueuedMessages(scheduleID string) ([]neoLocalSchedulePending, error) {
	if a == nil || strings.TrimSpace(scheduleID) == "" {
		return nil, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	messages := make([]neoLocalSchedulePending, 0, 1)
	for _, item := range a.queue {
		if stringValue(item.Meta["automationProvider"]) != "schedule" || stringValue(item.Meta["automationId"]) != scheduleID {
			continue
		}
		pending := neoLocalSchedulePending{
			ID:          stringValue(item.Meta["automationOccurrenceId"]),
			MessageID:   item.MessageID,
			ScheduledAt: stringValue(item.Meta["scheduledAt"]),
			CreatedAt:   item.CreatedAt,
		}
		if len(item.Content) > 0 {
			pending.Prompt = stringValue(mapValue(item.Content[0])["text"])
		}
		if err := neoValidateLocalSchedulePending(pending); err != nil {
			return nil, err
		}
		messages = append(messages, pending)
	}
	return messages, nil
}

func (rt *neoRuntime) removeLocalScheduleQueuedMessages(schedule neoLocalSchedule) error {
	if rt == nil || rt.store == nil {
		return nil
	}
	actor := rt.store.ensureThreadActor(schedule.TargetThreadID)
	if actor == nil || !actor.hasLocalThreadBootstrapState() {
		return nil
	}
	return actor.removeScheduledQueuedMessagesDurably(schedule.ID)
}

func (a *neoActor) removeScheduledQueuedMessagesDurably(scheduleID string) error {
	if a == nil || strings.TrimSpace(scheduleID) == "" {
		return nil
	}
	a.mu.Lock()
	removed := make([]neoQueuedMessage, 0, 1)
	filtered := a.queue[:0]
	for _, item := range a.queue {
		if stringValue(item.Meta["automationProvider"]) == "schedule" && stringValue(item.Meta["automationId"]) == scheduleID {
			removed = append(removed, item)
			continue
		}
		filtered = append(filtered, item)
	}
	if len(removed) == 0 {
		a.mu.Unlock()
		return nil
	}
	for index := len(filtered); index < len(a.queue); index++ {
		a.queue[index] = neoQueuedMessage{}
	}
	a.queue = filtered
	a.touchLocked()
	a.executorIdleGeneration++
	seqs := make([]int, len(removed))
	for index := range seqs {
		seqs[index] = a.nextSeqLocked()
	}
	a.mu.Unlock()
	if !a.syncLocalThreadSnapshotForShutdownNow() {
		return errors.New("persist scheduled message cancellation")
	}
	if err := a.syncScheduledThreadPath(); err != nil {
		return fmt.Errorf("sync scheduled message cancellation: %w", err)
	}
	for index, item := range removed {
		a.broadcast(map[string]any{"type": "queued_message_removed", "queuedMessageId": item.eventMessageID(), "seq": seqs[index]})
	}
	a.syncCloudAsync()
	return nil
}

func (rt *neoRuntime) recoverLocalScheduleCancellations() {
	if rt == nil {
		return
	}
	rt.scheduleMu.Lock()
	threadIDs := make([]string, 0)
	for threadID, schedule := range rt.schedules {
		if schedule.Cancellation != nil {
			threadIDs = append(threadIDs, threadID)
		}
	}
	rt.scheduleMu.Unlock()
	for _, threadID := range threadIDs {
		rt.recoverLocalScheduleCancellation(threadID)
	}
}

func (rt *neoRuntime) recoverLocalScheduleCancellation(threadID string) {
	if rt.store != nil {
		rt.store.ensureThreadActor(threadID)
	}
	guard := rt.localScheduleGuard(threadID)
	guard.Lock()
	defer guard.Unlock()
	rt.scheduleMu.Lock()
	schedule, ok := rt.schedules[threadID]
	if !ok || schedule.Cancellation == nil {
		rt.scheduleMu.Unlock()
		return
	}
	if rt.scheduleCancelling == nil {
		rt.scheduleCancelling = map[string]string{}
	}
	rt.scheduleCancelling[threadID] = schedule.ID
	rt.scheduleMu.Unlock()
	err := rt.restoreLocalScheduleCancellationMessages(schedule)
	rt.scheduleMu.Lock()
	delete(rt.scheduleCancelling, threadID)
	if err != nil {
		rt.schedulePersistRetryAt = time.Now().UTC().Add(neoLocalScheduleRetryInterval)
		rt.scheduleMu.Unlock()
		rt.wakeLocalScheduleLoop()
		log.Warnf("amp neo local runtime schedule cancellation recovery failed thread=%s: %v", threadID, err)
		return
	}
	previous := schedule
	schedule.Cancellation = nil
	rt.schedules[threadID] = schedule
	if err := rt.persistLocalSchedulesLocked(); err != nil {
		if !neoLocalSchedulePersistencePublished(err) {
			rt.schedules[threadID] = previous
		}
		rt.schedulePersistRetryAt = time.Now().UTC().Add(neoLocalScheduleRetryInterval)
		rt.scheduleMu.Unlock()
		rt.wakeLocalScheduleLoop()
		return
	}
	rt.scheduleMu.Unlock()
	if actor := rt.store.lookupThreadActor(threadID); actor != nil {
		actor.processQueueWithScheduleGuardHeld()
	}
}

func (rt *neoRuntime) restoreLocalScheduleCancellationMessages(schedule neoLocalSchedule) error {
	if rt == nil || rt.store == nil || schedule.Cancellation == nil {
		return nil
	}
	actor := rt.store.ensureThreadActor(schedule.TargetThreadID)
	if actor == nil || !actor.hasLocalThreadBootstrapState() {
		return errors.New("scheduled thread is no longer available locally")
	}
	actor.mu.Lock()
	queuedMessageIDs := make(map[string]bool, len(actor.queue))
	for _, item := range actor.queue {
		queuedMessageIDs[item.MessageID] = true
	}
	pendingMessages := make([]neoLocalSchedulePending, 0, len(schedule.Cancellation.Messages))
	for _, pending := range schedule.Cancellation.Messages {
		if actor.messageIndexLocked(pending.MessageID) >= 0 || queuedMessageIDs[pending.MessageID] {
			continue
		}
		pendingMessages = append(pendingMessages, pending)
		queuedMessageIDs[pending.MessageID] = true
	}
	if len(actor.queue)+len(pendingMessages) > neoMaxQueuedMessages {
		actor.mu.Unlock()
		return errors.New("scheduled thread message queue is full")
	}
	added := make([]neoQueuedMessage, 0, len(pendingMessages))
	seqs := make([]int, 0, len(pendingMessages))
	for _, pending := range pendingMessages {
		item := neoQueuedMessage{
			MessageID:       pending.MessageID,
			Content:         []any{map[string]any{"type": "text", "text": pending.Prompt}},
			Meta:            map[string]any{"automationId": schedule.ID, "automationOccurrenceId": pending.ID, "automationProvider": "schedule", "scheduledAt": pending.ScheduledAt},
			CreatedAt:       pending.CreatedAt,
			AgentMode:       actor.agentModeLocked(),
			ReasoningEffort: actor.reasoningEffortForModeLocked(actor.agentModeLocked()),
		}
		actor.queue = append(actor.queue, item)
		added = append(added, item)
		seqs = append(seqs, actor.nextSeqLocked())
	}
	if len(added) > 0 {
		actor.touchLocked()
		actor.executorIdleGeneration++
	}
	actor.mu.Unlock()
	if len(added) > 0 && !actor.syncLocalThreadSnapshotForShutdownNow() {
		return errors.New("persist schedule cancellation recovery")
	}
	if err := actor.syncScheduledThreadPath(); err != nil {
		return fmt.Errorf("sync schedule cancellation recovery: %w", err)
	}
	for index, item := range added {
		actor.broadcast(map[string]any{"type": "queued_message_added", "message": item.queueProtocol(), "seq": seqs[index]})
	}
	actor.syncCloudAsync()
	return nil
}

func (a *neoActor) syncScheduledThreadPath() error {
	if a == nil || a.runtime == nil {
		return errors.New("scheduled thread runtime is unavailable")
	}
	path := filepath.Join(a.threadStoreDir(), a.threadID+".json")
	if a.runtime.syncScheduledThreadPath != nil {
		return a.runtime.syncScheduledThreadPath(path, true)
	}
	return syncNeoDurablePath(path, true)
}

func (a *neoActor) scheduledQueuedMessageBlockedLocked(item neoQueuedMessage) bool {
	if a == nil || a.runtime == nil || stringValue(item.Meta["automationProvider"]) != "schedule" {
		return false
	}
	scheduleID := stringValue(item.Meta["automationId"])
	occurrenceID := stringValue(item.Meta["automationOccurrenceId"])
	if scheduleID == "" || occurrenceID == "" {
		return false
	}
	a.runtime.scheduleMu.Lock()
	defer a.runtime.scheduleMu.Unlock()
	if a.runtime.scheduleStoreErr != nil {
		return true
	}
	if a.runtime.scheduleDirectorySyncPending {
		return true
	}
	if a.runtime.scheduleCancelling[a.threadID] == scheduleID {
		return true
	}
	if _, deleting := a.runtime.scheduleDeletions[a.threadID]; deleting {
		return true
	}
	schedule, ok := a.runtime.schedules[a.threadID]
	if !ok || schedule.ID != scheduleID {
		return true
	}
	return schedule.Cancellation != nil || schedule.Pending != nil && schedule.Pending.ID == occurrenceID
}

func (a *neoActor) committedScheduledQueuePrunableLocked(localThreadSnapshotsEnabled bool) bool {
	if a == nil || a.runtime == nil || !localThreadSnapshotsEnabled || len(a.queue) == 0 {
		return false
	}
	if a.executorSocket != nil || a.replacingExecutorID != "" || a.resumeExecutorID != "" || a.recoveredExecutorPID != 0 {
		return false
	}
	scheduleID := ""
	messageIDs := make(map[string]struct{}, len(a.queue))
	occurrenceIDs := make(map[string]struct{}, len(a.queue))
	for _, item := range a.queue {
		itemScheduleID := strings.TrimSpace(stringValue(item.Meta["automationId"]))
		occurrenceID := strings.TrimSpace(stringValue(item.Meta["automationOccurrenceId"]))
		if stringValue(item.Meta["automationProvider"]) != "schedule" || itemScheduleID == "" || occurrenceID == "" || strings.TrimSpace(item.MessageID) == "" || strings.TrimSpace(item.AgentMode) == "" || item.Steer || item.ClientAPIKey != "" {
			return false
		}
		prompt := ""
		if len(item.Content) > 0 {
			prompt = stringValue(mapValue(item.Content[0])["text"])
		}
		if err := neoValidateLocalSchedulePending(neoLocalSchedulePending{ID: occurrenceID, MessageID: item.MessageID, ScheduledAt: stringValue(item.Meta["scheduledAt"]), CreatedAt: item.CreatedAt, Prompt: prompt}); err != nil {
			return false
		}
		if _, exists := messageIDs[item.MessageID]; exists {
			return false
		}
		if _, exists := occurrenceIDs[occurrenceID]; exists {
			return false
		}
		messageIDs[item.MessageID] = struct{}{}
		occurrenceIDs[occurrenceID] = struct{}{}
		if scheduleID == "" {
			scheduleID = itemScheduleID
		} else if scheduleID != itemScheduleID {
			return false
		}
	}
	a.runtime.scheduleMu.Lock()
	defer a.runtime.scheduleMu.Unlock()
	if a.runtime.scheduleStoreErr != nil || a.runtime.scheduleDirectorySyncPending || a.runtime.scheduleCancelling[a.threadID] == scheduleID {
		return false
	}
	if _, deleting := a.runtime.scheduleDeletions[a.threadID]; deleting {
		return false
	}
	schedule, ok := a.runtime.schedules[a.threadID]
	return ok && schedule.ID == scheduleID && schedule.Pending == nil && schedule.Cancellation == nil
}

func (rt *neoRuntime) hasPendingLocalScheduleErrorLocked() bool {
	for _, schedule := range rt.schedules {
		if schedule.Pending != nil && schedule.LastRunError != "" {
			return true
		}
	}
	return false
}

func neoRequiredScheduleText(input map[string]any, key string, maxLength int) (string, error) {
	value, ok := input[key].(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	if utf8.RuneCountInString(value) > maxLength {
		return "", fmt.Errorf("%s exceeds %d characters", key, maxLength)
	}
	return value, nil
}

func neoLocalScheduleLabel(input map[string]any) (*string, error) {
	raw, exists := input["scheduleLabel"]
	if !exists || raw == nil {
		return nil, nil
	}
	value, ok := raw.(string)
	if !ok {
		return nil, errors.New("scheduleLabel must be a string or null")
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	if utf8.RuneCountInString(value) > neoLocalScheduleLabelMax {
		return nil, fmt.Errorf("scheduleLabel exceeds %d characters", neoLocalScheduleLabelMax)
	}
	return &value, nil
}

func neoParseLocalSchedule(rawRule, timeZone string, anchor, after time.Time) (string, string, time.Time, error) {
	rule, hasDTStart, err := neoCanonicalLocalScheduleRule(rawRule)
	if err != nil {
		return "", "", time.Time{}, err
	}
	location, locationName, err := neoLocalScheduleLocation(timeZone)
	if err != nil {
		return "", "", time.Time{}, err
	}
	parseRule := rule
	if !hasDTStart {
		localAnchor := anchor.In(location).Truncate(time.Second)
		dtStart := "DTSTART:" + localAnchor.Format("20060102T150405")
		if locationName == "UTC" {
			dtStart += "Z"
		} else {
			dtStart = "DTSTART;TZID=" + locationName + ":" + localAnchor.Format("20060102T150405")
		}
		parseRule = dtStart + "\n" + rule
	}
	set, err := rrule.StrSliceToRRuleSetInLoc(strings.Split(parseRule, "\n"), location)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("invalid RFC 5545 schedule: %w", err)
	}
	if hasDTStart {
		locationName = set.GetDTStart().Location().String()
		if locationName == "" || strings.EqualFold(locationName, "UTC") {
			locationName = "UTC"
		}
	}
	next := set.After(after, false)
	return rule, locationName, next.UTC(), nil
}

func neoCanonicalLocalScheduleRule(raw string) (string, bool, error) {
	raw = strings.TrimSpace(strings.ReplaceAll(raw, "\r\n", "\n"))
	if raw == "" {
		return "", false, errors.New("schedule is required")
	}
	lines := make([]string, 0, 2)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) == 1 && strings.HasPrefix(strings.ToUpper(lines[0]), "FREQ=") {
		lines[0] = "RRULE:" + lines[0]
	}
	if len(lines) == 0 || len(lines) > 2 {
		return "", false, errors.New("schedule must contain one RRULE and an optional DTSTART")
	}
	hasDTStart := false
	rruleCount := 0
	for index, line := range lines {
		upper := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(upper, "DTSTART"):
			if index != 0 || hasDTStart {
				return "", false, errors.New("DTSTART must appear once before RRULE")
			}
			hasDTStart = true
			colon := strings.IndexByte(line, ':')
			if colon < 0 {
				return "", false, errors.New("DTSTART must contain a value after ':'")
			}
			header := strings.Split(line[:colon], ";")
			header[0] = "DTSTART"
			for parameterIndex := 1; parameterIndex < len(header); parameterIndex++ {
				name, value, found := strings.Cut(header[parameterIndex], "=")
				if found {
					header[parameterIndex] = strings.ToUpper(name) + "=" + value
				}
			}
			lines[index] = strings.Join(header, ";") + ":" + strings.ToUpper(line[colon+1:])
		case strings.HasPrefix(upper, "RRULE:"):
			rruleCount++
			if strings.Contains(upper, "FREQ=SECONDLY") {
				return "", false, errors.New("FREQ=SECONDLY is not supported")
			}
			lines[index] = upper
		default:
			return "", false, errors.New("schedule supports only DTSTART and RRULE")
		}
	}
	if rruleCount != 1 {
		return "", false, errors.New("schedule must contain exactly one RRULE")
	}
	return strings.Join(lines, "\n"), hasDTStart, nil
}

func neoLocalScheduleLocation(timeZone string) (*time.Location, string, error) {
	timeZone = strings.TrimSpace(timeZone)
	if timeZone == "" {
		timeZone = strings.TrimSpace(neoAmpTimezone())
	}
	if timeZone == "" || strings.EqualFold(timeZone, "UTC") {
		return time.UTC, "UTC", nil
	}
	location, err := time.LoadLocation(timeZone)
	if err != nil {
		return nil, "", fmt.Errorf("invalid IANA time zone %q", timeZone)
	}
	return location, location.String(), nil
}

func neoLocalScheduleNext(schedule neoLocalSchedule, after time.Time) (time.Time, error) {
	anchor, err := time.Parse(time.RFC3339Nano, schedule.AnchorAt)
	if err != nil {
		return time.Time{}, errors.New("schedule anchor is invalid")
	}
	_, _, next, err := neoParseLocalSchedule(schedule.Trigger.Schedule, schedule.AnchorTimeZone, anchor, after)
	return next, err
}

func (schedule neoLocalSchedule) response() map[string]any {
	var scheduleLabel any
	if schedule.Trigger.ScheduleLabel != nil {
		scheduleLabel = *schedule.Trigger.ScheduleLabel
	}
	return map[string]any{
		"id":             schedule.ID,
		"title":          schedule.Title,
		"targetThreadID": schedule.TargetThreadID,
		"trigger": map[string]any{
			"provider":      "schedule",
			"schedule":      schedule.Trigger.Schedule,
			"scheduleLabel": scheduleLabel,
		},
		"prompt":         schedule.Prompt,
		"enabled":        schedule.Enabled,
		"disabledReason": neoNullableString(schedule.DisabledReason),
		"nextRunAt":      neoNullableString(schedule.NextRunAt),
		"lastRunAt":      neoNullableString(schedule.LastRunAt),
		"lastRunError":   neoNullableString(schedule.LastRunError),
		"timeZone":       schedule.AnchorTimeZone,
		"createdAt":      schedule.CreatedAt,
		"updatedAt":      schedule.UpdatedAt,
	}
}

func neoNullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
