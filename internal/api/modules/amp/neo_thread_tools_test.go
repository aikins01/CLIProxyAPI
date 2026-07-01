package amp

import (
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestNeoActorRunsTopLevelThreadToolsLocallyWhenExecutorOmitsThem(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	sourceID := "T-019e1046-656d-7132-879f-390ded941c40"
	targetID := "T-019e1046-656d-7132-879f-390ded941c41"
	source := rt.store.ensureThreadActor(sourceID)
	target := rt.store.ensureThreadActor(targetID)
	source.executorBootstrapComplete = true

	for _, name := range []string{"create_thread", "archive_thread", "archive_threads", "unarchive_thread", "send_message_to_thread"} {
		if !source.shouldRunLocalActorTool(name) {
			t.Fatalf("%s should run locally when executor omitted it", name)
		}
	}
	tools := source.inferenceRequestLocked("agg-man", "", "").Tools
	got := map[string]bool{}
	for _, tool := range tools {
		got[tool.Name] = true
	}
	for _, name := range []string{"create_thread", "archive_thread", "archive_threads", "unarchive_thread", "send_message_to_thread"} {
		if !got[name] {
			t.Fatalf("agg-man tools missing synthetic %s: %#v", name, tools)
		}
	}
	source.tools["archive_thread"] = neoToolSpec{Name: "archive_thread"}
	if source.shouldRunLocalActorTool("archive_thread") {
		t.Fatal("archive_thread should not run locally when executor registered it")
	}
	delete(source.tools, "archive_thread")

	pending := neoPendingTool{ID: "TU-archive", Name: "archive_thread", Input: map[string]any{"targetThreadId": targetID}, AgentMode: "agg-man", MessageID: "M-assistant"}
	source.pendingTools[pending.ID] = pending
	source.agentState = "running_tools"
	source.runLocalActorTool(pending, source.generation)

	source.mu.Lock()
	_, stillPending := source.pendingTools[pending.ID]
	var result *neoMessage
	for i := range source.messages {
		if source.messages[i].MessageID == toolResultMessageID(pending.ID) {
			message := source.messages[i]
			result = &message
		}
	}
	sourceArchived := source.archived
	source.mu.Unlock()
	target.mu.Lock()
	targetArchived := target.archived
	target.mu.Unlock()
	if stillPending || sourceArchived || !targetArchived {
		t.Fatalf("archive state pending=%v source=%v target=%v", stillPending, sourceArchived, targetArchived)
	}
	if result == nil || len(result.Content) == 0 {
		t.Fatalf("missing archive tool result: %#v", result)
	}
	run := mapValue(mapValue(result.Content[0])["run"])
	if stringValue(run["status"]) != "done" || !strings.Contains(runToText(run), targetID) {
		t.Fatalf("archive run = %#v", run)
	}
}

func TestNeoActorRunsSendMessageToThreadLocallyWhenExecutorOmitsIt(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	sourceID := "T-019e1046-656d-7132-879f-390ded941c42"
	targetID := "T-019e1046-656d-7132-879f-390ded941c43"
	source := rt.store.ensureThreadActor(sourceID)
	target := rt.store.ensureThreadActor(targetID)
	source.executorBootstrapComplete = true

	pending := neoPendingTool{ID: "TU-send", Name: "send_message_to_thread", Input: map[string]any{"targetThreadId": targetID, "workflow": "code_review"}, AgentMode: "agg-man", MessageID: "M-assistant"}
	source.pendingTools[pending.ID] = pending
	source.agentState = "running_tools"
	source.runLocalActorTool(pending, source.generation)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		target.mu.Lock()
		if len(target.queue) > 0 {
			queued := target.queue[0]
			target.mu.Unlock()
			if got := textFromBlocks(queued.Content); got != "Review the changes with the code review tool." {
				t.Fatalf("queued content = %q", got)
			}
			if got := stringValue(mapValue(queued.queueProtocol()["queuedMessage"])["parentToolUseId"]); got != pending.ID {
				t.Fatalf("queued protocol parent tool id = %q, want %s", got, pending.ID)
			}
			target.mu.Lock()
			stored, _, _ := target.storeQueuedUserMessageLocked(queued, false)
			target.mu.Unlock()
			if stored.ParentToolUseID != pending.ID {
				t.Fatalf("stored parent tool id = %q, want %s", stored.ParentToolUseID, pending.ID)
			}
			if queued.AgentMode != "review" || queued.ReasoningEffort != "medium" {
				t.Fatalf("queued mode = %q/%q", queued.AgentMode, queued.ReasoningEffort)
			}
			break
		}
		target.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	target.mu.Lock()
	queueLen := len(target.queue)
	target.mu.Unlock()
	if queueLen == 0 {
		t.Fatal("timed out waiting for target thread queue")
	}

	source.mu.Lock()
	_, stillPending := source.pendingTools[pending.ID]
	var result *neoMessage
	for i := range source.messages {
		if source.messages[i].MessageID == toolResultMessageID(pending.ID) {
			message := source.messages[i]
			result = &message
		}
	}
	source.mu.Unlock()
	if stillPending || result == nil || len(result.Content) == 0 {
		t.Fatalf("send tool did not complete: pending=%v result=%#v", stillPending, result)
	}
	run := mapValue(mapValue(result.Content[0])["run"])
	if stringValue(run["status"]) != "done" || !strings.Contains(runToText(run), "code_review") {
		t.Fatalf("send run = %#v", run)
	}
}

func TestNeoThreadToolSendMessageFailsClosedWithoutRuntime(t *testing.T) {
	actor := &neoActor{threadID: "T-019e1046-656d-7132-879f-390ded941c47"}
	_, err := actor.executeLocalSendMessageToThreadTool(neoPendingTool{
		ID:   "TU-send-missing-runtime",
		Name: "send_message_to_thread",
		Input: map[string]any{
			"targetThreadId": "T-019e1046-656d-7132-879f-390ded941c48",
			"message":        "hello",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "missing local runtime") {
		t.Fatalf("error = %v, want missing local runtime", err)
	}
}

func TestNeoThreadToolCreatesThreadAndArchivesMultiple(t *testing.T) {
	useTempNeoThreadStore(t)
	rt := newNeoRuntime(&config.Config{})
	sourceID := "T-019e1046-656d-7132-879f-390ded941c44"
	createdID := "T-019e1046-656d-7132-879f-390ded941c45"
	otherID := "T-019e1046-656d-7132-879f-390ded941c46"
	source := rt.store.ensureThreadActor(sourceID)

	created, err := source.executeLocalCreateThreadTool(map[string]any{"threadId": createdID})
	if err != nil {
		t.Fatalf("create_thread error: %v", err)
	}
	if stringValue(created["threadId"]) != createdID {
		t.Fatalf("created result = %#v", created)
	}
	createdActor := rt.store.ensureThreadActor(createdID)
	otherActor := rt.store.ensureThreadActor(otherID)
	archived, err := source.executeLocalArchiveThreadsTool(map[string]any{"threadIds": []any{createdID, "https://ampcode.com/threads/" + otherID}})
	if err != nil {
		t.Fatalf("archive_threads error: %v", err)
	}
	if len(arrayValue(archived["archived"])) != 2 {
		t.Fatalf("archive_threads result = %#v", archived)
	}
	createdActor.mu.Lock()
	createdArchived := createdActor.archived
	createdActor.mu.Unlock()
	otherActor.mu.Lock()
	otherArchived := otherActor.archived
	otherActor.mu.Unlock()
	if !createdArchived || !otherArchived {
		t.Fatalf("archived flags created=%v other=%v", createdArchived, otherArchived)
	}
}
