package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestClassifyStringsExtractsParitySignals(t *testing.T) {
	strs := []string{
		`await ja("/api/thread-actors/"+threadId,{method:"POST"}); Ph(R,"GET","/metadata"); api.post("/threads/T-12345678-1234-1234-1234-123456789abc")`,
		`fetch("/actors/metadata?namespace=default")`,
		`T.command("apps");T.command("secrets");T.command("report").description("Generate and send a diagnostic report for the amp --no-tui runner");T.option("--remote-control-terminal")`,
		`["user:message","user:message:interrupt","user:message:append-content","user:message-queue:enqueue","user:message-queue:dequeue","user:message-queue:discard","user:tool-input","tool:data","tool:processed","assistant:message","assistant:message-update","thread:truncate","title","agent-mode","reasoning-effort","environment","max-tokens","main-thread"]`,
		`J.discriminatedUnion("type",[J.object({type:J.literal("message_added")}),J.object({type:J.literal("message_updated")}),J.object({type:J.literal("delta")}),J.object({type:J.literal("thread_truncated")}),J.object({type:J.literal("queued_messages")}),J.object({type:J.literal("queued_message_added")}),J.object({type:J.literal("queued_message_removed")}),J.object({type:J.literal("queued_message_dequeued")}),J.object({type:J.literal("tool_progress")}),J.object({type:J.literal("tool_approval_queue")}),J.object({type:J.literal("tool_lease")}),J.object({type:J.literal("thread_settings")}),J.object({type:J.literal("thread_title")}),J.object({type:J.literal("thread_status")}),J.object({type:J.literal("thread_relationships")}),J.object({type:J.literal("agent_state")}),J.object({type:J.literal("cancelled")}),J.object({type:J.literal("compaction_started")}),J.object({type:J.literal("compaction_complete")}),J.object({type:J.literal("compaction_records")}),J.object({type:J.literal("retry_scheduled")}),J.object({type:J.literal("retry_started")}),J.object({type:J.literal("retry_cancelled")}),J.object({type:J.literal("error_set")}),J.object({type:J.literal("error_cleared")}),J.object({type:J.literal("error")}),J.object({type:J.literal("edit_rejected")}),J.object({type:J.literal("environment_update")}),J.object({type:J.literal("observers")}),J.object({type:J.literal("plugin_message")}),J.object({type:J.literal("client_append_user_msg")}),J.object({type:J.literal("executor_tool_result")}),J.object({type:J.literal("executor_tools_register")})])`,
		`type:J.literal("client_terminal_open") type:J.literal("executor_terminal_output")`,
		`case "user:interrupted": case "system:non-terminal-tool-result": case "system:this": case "user:this":`,
		`switch(status){case"done":case"error":case"rejected-by-user":case"cancelled":case"in-progress":case"cancellation-requested":case"queued":case"blocked-on-user":} --stream-json --stream-json-thinking agent_mode reasoning_effort mcp_servers session_id duration_ms num_turns error_during_execution is_error`,
		`tools.disable:{value:["browser_navigate","builtin:edit_file"]}; SFT=new Set(["read_file","ripgrep","Read","Grep","glob","Glob","file_tree","view_media","gmail_read","gmail_write"]); MFT={enableToolSpecs:disableTools}; mcpServers mcp__server__tool skills.disableClaudeCodeSkills skills.path toolbox.path applyPatchFreeform browser_take_screenshot`,
		`draftThreadSettings reasoning.effort internal.model agentMode reasoningEffort lastReasoningEffortByMode lastSpeedByMode explicitEffort openai.speed anthropic.speed gemini.thinkingLevel`,
		`anthropic-beta anthropic-version 2023-06-01 interleaved-thinking-2025-05-14 fast-mode-2026-02-01 x-amp-feature x-amp-thread-id x-amp-message-id x-amp-override-provider X-Amp-Client-Application amp.chat amp.review openai-websocket google-upload-url`,
		`Vw="x-amp-feature",QRT="x-amp-thread-id",LU="x-amp-message-id",ART="X-Amp-Client-Application",RTT="X-Amp-Client-Type",TTT="X-Amp-Client-Version"; VpT="fast-mode-2026-02-01"`,
		`function ZpT(R,T,e,i,r){let a=[];if((R["anthropic.thinking.enabled"]??!0)&&R["anthropic.interleavedThinking.enabled"]&&!FOR(e))a.push("interleaved-thinking-2025-05-14");let c;if(R["anthropic.provider"])c=R["anthropic.provider"];if(XpT(e,R["anthropic.speed"])==="fast")a.push(VpT),c="anthropic";return{...OU(Hn()),...a.length>0?{"anthropic-beta":a.join(",")}:{},...c?{"x-amp-override-provider":c}:{},[Vw]:"amp.chat",...i!=null?{[LU]:String(i)}:{},...pU(T)}}`,
		`function jE(R,T){return new LdR({httpOptions:{headers:{[Vw]:T?.featureHeader??"amp.chat",...T?.messageId!=null?{[LU]:String(T.messageId)}:{},...pU(T?.threadMeta)}}})}`,
		`function VhT(R,T,e){return{...OU(Hn()),[Vw]:"amp.chat",...pU(R),...T!=null?{[LU]:String(T)}:{},...e??{}}}`,
		`let review=await xM(vE,r,[],i?{id:i}:void 0,a,e,{temperature:0.1,systemInstruction:"You are an expert software engineer reviewing code changes.",thinkingConfig:{thinkingLevel:"MINIMAL"}},"amp.review")`,
		`w7T="amp.read-thread"; async function JrR({contents:R,systemPrompt:T,currentThread:e,config:i,signal:r,serviceAuthToken:a,toolsEnabled:c}){return N7T({contents:R,systemPrompt:T,currentThread:e,toolsEnabled:c}),(await xM(ojR,R,c?XjT:[],e,i,r,{systemInstruction:T},w7T,a)).message}`,
		`w7T="amp.read-thread"; async function K7T({contents:R,systemPrompt:T,currentThread:e,config:i,signal:r,serviceAuthToken:a,toolsEnabled:c}){return N7T({contents:R,systemPrompt:T,currentThread:e,toolsEnabled:c}),(await xM(ojR,R,c?XjT:[],e,i,r,{systemInstruction:T,temperature:0.1,thinkingConfig:{includeThoughts:!1,thinkingLevel:w2.MINIMAL}},w7T,a)).message} function N7T(){_R.info("Thread reader full model messages")}`,
		`async function P_T(R,T,e,i,r,a,c){let h=await jE(r,{threadMeta:i,featureHeader:c??"amp.image-generation"});return h.models.generateContent({model:R})}`,
		`async function DhT(R,T,e,i,r,a,c){let h=VhT(i,void 0,{[Vw]:c??"amp.image-generation",accept:"application/json"});return h}`,
		`if(UhT(l))s=await P_T(A9.GEMINI_3_PRO_IMAGE.name,R.prompt,h.length>0?h:void 0,T,e,a,"amp.painter");else s=await DhT(l,R.prompt,h.length>0?h:void 0,T,e,a,"amp.painter")`,
		`function I_T(R,T,e){return{...LU(Hs()),[Vw]:"amp.chat",...PU(R),...T!=null?{[xU]:String(T)}:{},...e??{}}}async function I_imgT(R,T){return await _.images.edit({model:R,prompt:T})}`,
		`let _=await jE(i,{threadMeta:r,featureHeader:c??"amp.image-generation"}),l=await _.models.generateContent({model:h.name,contents:n})`,
		`async function J_T(R,T,e,r,i,a,c){let _=I_T(r,void 0,{[Vw]:c??"amp.image-generation",accept:"application/json"}),n=await h.images.edit({model:R,prompt:T})}`,
		`if(E_T(l))n=await xhT(A9.GEMINI_3_PRO_IMAGE.name,R.prompt,_.length>0?_:void 0,T,e,a,"amp.painter");else n=await J_T(l,R.prompt,_.length>0?_:void 0,T,e,a,"amp.painter");async function E_imgT(R){return await _.images.generate({model:R})}`,
		`jpT=1e5;compactionControl;contextTokenThreshold??jpT;usage.input_tokens;usage.cache_creation_input_tokens;usage.cache_read_input_tokens;usage.output_tokens;_.content.filter((s)=>s.type!=="tool_use");"x-stainless-helper":"compaction";params.messages=[{role:"user",content:c.content}];let{role:c,content:_}=await`,
		`params.compactionControl;contextTokenThreshold??vxT;usage.input_tokens;usage.cache_creation_input_tokens;usage.cache_read_input_tokens;usage.output_tokens;if(a[a.length-1].role==="assistant"){let h=a[a.length-1];if(Array.isArray(h.content)){let _=h.content.filter((n)=>n.type!=="tool_use");if(_.length===0)a.pop();else h.content=_}}headers:{"x-stainless-helper":"compaction"};params.messages=[{role:"user",content:c.content}]`,
		`ei={DEEP:{key:"deep",displayName:"Deep",primaryModel:Nr("GPT_5_5"),includeTools:Ab,deferredTools:AD,visible:!0,visibleInV2:!0,reasoningEffort:"medium",reasoningEffortControl:{levels:["low","medium","xhigh"]}},SMART:{key:"smart",displayName:"Smart",primaryModel:Nr("CLAUDE_OPUS_4_7"),includeTools:D6,deferredTools:Fb,visible:!0,visibleInV2:!0,reasoningEffort:"high",reasoningEffortControl:{levels:["high","xhigh","max"]}},AGG:{key:"agg-man",displayName:"Agg",primaryModel:Nr("GPT_5_5"),includeTools:PD,visible:!1,reasoningEffort:"none",serverOnly:!0},NOSTROMO:{key:BD,displayName:"nostromo",primaryModel:Nr("AMP_NOSTROMO"),includeTools:TD,visible:!0,visibleInV2:!0,reasoningEffort:"low"}},Fc=ei.AGG.key`,
		`ya={DEEP:{key:"deep",displayName:"Deep",primaryModel:Vi("GPT_5_5"),includeTools:eH,deferredTools:o9R,visible:!0,visibleInV2:!0,reasoningEffort:"medium",reasoningEffortControl:{levels:["low","medium","xhigh"]}},SMART:{key:"smart",displayName:"Smart",primaryModel:Vi("CLAUDE_OPUS_4_7"),includeTools:hP,deferredTools:TH,visible:!0,visibleInV2:!0,reasoningEffort:"high",reasoningEffortControl:{levels:["high","xhigh","max"]}},RUSH:{key:"rush",displayName:"Rush",primaryModel:Vi("GPT_5_5"),includeTools:s9R,visible:!0,reasoningEffort:"none"},AGG:{key:"agg-man",displayName:"Agg",primaryModel:Vi("GPT_5_5"),includeTools:l9R,visible:!1,reasoningEffort:"none",serverOnly:!0},LARGE:{key:"large",displayName:"Large",primaryModel:Vi("CLAUDE_OPUS_4_6"),includeTools:hP,deferredTools:TH,visible:!0},NOSTROMO:{key:BbR,displayName:"nostromo",primaryModel:Vi("AMP_NOSTROMO"),includeTools:y9R,visible:!0,visibleInV2:!0,reasoningEffort:"low"}}`,
		`A9={AMP_NOSTROMO:{provider:K.AMP,name:"amp-nostromo-v1",displayName:"nostromo",contextWindow:400000,maxOutputTokens:128000},CLAUDE_OPUS_4_6:{provider:K.ANTHROPIC,name:"claude-opus-4-6",displayName:"Claude Opus 4.6",contextWindow:332000,maxOutputTokens:32000},CLAUDE_OPUS_4_7:{provider:K.ANTHROPIC,name:"claude-opus-4-7",displayName:"Claude Opus 4.7",contextWindow:332000,maxOutputTokens:32000},CLAUDE_OPUS_4_8:{provider:K.ANTHROPIC,name:"claude-opus-4-8",displayName:"Claude Opus 4.8",contextWindow:332000,maxOutputTokens:32000},CLAUDE_SONNET_4:{provider:K.ANTHROPIC,name:"claude-sonnet-4-20250514",displayName:"Claude Sonnet 4",contextWindow:1e6,maxOutputTokens:32000},GPT_5_5:{provider:K.OPENAI,name:"gpt-5.5",displayName:"GPT-5.5",contextWindow:400000,maxOutputTokens:128000},BASETEN_KIMI_K2P5:{provider:K.BASENTEN,name:"moonshotai/Kimi-K2.5",displayName:"Kimi K2.5",contextWindow:262144,maxOutputTokens:32000},BASETEN_GLM_5_2:{provider:K.BASENTEN,name:"zai-org/GLM-5.2",displayName:"GLM 5.2",contextWindow:200000,maxOutputTokens:32000}}`,
		`IOR="claude-opus-4-6-1m",JpT=1e6,QpT=32000; ZB=A9.CLAUDE_OPUS_4_6.name; if(UpT(R)||T?.enableLargeContext&&Sf(R)===ZB)return JpT`,
		`Ao0=R8.CLAUDE_OPUS_4_5.name,a$=R8.CLAUDE_OPUS_4_6.name,C7R=R8.CLAUDE_OPUS_4_7.name,M7R=R8.CLAUDE_OPUS_4_8.name; function puT(T){let R=Sx(T);return R===a$||R===C7R||R===M7R} function z7R(T,R){if(puT(T)){let a=["low","medium","high","xhigh","max"].includes(R.reasoningEffort)?R.reasoningEffort:"medium";return{thinking:{type:"adaptive",display:"summarized"},...{output_config:{effort:a}}}}}`,
		`function yLT(R,T,e){let[i,r]=R.includes("/")?R.split("/",2):["",R],a=e?ee(e)?.reasoningEffort:void 0,c=lLT(T,e);switch(i){case"anthropic":return oLT(c)??a??(r===A9.CLAUDE_OPUS_4_7.name?"medium":"high");case"openai":return sLT(c)??a??"medium";case"vertexai":return T["gemini.thinkingLevel"]??a??"medium";default:return a??"medium"}}`,
		`DCT={"anthropic.interleavedThinking.enabled":{value:false},"painter.model":{value:"gpt-image-2"},showCosts:{value:true},constructor:{value:"noise"}}`,
		`model list: gpt-5.5 amp-nostromo-v1 claude-opus-4-8 gemini-3-pro-preview codex-config o0IwQDAPBgNVHRMBAf8EBTADAQH`,
		`threadActorTransport json-rpc threadActor threadStatusUpdated userActor RivetKit rvt-token wsToken local-client ActionRequest ActionResponse SubscriptionRequest serializeWithEmbeddedVersion deserializeWithEmbeddedVersion rivet_encoding.`,
		`Do not include run_check findings in submit_review`,
		`CLI appends structured check findings mechanically`,
		`"instructions": "Outcome-first brief for the check agent (see system guidance)."`,
		`let o=input,k=o&&typeof o==="object"&&"checkURI"in o&&typeof o.checkURI==="string"?o.checkURI:void 0,p=o&&typeof o==="object"&&"checkName"in o&&typeof o.checkName==="string"?o.checkName:void 0`,
		`severity!=="low" The following checks were run`,
		"rR.yellow(\"issues found\"); i.push(`${e.result.check.name}: ${n}`)",
		"Warning: `amp review` is deprecated. Ask Amp to review the changes with the oracle instead: echo 'Ask the oracle to review uncommitted changes' | amp; echo 'Ask the oracle to review the changes since the merge base' | amp",
		"Please perform compaction guidance for the system prompt and code review workflow. This deliberately long prompt-like segment has enough ordinary words to be fingerprinted without storing the body in the baseline.",
	}

	signals := classifyStrings(strs)

	assertContains(t, signals.CLICommandLiterals, "apps")
	assertContains(t, signals.CLICommandLiterals, "report")
	assertContains(t, signals.CLICommandLiterals, "secrets")
	assertContains(t, cliControlSurfaceStrings(signals.CLIControlSurfaces), "apps=amp-owned")
	assertContains(t, cliControlSurfaceStrings(signals.CLIControlSurfaces), "orb-secrets=amp-owned")
	assertContains(t, cliControlSurfaceStrings(signals.CLIControlSurfaces), "remote-control-terminal=local-runtime")
	assertContains(t, cliControlSurfaceStrings(signals.CLIControlSurfaces), "runner-report=amp-owned")
	assertContains(t, signals.Routes, "/api/thread-actors/")
	assertContains(t, signals.Routes, "/actors/metadata?namespace=default")
	assertContains(t, signals.Routes, "/threads/:threadID")
	assertContains(t, routeCoverageStrings(signals.RouteCoverage), "/api/thread-actors/=local-runtime")
	assertContains(t, routeCoverageStrings(signals.RouteCoverage), "/actors/metadata?namespace=default=local-runtime")
	assertContains(t, routeCoverageStrings(signals.RouteCoverage), "/threads/:threadID=amp-owned")
	assertContains(t, routeMethodStrings(signals.RouteMethods), "/api/thread-actors/=POST=local-runtime")
	assertContains(t, routeMethodStrings(signals.RouteMethods), "/metadata=GET=local-runtime")
	assertContains(t, routeMethodStrings(signals.RouteMethods), "/threads/:threadID=POST=amp-owned")
	expectedThreadDeltas := map[string]string{
		"agent-mode":                  "settings",
		"agent_state":                 "execution-state",
		"assistant:message":           "assistant-message",
		"assistant:message-update":    "assistant-message",
		"cancelled":                   "execution-state",
		"client_append_user_msg":      "client-command",
		"client_terminal_open":        "client-command",
		"compaction_complete":         "compaction",
		"compaction_records":          "compaction",
		"compaction_started":          "compaction",
		"delta":                       "assistant-message",
		"edit_rejected":               "error",
		"environment":                 "environment",
		"environment_update":          "environment",
		"error":                       "error",
		"error_cleared":               "error",
		"error_set":                   "error",
		"executor_terminal_output":    "executor-bridge",
		"executor_tool_result":        "executor-bridge",
		"executor_tools_register":     "executor-bridge",
		"main-thread":                 "relationship",
		"max-tokens":                  "settings",
		"message_added":               "message",
		"message_updated":             "message",
		"observers":                   "observer",
		"plugin_message":              "plugin",
		"queued_message_added":        "queue",
		"queued_message_dequeued":     "queue",
		"queued_message_removed":      "queue",
		"queued_messages":             "queue",
		"reasoning-effort":            "settings",
		"retry_cancelled":             "retry",
		"retry_scheduled":             "retry",
		"retry_started":               "retry",
		"thread_relationships":        "relationship",
		"thread_settings":             "settings",
		"thread_status":               "status",
		"thread_title":                "metadata",
		"thread_truncated":            "history",
		"thread:truncate":             "history",
		"title":                       "metadata",
		"tool:data":                   "tool-result",
		"tool_approval_queue":         "tool-input",
		"tool_lease":                  "tool-state",
		"tool:processed":              "tool-result",
		"tool_progress":               "tool-result",
		"user:message":                "user-message",
		"user:message-queue:dequeue":  "queue",
		"user:message-queue:discard":  "queue",
		"user:message-queue:enqueue":  "queue",
		"user:message:append-content": "user-message",
		"user:message:interrupt":      "user-message",
		"user:tool-input":             "tool-input",
	}
	threadDeltaCoverage := threadDeltaCoverageStrings(signals.ThreadDeltaCoverage)
	for delta, area := range expectedThreadDeltas {
		assertContains(t, signals.ThreadDeltaEvents, delta)
		assertContains(t, threadDeltaCoverage, delta+"="+area)
	}
	assertContains(t, signals.ToolCancelReasons, "user:interrupted")
	assertContains(t, signals.ToolCancelReasons, "system:non-terminal-tool-result")
	assertNotContains(t, signals.ToolCancelReasons, "system:this")
	assertNotContains(t, signals.ToolCancelReasons, "user:this")
	assertContains(t, toolCancelCoverageStrings(signals.ToolCancelCoverage), "user:interrupted=user-interrupt")
	assertContains(t, toolCancelCoverageStrings(signals.ToolCancelCoverage), "system:non-terminal-tool-result=restore-cleanup")
	assertContains(t, signals.ToolRunStatuses, "blocked-on-user")
	assertContains(t, signals.ToolRunStatuses, "done")
	assertContains(t, signals.ToolRunStatuses, "rejected-by-user")
	assertContains(t, toolRunCoverageStrings(signals.ToolRunCoverage), "blocked-on-user=pending")
	assertContains(t, toolRunCoverageStrings(signals.ToolRunCoverage), "done=terminal-success")
	assertContains(t, toolRunCoverageStrings(signals.ToolRunCoverage), "rejected-by-user=terminal-error")
	assertContains(t, signals.ToolCatalog, "browser_navigate")
	assertContains(t, signals.ToolCatalog, "browser_take_screenshot")
	assertContains(t, signals.ToolCatalog, "builtin:edit_file")
	assertContains(t, signals.ToolCatalog, "read_file")
	assertContains(t, signals.ToolCatalog, "ripgrep")
	assertContains(t, signals.ToolCatalog, "Glob")
	assertContains(t, signals.ToolCatalog, "mcp__server__tool")
	assertContains(t, signals.ToolCatalog, "gmail_read")
	assertContains(t, signals.ToolCatalog, "gmail_write")
	assertContains(t, toolCatalogCoverageStrings(signals.ToolCatalogCoverage), "browser_navigate=browser")
	assertContains(t, toolCatalogCoverageStrings(signals.ToolCatalogCoverage), "builtin:edit_file=file-edit")
	assertContains(t, toolCatalogCoverageStrings(signals.ToolCatalogCoverage), "read_file=file-read")
	assertContains(t, toolCatalogCoverageStrings(signals.ToolCatalogCoverage), "Glob=legacy-file-search")
	assertContains(t, toolCatalogCoverageStrings(signals.ToolCatalogCoverage), "mcp__server__tool=mcp")
	assertContains(t, toolCatalogCoverageStrings(signals.ToolCatalogCoverage), "gmail_read=email")
	assertContains(t, toolCatalogCoverageStrings(signals.ToolCatalogCoverage), "gmail_write=email")
	assertContains(t, signals.StreamJSONMarkers, "--stream-json")
	assertContains(t, signals.StreamJSONMarkers, "agent_mode")
	assertContains(t, signals.StreamJSONMarkers, "error_during_execution")
	assertContains(t, streamJSONCoverageStrings(signals.StreamJSONCoverage), "--stream-json=cli-flag")
	assertContains(t, streamJSONCoverageStrings(signals.StreamJSONCoverage), "agent_mode=init-field")
	assertContains(t, streamJSONCoverageStrings(signals.StreamJSONCoverage), "error_during_execution=error-subtype")
	assertContains(t, signals.ModeSettingMarkers, "reasoningEffort")
	assertContains(t, signals.ModeSettingMarkers, "draftThreadSettings")
	assertContains(t, signals.ModeSettingMarkers, "lastSpeedByMode")
	assertNotContains(t, signals.ModeSettingMarkers, "reasoning.effort")
	assertContains(t, modeSettingCoverageStrings(signals.ModeSettingCoverage), "draftThreadSettings=draft-settings")
	assertContains(t, modeSettingCoverageStrings(signals.ModeSettingCoverage), "lastSpeedByMode=session-default")
	assertContains(t, signals.ProviderProtocol, "anthropic-beta")
	assertContains(t, signals.ProviderProtocol, "interleaved-thinking-2025-05-14")
	assertContains(t, signals.ProviderProtocol, "x-amp-feature")
	assertContains(t, signals.ProviderProtocol, "amp.chat")
	assertContains(t, providerCoverageStrings(signals.ProviderCoverage), "anthropic-beta=anthropic-header")
	assertContains(t, providerCoverageStrings(signals.ProviderCoverage), "interleaved-thinking-2025-05-14=anthropic-beta")
	assertContains(t, providerCoverageStrings(signals.ProviderCoverage), "x-amp-feature=amp-provider-header")
	assertContains(t, providerCoverageStrings(signals.ProviderCoverage), "x-amp-client-application=amp-client-header")
	assertContains(t, providerCoverageStrings(signals.ProviderCoverage), "amp.chat=amp-feature")
	assertContains(t, providerCoverageStrings(signals.ProviderCoverage), "google-upload-url=google-upload")
	assertContains(t, signals.ReviewContract, "human-review-check-footer-counts-issues")
	assertContains(t, signals.ReviewContract, "human-review-filters-low-severity")
	assertContains(t, signals.ReviewContract, "review-cli-deprecation-warning")
	assertContains(t, signals.ReviewContract, "review-cli-appends-check-findings")
	assertContains(t, signals.ReviewContract, "review-submit-omits-run-check-findings")
	assertContains(t, signals.ReviewContract, "run-check-instructions-input-shape")
	assertContains(t, signals.ReviewContract, "run-check-uri-input-shape")
	assertContains(t, agentModeProfileStrings(signals.AgentModeProfiles), "deep|primary=GPT_5_5|reasoning=medium|levels=low,medium,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false")
	assertContains(t, agentModeProfileStrings(signals.AgentModeProfiles), "smart|primary=CLAUDE_OPUS_4_7|reasoning=high|levels=high,max,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false")
	assertContains(t, agentModeProfileStrings(signals.AgentModeProfiles), "rush|primary=GPT_5_5|reasoning=none|levels=|include=present|deferred=false|visible=true|visibleInV2=false|serverOnly=false")
	assertContains(t, agentModeProfileStrings(signals.AgentModeProfiles), "agg-man|primary=GPT_5_5|reasoning=none|levels=|include=present|deferred=false|visible=false|visibleInV2=false|serverOnly=true")
	assertContains(t, agentModeProfileStrings(signals.AgentModeProfiles), "large|primary=CLAUDE_OPUS_4_6|reasoning=|levels=|include=present|deferred=true|visible=true|visibleInV2=false|serverOnly=false")
	assertContains(t, agentModeProfileStrings(signals.AgentModeProfiles), "nostromo|primary=AMP_NOSTROMO|reasoning=low|levels=|include=present|deferred=false|visible=true|visibleInV2=true|serverOnly=false")
	for _, profile := range signals.AgentModeProfiles {
		if profile.IncludeTools != "" && profile.IncludeTools != "present" {
			t.Fatalf("agent mode %q include tools = %q, want normalized presence", profile.Name, profile.IncludeTools)
		}
	}
	assertContains(t, agentModeRouteStrings(signals.AgentModeRoutes), "agg-man|provider=openai|model=gpt-5.5|primary=GPT_5_5|reasoning=none|context=400000|max_out=128000")
	assertContains(t, agentModeRouteStrings(signals.AgentModeRoutes), "deep|provider=openai|model=gpt-5.5|primary=GPT_5_5|reasoning=medium|context=400000|max_out=128000")
	assertContains(t, agentModeRouteStrings(signals.AgentModeRoutes), "large|provider=anthropic|model=claude-opus-4-6|primary=CLAUDE_OPUS_4_6|reasoning=|context=332000|max_out=32000|effective_context=1000000|effective_max_input=968000|large_alias=claude-opus-4-6-1m")
	assertContains(t, agentModeRouteStrings(signals.AgentModeRoutes), "nostromo|provider=amp|model=amp-nostromo-v1|primary=AMP_NOSTROMO|reasoning=low|context=400000|max_out=128000")
	assertContains(t, agentModeRouteStrings(signals.AgentModeRoutes), "rush|provider=openai|model=gpt-5.5|primary=GPT_5_5|reasoning=none|context=400000|max_out=128000")
	assertContains(t, agentModeRouteStrings(signals.AgentModeRoutes), "smart|provider=anthropic|model=claude-opus-4-7|primary=CLAUDE_OPUS_4_7|reasoning=high|context=332000|max_out=32000")
	assertContains(t, agentModeCoverageStrings(signals.AgentModeCoverage), "deep=local-runtime")
	assertContains(t, agentModeCoverageStrings(signals.AgentModeCoverage), "agg-man=server-only")
	assertContains(t, signals.Settings, "anthropic.interleavedThinking.enabled")
	assertContains(t, signals.Settings, "painter.model")
	assertContains(t, signals.Settings, "showCosts")
	assertNotContains(t, signals.Settings, "constructor")
	assertContains(t, settingDefaultStrings(signals.SettingDefaults), "anthropic.interleavedThinking.enabled=false=local-runtime")
	assertContains(t, settingDefaultStrings(signals.SettingDefaults), "painter.model=gpt-image-2=local-runtime")
	assertContains(t, settingDefaultStrings(signals.SettingDefaults), "showCosts=true=remote-web")
	assertContains(t, settingCoverageStrings(signals.SettingCoverage), "painter.model=local-runtime")
	assertContains(t, settingCoverageStrings(signals.SettingCoverage), "showCosts=remote-web")
	assertContains(t, signals.Models, "gpt-5.5")
	assertContains(t, signals.Models, "amp-nostromo-v1")
	assertContains(t, signals.Models, "claude-opus-4-8")
	assertContains(t, signals.Models, "gemini-3-pro-preview")
	assertNotContains(t, signals.Models, "codex-config")
	assertNotContains(t, signals.Models, "o0IwQDAPBgNVHRMBAf8EBTADAQH")
	assertContains(t, modelLimitStrings(signals.ModelLimits), "claude-opus-4-8|enum=CLAUDE_OPUS_4_8|provider=anthropic|display=Claude Opus 4.8|context=332000|max_out=32000")
	assertContains(t, modelLimitStrings(signals.ModelLimits), "claude-sonnet-4-20250514|enum=CLAUDE_SONNET_4|provider=anthropic|display=Claude Sonnet 4|context=1000000|max_out=32000")
	assertContains(t, modelLimitStrings(signals.ModelLimits), "gpt-5.5|enum=GPT_5_5|provider=openai|display=GPT-5.5|context=400000|max_out=128000")
	assertContains(t, modelLimitStrings(signals.ModelLimits), "moonshotai/Kimi-K2.5|enum=BASETEN_KIMI_K2P5|provider=baseten|display=Kimi K2.5|context=262144|max_out=32000")
	assertContains(t, modelLimitStrings(signals.ModelLimits), "zai-org/GLM-5.2|enum=BASETEN_GLM_5_2|provider=baseten|display=GLM 5.2|context=200000|max_out=32000")
	assertContains(t, largeContextRuleStrings(signals.LargeContextRules), "CLAUDE_OPUS_4_6|alias=claude-opus-4-6-1m|context=1000000|max_out=32000|max_input=968000|requires_enable=true")
	assertContains(t, adaptiveThinkingRuleStrings(signals.AdaptiveThinking), "CLAUDE_OPUS_4_6,CLAUDE_OPUS_4_7,CLAUDE_OPUS_4_8|models=claude-opus-4-6,claude-opus-4-7,claude-opus-4-8|levels=low,medium,high,xhigh,max|default=medium|type=adaptive|display=summarized|output_config=true")
	assertContains(t, providerReasoningRuleStrings(signals.ProviderReasoning), "anthropic|sources=setting:reasoning.effort,mode:reasoningEffort,model-default|setting=|default=high|special=CLAUDE_OPUS_4_7/claude-opus-4-7:medium")
	assertContains(t, providerReasoningRuleStrings(signals.ProviderReasoning), "openai|sources=setting:reasoning.effort,mode:reasoningEffort,provider-default|setting=|default=medium")
	assertContains(t, providerReasoningRuleStrings(signals.ProviderReasoning), "vertexai|sources=setting:gemini.thinkingLevel,mode:reasoningEffort,provider-default|setting=gemini.thinkingLevel|default=medium")
	assertContains(t, providerHeaderRuleStrings(signals.ProviderHeaders), "anthropic|feature=x-amp-feature:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-2026-02-01@anthropic.speed=fast=>anthropic")
	assertContains(t, providerFeatureRuleStrings(signals.ProviderFeatures), "amp.chat|provider=anthropic|callsite=anthropic-chat|tool=|header=x-amp-feature|default=true")
	assertContains(t, providerFeatureRuleStrings(signals.ProviderFeatures), "amp.chat|provider=google|callsite=google-client-default|tool=|header=x-amp-feature|default=true")
	assertContains(t, providerFeatureRuleStrings(signals.ProviderFeatures), "amp.chat|provider=openai|callsite=openai-compatible-client-default|tool=|header=x-amp-feature|default=true")
	assertContains(t, providerFeatureRuleStrings(signals.ProviderFeatures), "amp.review|provider=google|callsite=code-review|tool=code_review|header=x-amp-feature|default=false")
	assertContains(t, providerFeatureRuleStrings(signals.ProviderFeatures), "amp.read-thread|provider=google|callsite=thread-reader|tool=read_thread|header=x-amp-feature|default=false")
	assertContains(t, providerFeatureRuleStrings(signals.ProviderFeatures), "amp.image-generation|provider=google|callsite=google-image-generation-default|tool=|header=x-amp-feature|default=true")
	assertContains(t, providerFeatureRuleStrings(signals.ProviderFeatures), "amp.image-generation|provider=openai|callsite=openai-image-generation-default|tool=|header=x-amp-feature|default=true")
	assertContains(t, providerFeatureRuleStrings(signals.ProviderFeatures), "amp.painter|provider=google|callsite=painter-gemini-image|tool=painter|header=x-amp-feature|default=false")
	assertContains(t, providerFeatureRuleStrings(signals.ProviderFeatures), "amp.painter|provider=openai|callsite=painter-openai-image|tool=painter|header=x-amp-feature|default=false")
	assertContains(t, compactionRuleStrings(signals.CompactionRules), "anthropic-tool-runner|provider=anthropic|trigger=observed-usage|timing=post-response|threshold=100000|usage=input_tokens,cache_creation_input_tokens,cache_read_input_tokens,output_tokens|summary_prompt=continuation-summary|history_role=user|tail_assistant=strip-tool-use-blocks|helper=x-stainless-helper:compaction")
	assertContains(t, modelCoverageStrings(signals.ModelCoverage), "gpt-5.5=openai/gpt")
	assertContains(t, modelCoverageStrings(signals.ModelCoverage), "amp-nostromo-v1=amp/amp-nostromo")
	assertContains(t, modelCoverageStrings(signals.ModelCoverage), "claude-opus-4-8=anthropic/claude-opus")
	assertContains(t, modelCoverageStrings(signals.ModelCoverage), "gemini-3-pro-preview=google/gemini-pro")
	assertContains(t, signals.ActorRuntime, "threadActor")
	assertContains(t, signals.ActorRuntime, "threadStatusUpdated")
	assertContains(t, signals.ActorRuntime, "rvt-token")
	assertContains(t, signals.ActorRuntime, "ActionRequest")
	assertContains(t, signals.ActorRuntime, "ActionResponse")
	assertContains(t, signals.ActorRuntime, "SubscriptionRequest")
	assertContains(t, signals.ActorRuntime, "rivet_encoding.4")
	assertContains(t, signals.ActorRuntime, "serializeWithEmbeddedVersion")
	assertContains(t, signals.ActorRuntime, "deserializeWithEmbeddedVersion")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "threadActor=local-runtime")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "threadStatusUpdated=local-runtime")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "rvt-token=actor-protocol")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "RivetKit=actor-protocol")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "ActionRequest=actor-protocol")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "ActionResponse=actor-protocol")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "SubscriptionRequest=actor-protocol")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "rivet_encoding.4=actor-protocol")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "serializeWithEmbeddedVersion=actor-protocol")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "deserializeWithEmbeddedVersion=actor-protocol")
	if len(signals.PromptFingerprints) != 1 {
		t.Fatalf("prompt fingerprints = %d, want 1", len(signals.PromptFingerprints))
	}
	if signals.PromptFingerprints[0].Kind != "prompt" {
		t.Fatalf("prompt kind = %q, want prompt", signals.PromptFingerprints[0].Kind)
	}
	assertContains(t, signals.PromptFingerprints[0].Tags, "compaction")
	assertContains(t, signals.PromptFingerprints[0].Tags, "code-review")
	assertContains(t, promptTagCountStrings(signals.PromptTagCounts), "prompt/code-review=1")
	assertContains(t, promptTagCountStrings(signals.PromptTagCounts), "prompt/compaction=1")
}

func TestClassifyStringsSynthesizesRivetEncodingWhenEmbeddedVersionMarkersAreSplit(t *testing.T) {
	signals := classifyStrings([]string{
		`ActionRequest`,
		`ActionResponse`,
		`rivet_encoding.`,
		`serializeWithEmbeddedVersion`,
		`deserializeWithEmbeddedVersion`,
	})

	assertContains(t, signals.ActorRuntime, "rivet_encoding.4")
	assertContains(t, actorCoverageStrings(signals.ActorCoverage), "rivet_encoding.4=actor-protocol")
}

func TestClassifyStringsRejectsReviewContractNearMisses(t *testing.T) {
	tests := []struct {
		name   string
		strs   []string
		marker string
	}{
		{
			name:   "run check uri shape rejects near miss validation fields",
			strs:   []string{`"checkURI_internal"in o&&typeof o.checkURI_internal==="string";"checkNameDeclaration"in o&&typeof o.checkNameDeclaration==="string"`},
			marker: "run-check-uri-input-shape",
		},
		{
			name:   "run check uri shape requires both validation fields together",
			strs:   []string{`"checkURI"in o&&typeof o.checkURI==="string"`, `"checkName"in o&&typeof o.checkName==="string"`},
			marker: "run-check-uri-input-shape",
		},
		{
			name:   "run check uri shape requires same validated object",
			strs:   []string{`"checkURI"in a&&typeof a.checkURI==="string";"checkName"in b&&typeof b.checkName==="string"`},
			marker: "run-check-uri-input-shape",
		},
		{
			name:   "run check uri shape rejects reused object name in separate scopes",
			strs:   []string{`function a(){return "checkURI"in o&&typeof o.checkURI==="string"}function b(){return "checkName"in o&&typeof o.checkName==="string"}`},
			marker: "run-check-uri-input-shape",
		},
		{
			name:   "instructions shape requires json field context",
			strs:   []string{`instructions Outcome-first brief for the check agent`},
			marker: "run-check-instructions-input-shape",
		},
		{
			name:   "low severity filter requires check footer context",
			strs:   []string{`severity!=="low"`},
			marker: "human-review-filters-low-severity",
		},
		{
			name:   "check footer status ignores review instructions text",
			strs:   []string{`Only report issues found by checks`},
			marker: "human-review-check-footer-counts-issues",
		},
		{
			name:   "check footer status requires check name renderer",
			strs:   []string{`rR.yellow("issues found");`},
			marker: "human-review-check-footer-counts-issues",
		},
		{
			name:   "review deprecation warning requires both oracle commands",
			strs:   []string{`Warning: amp review is deprecated. Ask Amp to review the changes with the oracle instead: Ask the oracle to review uncommitted changes`},
			marker: "review-cli-deprecation-warning",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signals := classifyStrings(tt.strs)
			assertNotContains(t, signals.ReviewContract, tt.marker)
		})
	}
}

func TestClassifyStringsDetectsReviewContractMarkers(t *testing.T) {
	tests := []struct {
		name   string
		str    string
		marker string
	}{
		{
			name:   "submit review omits run check findings",
			str:    `Do not include run_check findings in submit_review`,
			marker: "review-submit-omits-run-check-findings",
		},
		{
			name:   "cli appends check findings",
			str:    `CLI appends structured check findings mechanically`,
			marker: "review-cli-appends-check-findings",
		},
		{
			name:   "run check uri input shape",
			str:    `let o=input,k=o&&typeof o==="object"&&"checkURI"in o&&typeof o.checkURI==="string"?o.checkURI:void 0,p=o&&typeof o==="object"&&"checkName"in o&&typeof o.checkName==="string"?o.checkName:void 0`,
			marker: "run-check-uri-input-shape",
		},
		{
			name:   "run check uri input shape with adjacent statements",
			str:    `"checkURI"in o&&typeof o.checkURI==="string";"checkName"in o&&typeof o.checkName==="string"`,
			marker: "run-check-uri-input-shape",
		},
		{
			name:   "run check uri input shape with remapped minifier variable",
			str:    `let _=o.input,k=_&&typeof _==="object"&&"checkURI"in _&&typeof _.checkURI==="string"?_.checkURI:void 0,p=_&&typeof _==="object"&&"checkName"in _&&typeof _.checkName==="string"?_.checkName:void 0`,
			marker: "run-check-uri-input-shape",
		},
		{
			name:   "run check instructions input shape",
			str:    `"instructions":"Outcome-first brief for the check agent"`,
			marker: "run-check-instructions-input-shape",
		},
		{
			name:   "human review filters low severity",
			str:    `if(severity!=="low")out.push("The following checks were run")`,
			marker: "human-review-filters-low-severity",
		},
		{
			name:   "human review check footer counts issues",
			str:    `rR.yellow("issues found"); footer.push(result.check.name)`,
			marker: "human-review-check-footer-counts-issues",
		},
		{
			name:   "review cli deprecation warning",
			str:    "Warning: `amp review` is deprecated. Ask Amp to review the changes with the oracle instead: echo 'Ask the oracle to review uncommitted changes' | amp; echo 'Ask the oracle to review the changes since the merge base' | amp",
			marker: "review-cli-deprecation-warning",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signals := classifyStrings([]string{tt.str})
			assertContains(t, signals.ReviewContract, tt.marker)
		})
	}
}

func TestClassifyStringsDetectsSplitReviewCLIDeprecationWarning(t *testing.T) {
	tests := map[string][]string{
		"three strings": {
			"Warning: `amp review` is deprecated. Ask Amp to review the changes with the oracle instead:",
			"echo 'Ask the oracle to review uncommitted changes' | amp",
			"echo 'Ask the oracle to review the changes since the merge base' | amp",
		},
		"first two fragments together": {
			"Ask Amp to review the changes with the oracle instead: echo 'Ask the oracle to review uncommitted changes' | amp",
			"echo 'Ask the oracle to review the changes since the merge base' | amp",
		},
		"last two fragments together": {
			"Warning: Ask Amp to review the changes with the oracle instead:",
			"echo 'Ask the oracle to review uncommitted changes' | amp; echo 'Ask the oracle to review the changes since the merge base' | amp",
		},
		"fragment split inside phrase": {
			"Warning: `amp review` is deprecated. Ask Amp to review the changes with the oracle ",
			"instead: echo 'Ask the oracle to review uncommitted changes' | amp; echo 'Ask the oracle to review the changes since the merge base' | amp",
		},
		"four strings with fragments split inside phrases": {
			"Warning: `amp review` is deprecated. Ask Amp to review the changes with the oracle ",
			"instead: echo 'Ask the oracle to review uncommitted changes' | amp; ",
			"echo 'Ask the oracle to review ",
			"the changes since the merge base' | amp",
		},
	}

	for name, strs := range tests {
		t.Run(name, func(t *testing.T) {
			signals := classifyStrings(strs)
			assertContains(t, signals.ReviewContract, "review-cli-deprecation-warning")
		})
	}
}

func TestClassifyStringsRejectsInvalidSplitReviewCLIDeprecationWarning(t *testing.T) {
	tests := map[string][]string{
		"non-adjacent fragments": {
			"Warning: `amp review` is deprecated. Ask Amp to review the changes with the oracle instead:",
			"echo 'Ask the oracle to review uncommitted changes' | amp",
			"unrelated binary string",
			"echo 'Ask the oracle to review the changes since the merge base' | amp",
		},
		"reordered fragments": {
			"Warning: `amp review` is deprecated. Ask Amp to review the changes with the oracle instead:",
			"echo 'Ask the oracle to review the changes since the merge base' | amp",
			"echo 'Ask the oracle to review uncommitted changes' | amp",
		},
	}

	for name, strs := range tests {
		t.Run(name, func(t *testing.T) {
			signals := classifyStrings(strs)
			assertNotContains(t, signals.ReviewContract, "review-cli-deprecation-warning")
		})
	}
}

func TestPromptFingerprintClassifiesMinifiedSourceSeparately(t *testing.T) {
	source := strings.Repeat(`function No0(){let R="reasoning effort";if(R?.x??false){return new XT("tool call failed");}}async function X(){await Y();return this.toolbox.path=>R;};`, 8)
	fp, ok := promptFingerprint(source)
	if !ok {
		t.Fatal("source-like prompt fingerprint was not extracted")
	}
	if fp.Kind != "source" {
		t.Fatalf("source-like fingerprint kind = %q, want source", fp.Kind)
	}

	prompt := strings.Repeat("Please perform compaction guidance for the system prompt and code review workflow with available tools and skills. ", 12)
	fp, ok = promptFingerprint(prompt)
	if !ok {
		t.Fatal("prompt fingerprint was not extracted")
	}
	if fp.Kind != "prompt" {
		t.Fatalf("prompt fingerprint kind = %q, want prompt", fp.Kind)
	}
}

func TestBuildPromptCatalogExtractsBoundedExcerpts(t *testing.T) {
	prompt := strings.Repeat("Please perform compaction guidance for the system prompt and code review workflow with available tools and skills. ", 16)
	source := strings.Repeat(`function No0(){let R="reasoning effort";if(R?.x??false){return new XT("tool call failed");}}async function X(){await Y();return this.toolbox.path=>R;};`, 8)
	path := filepath.Join(t.TempDir(), "amp")
	if err := os.WriteFile(path, []byte("prefix\x00"+prompt+"\x00"+source+"\x00"), 0o644); err != nil {
		t.Fatal(err)
	}

	catalog, err := BuildPromptCatalog(path)
	if err != nil {
		t.Fatal(err)
	}

	promptFP, ok := promptFingerprint(prompt)
	if !ok {
		t.Fatal("prompt fingerprint was not extracted")
	}
	promptExcerpt, ok := catalog[promptFP.SHA256]
	if !ok {
		t.Fatal("prompt fingerprint missing from catalog")
	}
	if promptExcerpt.Fingerprint.Kind != "prompt" {
		t.Fatalf("prompt catalog kind = %q, want prompt", promptExcerpt.Fingerprint.Kind)
	}
	if !strings.Contains(promptExcerpt.Excerpt, "compaction guidance") {
		t.Fatalf("prompt excerpt did not include anchor: %q", promptExcerpt.Excerpt)
	}
	if len(promptExcerpt.Excerpt) > 606 {
		t.Fatalf("prompt excerpt length = %d, want bounded", len(promptExcerpt.Excerpt))
	}

	sourceFP, ok := promptFingerprint(source)
	if !ok {
		t.Fatal("source fingerprint was not extracted")
	}
	sourceExcerpt, ok := catalog[sourceFP.SHA256]
	if !ok {
		t.Fatal("source fingerprint missing from catalog")
	}
	if sourceExcerpt.Fingerprint.Kind != "source" {
		t.Fatalf("source catalog kind = %q, want source", sourceExcerpt.Fingerprint.Kind)
	}
	if !strings.Contains(sourceExcerpt.Excerpt, "reasoning effort") {
		t.Fatalf("source excerpt did not include anchor: %q", sourceExcerpt.Excerpt)
	}
}

func TestPromptTagCountsAreSeparatedByKind(t *testing.T) {
	counts := promptTagCounts(map[string]PromptFingerprint{
		"prompt": {SHA256: "prompt", Kind: "prompt", Tags: []string{"tools", "guidance"}},
		"source": {SHA256: "source", Kind: "source", Tags: []string{"tools", "settings"}},
	})
	strings := promptTagCountStrings(counts)

	assertContains(t, strings, "prompt/guidance=1")
	assertContains(t, strings, "prompt/tools=1")
	assertContains(t, strings, "source/settings=1")
	assertContains(t, strings, "source/tools=1")
}

func TestKnownPromptTagCountsMatchCommittedBaseline(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}

	kindCounts := map[string]int{}
	for _, fingerprint := range baseline.Signals.PromptFingerprints {
		kindCounts[promptKindOrDefault(fingerprint.Kind)]++
	}
	if fmt.Sprint(kindCounts) != fmt.Sprint(knownPromptKindCountValues) {
		t.Fatalf("known prompt kind counts = %#v, baseline = %#v", knownPromptKindCountValues, kindCounts)
	}

	tagCounts := map[string]int{}
	for _, count := range baseline.Signals.PromptTagCounts {
		tagCounts[promptTagCountKey(count.Kind, count.Tag)] = count.Count
	}
	if fmt.Sprint(tagCounts) != fmt.Sprint(knownPromptTagCountValues) {
		t.Fatalf("known prompt tag counts = %#v, baseline = %#v", knownPromptTagCountValues, tagCounts)
	}

	tagSetCounts := map[string]int{}
	for _, fingerprint := range baseline.Signals.PromptFingerprints {
		key := promptTagCountKey(promptKindOrDefault(fingerprint.Kind), strings.Join(fingerprint.Tags, ","))
		tagSetCounts[key]++
	}
	assertStringSetsEqual(t, "prompt tag-set counts", intMapStrings(tagSetCounts), intMapStrings(knownPromptTagSetCountValues))
}

func TestCommittedBaselineHasCurrentSchemaReleaseSourceAndNoAuditProblems(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}

	if baseline.Schema != snapshotSchema {
		t.Fatalf("baseline schema = %d, want %d", baseline.Schema, snapshotSchema)
	}
	if !snapshotLooksLikeReleaseBinary(baseline) {
		t.Fatalf("baseline source size = %d, want release binary size >= %d", baseline.Source.SizeBytes, minReleaseBinarySizeBytes)
	}
	if strings.TrimSpace(baseline.Source.Path) == "" {
		t.Fatal("baseline source path is empty")
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(baseline.Source.SHA256) {
		t.Fatalf("baseline source sha256 = %q, want lowercase hex sha256", baseline.Source.SHA256)
	}
	if len(baseline.Source.Versions) == 0 {
		t.Fatal("baseline source versions are empty")
	}
	for _, version := range baseline.Source.Versions {
		if !versionPattern.MatchString(version) {
			t.Fatalf("baseline source version = %q, want Amp version", version)
		}
	}
	if baseline.Source.StringsScanned == 0 {
		t.Fatal("baseline source strings_scanned is empty")
	}
	assertStringSetsEqual(t, "baseline audit problems", snapshotAuditProblems(baseline), nil)
}

func TestCommittedBaselineMatchesInstalledAmpBinaryWhenPathMatches(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}
	if baseline.Source.Path != defaultAmpBinaryPath {
		t.Skipf("baseline source path %q does not match default Amp binary path %q", baseline.Source.Path, defaultAmpBinaryPath)
	}
	if _, err := os.Stat(defaultAmpBinaryPath); err != nil {
		t.Skipf("installed Amp binary is not available at %q: %v", defaultAmpBinaryPath, err)
	}
	raw, err := os.ReadFile(defaultAmpBinaryPath)
	if err != nil {
		t.Fatalf("read installed Amp binary: %v", err)
	}
	installedSHA := sha256.Sum256(raw)
	if installedHex := hex.EncodeToString(installedSHA[:]); installedHex != baseline.Source.SHA256 {
		t.Skipf("installed Amp binary sha256 %s does not match baseline source sha256 %s; the baseline binary was superseded (likely by Amp auto-update)", installedHex, baseline.Source.SHA256)
	}

	current, err := BuildSnapshot(defaultAmpBinaryPath)
	if err != nil {
		t.Fatalf("build current Amp snapshot: %v", err)
	}
	diff := diffSnapshots(baseline, current)
	if hasDiff(diff) {
		t.Fatalf("committed baseline differs from installed Amp binary: categories=%v prompt_added=%d prompt_removed=%d", sortedBoolMapKeys(changedCategorySet(diff)), len(diff.Prompts.Added), len(diff.Prompts.Removed))
	}
	if problems := snapshotAuditProblems(current); len(problems) > 0 {
		t.Fatalf("installed Amp snapshot has audit problems: %v", problems)
	}
}

var knownPromptTagSetCountValues = map[string]int{
	"prompt/artifacts,painter,skills,tools":             1,
	"prompt/code-review,settings":                       1,
	"prompt/compaction":                                 6,
	"prompt/guidance":                                   4,
	"prompt/painter":                                    1,
	"prompt/settings":                                   1,
	"prompt/skills":                                     9,
	"prompt/skills,system-prompt,tools":                 1,
	"prompt/skills,tools":                               3,
	"prompt/tools":                                      36,
	"source/artifacts":                                  1,
	"source/artifacts,code-review,painter,skills,tools": 1,
	"source/artifacts,compaction,guidance,skills,tools": 1,
	"source/artifacts,compaction,painter,skills,tools":  1,
	"source/artifacts,guidance,skills,tools":            1,
	"source/artifacts,painter,settings":                 1,
	"source/code-review,guidance,settings,skills,tools": 1,
	"source/compaction":                                 8,
	"source/compaction,skills":                          1,
	"source/compaction,skills,tools":                    3,
	"source/compaction,tools":                           6,
	"source/guidance":                                   9,
	"source/guidance,settings":                          1,
	"source/guidance,settings,skills,tools":             1,
	"source/guidance,skills":                            3,
	"source/guidance,skills,tools":                      6,
	"source/guidance,tools":                             9,
	"source/painter":                                    1,
	"source/painter,skills":                             1,
	"source/painter,tools":                              1,
	"source/settings":                                   3,
	"source/settings,skills,tools":                      1,
	"source/settings,system-prompt,tools":               1,
	"source/settings,tools":                             5,
	"source/skills":                                     18,
	"source/skills,tools":                               21,
	"source/tools":                                      109,
}

func TestLifecycleChecklistKnownPromptTagCountsRunFocusedChecks(t *testing.T) {
	cases := map[string][]string{
		"prompt/artifacts":     {"remote web control surface"},
		"prompt/code-review":   {"tools, code review, skills, and images"},
		"prompt/compaction":    {"compaction and continuation prompts"},
		"prompt/guidance":      {"compaction and continuation prompts", "streaming assistant and tool edits"},
		"prompt/painter":       {"tools, code review, skills, and images"},
		"prompt/settings":      {"model routing, modes, and reasoning", "remote web control surface"},
		"prompt/skills":        {"tools, code review, skills, and images"},
		"prompt/system-prompt": {"compaction and continuation prompts", "streaming assistant and tool edits"},
		"prompt/tools":         {"tools, code review, skills, and images", "streaming assistant and tool edits", "upstream-owned thread read and search", "remote web control surface"},
		"source/artifacts":     {"remote web control surface"},
		"source/code-review":   {"tools, code review, skills, and images"},
		"source/compaction":    {"compaction and continuation prompts"},
		"source/guidance":      {"compaction and continuation prompts", "streaming assistant and tool edits"},
		"source/painter":       {"tools, code review, skills, and images"},
		"source/settings":      {"model routing, modes, and reasoning", "remote web control surface"},
		"source/skills":        {"tools, code review, skills, and images"},
		"source/system-prompt": {"compaction and continuation prompts", "streaming assistant and tool edits"},
		"source/tools":         {"tools, code review, skills, and images", "streaming assistant and tool edits", "upstream-owned thread read and search", "remote web control surface"},
	}
	if len(cases) != len(knownPromptTagCountValues) {
		t.Fatalf("known prompt tag count mapping covers %d tags, want %d", len(cases), len(knownPromptTagCountValues))
	}
	for tag, expectedAreas := range cases {
		t.Run(tag, func(t *testing.T) {
			count, ok := knownPromptTagCountValues[tag]
			if !ok {
				t.Fatalf("test case uses unknown prompt/source tag %q", tag)
			}
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "prompt-tag-counts",
				Added: []string{fmt.Sprintf("%s=%d", tag, count)},
			}}}
			areas := lifecycleCheckAreas(lifecycleChecklist(Snapshot{}, diff, false))

			assertNotContains(t, areas, "unknown signal triage")
			for _, area := range expectedAreas {
				assertContains(t, areas, area)
			}
		})
	}
}

func TestKnownModelModeReasoningValuesMatchCommittedBaseline(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}
	if _, ok := knownRawModelValues["claude-opus-5"]; !ok {
		t.Fatal("claude-opus-5 is missing from known raw models")
	}
	wantOpusFive := modelLimitExpectation{Enum: "CLAUDE_OPUS_5", Provider: "anthropic", DisplayName: "Claude Opus 5", ContextWindow: 1000000, MaxOutputTokens: 128000}
	if got := knownModelLimitValues["claude-opus-5"]; got != wantOpusFive {
		t.Fatalf("claude-opus-5 limit = %#v, want %#v", got, wantOpusFive)
	}

	assertStringSetsEqual(t, "models", baseline.Signals.Models, sortedKeys(activeRawModelValues()))
	assertStringSetsEqual(t, "model coverage", modelCoverageStrings(baseline.Signals.ModelCoverage), sortedKeys(expectedCoverageValuesFromSet(activeRawModelValues(), func(name string) string {
		provider, family := modelProviderAndFamily(name)
		return provider + "/" + family
	})))
	assertStringSetsEqual(t, "agent mode profiles", agentModeProfileStrings(baseline.Signals.AgentModeProfiles), sortedKeys(knownAgentModeProfileValues))
	assertStringSetsEqual(t, "agent mode routes", agentModeRouteStrings(baseline.Signals.AgentModeRoutes), sortedKeys(knownAgentModeRouteValues))
	assertStringSetsEqual(t, "agent mode coverage", agentModeCoverageStrings(baseline.Signals.AgentModeCoverage), sortedKeys(expectedCoverageValuesFromMap(activeAgentModeScopes())))

	expectedModelLimits := make([]string, 0, len(knownModelLimitValues))
	for name, expectation := range knownModelLimitValues {
		expectedModelLimits = append(expectedModelLimits, modelLimitExpectationString(name, expectation))
	}
	assertStringSetsEqual(t, "model limits", modelLimitStrings(baseline.Signals.ModelLimits), expectedModelLimits)
	assertStringSetsEqual(t, "large context rules", largeContextRuleStrings(baseline.Signals.LargeContextRules), sortedKeys(knownLargeContextRuleValues))
	assertStringSetsEqual(t, "adaptive thinking rules", adaptiveThinkingRuleStrings(baseline.Signals.AdaptiveThinking), sortedKeys(knownAdaptiveThinkingRuleValues))
	assertStringSetsEqual(t, "provider reasoning rules", providerReasoningRuleStrings(baseline.Signals.ProviderReasoning), sortedKeys(knownProviderReasoningRuleValues))
}

func TestKnownSettingsValuesMatchCommittedBaseline(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}
	if got := settingScopes["agent.speed"]; got != "local-runtime" {
		t.Fatalf("agent.speed scope = %q, want local-runtime", got)
	}
	if got := knownSettingDefaultValues["agent.speed"]; got != "undefined" {
		t.Fatalf("agent.speed default = %q, want undefined", got)
	}

	assertStringSetsEqual(t, "settings", baseline.Signals.Settings, sortedStringMapKeys(activeSettingScopes()))
	assertStringSetsEqual(t, "setting coverage", settingCoverageStrings(baseline.Signals.SettingCoverage), sortedKeys(expectedCoverageValuesFromMap(activeSettingScopes())))

	expectedDefaults := make([]string, 0, len(knownSettingDefaultValues))
	for name, value := range knownSettingDefaultValues {
		if _, retired := retiredSettingNames[name]; retired {
			continue
		}
		expectedDefaults = append(expectedDefaults, name+"="+value+"="+settingScopes[name])
	}
	assertStringSetsEqual(t, "setting defaults", settingDefaultStrings(baseline.Signals.SettingDefaults), expectedDefaults)
	assertStringSetsEqual(t, "mode setting markers", baseline.Signals.ModeSettingMarkers, sortedStringMapKeys(activeModeSettingMarkers()))
	assertStringSetsEqual(t, "mode setting coverage", modeSettingCoverageStrings(baseline.Signals.ModeSettingCoverage), sortedKeys(expectedCoverageValuesFromMap(activeModeSettingMarkers())))
}

// Categories current release binaries no longer carry; the committed baseline
// is expected to have no entries for them.
var retiredLifecycleCategories = map[string]bool{
	"large-context-rules":      true,
	"adaptive-thinking-rules":  true,
	"provider-reasoning-rules": true,
	"provider-header-rules":    true,
	"provider-feature-rules":   true,
	"compaction-rules":         true,
	"tool-cancel-reasons":      true,
	"tool-cancel-coverage":     true,
}

func TestKnownLifecycleToolValuesMatchCommittedBaseline(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}

	assertStringSetsEqual(t, "thread delta events", baseline.Signals.ThreadDeltaEvents, sortedKeys(knownRawThreadDeltaEventValues))
	assertStringSetsEqual(t, "thread delta coverage", threadDeltaCoverageStrings(baseline.Signals.ThreadDeltaCoverage), sortedKeys(expectedCoverageValuesFromSet(knownRawThreadDeltaEventValues, threadDeltaArea)))
	// the cancel-reason taxonomy left the client at gaac893; current baselines
	// must not carry any.
	assertStringSetsEqual(t, "tool cancel reasons", baseline.Signals.ToolCancelReasons, nil)
	assertStringSetsEqual(t, "tool cancel coverage", toolCancelCoverageStrings(baseline.Signals.ToolCancelCoverage), nil)
	assertStringSetsEqual(t, "tool run statuses", baseline.Signals.ToolRunStatuses, sortedKeys(knownToolRunStatusValues))
	assertStringSetsEqual(t, "tool run coverage", toolRunCoverageStrings(baseline.Signals.ToolRunCoverage), sortedKeys(expectedCoverageValuesFromSet(knownToolRunStatusValues, func(name string) string {
		return toolRunStatusAreas[name]
	})))
	assertStringSetsEqual(t, "tool catalog markers", baseline.Signals.ToolCatalog, sortedKeys(withoutRetired(knownToolCatalogMarkerValues, retiredToolCatalogMarkerValues)))
	assertStringSetsEqual(t, "tool catalog coverage", toolCatalogCoverageStrings(baseline.Signals.ToolCatalogCoverage), sortedKeys(expectedCoverageValuesFromSet(withoutRetired(knownToolCatalogMarkerValues, retiredToolCatalogMarkerValues), func(name string) string {
		return toolCatalogMarkers[name]
	})))
	assertStringSetsEqual(t, "stream-json markers", baseline.Signals.StreamJSONMarkers, sortedStringMapKeys(streamJSONMarkers))
	assertStringSetsEqual(t, "stream-json coverage", streamJSONCoverageStrings(baseline.Signals.StreamJSONCoverage), sortedKeys(expectedCoverageValuesFromMap(streamJSONMarkers)))
}

func TestKnownRouteProviderActorValuesMatchCommittedBaseline(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}

	assertStringSetsEqual(t, "routes", baseline.Signals.Routes, sortedKeys(withoutRetired(knownRouteValues, retiredRouteNames)))
	assertStringSetsEqual(t, "route methods", routeMethodStrings(baseline.Signals.RouteMethods), sortedKeys(withoutRetired(knownRouteMethodValues, retiredRouteMethodValues)))
	assertStringSetsEqual(t, "route coverage", routeCoverageStrings(baseline.Signals.RouteCoverage), sortedKeys(expectedCoverageValuesFromSet(withoutRetired(knownRouteValues, retiredRouteNames), routeScope)))
	assertStringSetsEqual(t, "thread reader markers", baseline.Signals.ThreadReaderMarkers, sortedKeys(expectedThreadReaderMarkerValues))
	assertStringSetsEqual(t, "thread reader coverage", threadReaderCoverageStrings(baseline.Signals.ThreadReaderCoverage), sortedKeys(expectedCoverageValuesFromSet(expectedThreadReaderMarkerValues, func(name string) string {
		return threadReaderMarkers[name]
	})))
	assertStringSetsEqual(t, "provider protocol markers", baseline.Signals.ProviderProtocol, sortedStringMapKeys(activeProviderProtocolMarkers()))
	assertStringSetsEqual(t, "provider protocol coverage", providerCoverageStrings(baseline.Signals.ProviderCoverage), sortedKeys(expectedCoverageValuesFromMap(activeProviderProtocolMarkers())))
	assertStringSetsEqual(t, "review contract markers", baseline.Signals.ReviewContract, sortedKeys(withoutRetired(knownReviewContractMarkerValues, retiredReviewContractMarkerValues)))
	assertStringSetsEqual(t, "provider header rules", providerHeaderRuleStrings(baseline.Signals.ProviderHeaders), sortedKeys(knownProviderHeaderRuleValues))
	assertStringSetsEqual(t, "provider feature rules", providerFeatureRuleStrings(baseline.Signals.ProviderFeatures), sortedKeys(knownProviderFeatureRuleValues))
	assertStringSetsEqual(t, "compaction rules", compactionRuleStrings(baseline.Signals.CompactionRules), sortedKeys(knownCompactionRuleValues))
	assertStringSetsEqual(t, "actor runtime markers", baseline.Signals.ActorRuntime, sortedKeys(knownActorMarkerValues))
	assertStringSetsEqual(t, "actor runtime coverage", actorCoverageStrings(baseline.Signals.ActorCoverage), sortedKeys(expectedCoverageValuesFromSet(knownActorMarkerValues, func(name string) string {
		return actorMarkerAreas[name]
	})))
}

func TestWritePromptDiffExcerptsShowsAddedExcerptAndRemovedUnavailable(t *testing.T) {
	added := PromptFingerprint{SHA256: "abcdef0123456789", Length: 200, Kind: "prompt", Tags: []string{"guidance"}}
	removed := PromptFingerprint{SHA256: "fedcba9876543210", Length: 220, Kind: "source", Tags: []string{"tools"}}
	diff := promptDiff{
		Added:   []PromptFingerprint{added},
		Removed: []PromptFingerprint{removed},
	}
	catalog := map[string]promptExcerpt{
		added.SHA256: {
			Fingerprint: added,
			Excerpt:     "Please inspect guidance before updating the baseline.",
		},
	}
	var out strings.Builder

	writePromptDiffExcerpts(&out, diff, catalog)

	text := out.String()
	assertContainsString(t, text, "+ abcdef012345 len=200 kind=prompt tags=guidance")
	assertContainsString(t, text, "Please inspect guidance")
	assertContainsString(t, text, "- fedcba987654 len=220 kind=source tags=tools")
	assertContainsString(t, text, "excerpt unavailable; fingerprint is not present in the current binary")
}

func TestPrintSuggestionsReportsUnknownPromptAndSourceTags(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{
		{SHA256: "prompt", Length: 240, Kind: "prompt", Tags: []string{"future-prompt-surface"}},
		{SHA256: "source", Length: 800, Kind: "source", Tags: []string{"future-source-surface"}},
	}}}

	text := captureAuditStdout(t, func() {
		printSuggestions(diff)
	})

	assertContainsString(t, text, "unknown prompt tags detected: classify the prompt surface before refreshing the baseline: future-prompt-surface")
	assertContainsString(t, text, "unknown source tags detected: classify the bundled source surface before refreshing the baseline: future-source-surface")
}

func TestPrintSuggestionsReportsUnknownPromptKinds(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "future",
		Length: 240,
		Kind:   "future-kind",
		Tags:   []string{"guidance"},
	}}}}

	text := captureAuditStdout(t, func() {
		printSuggestions(diff)
	})

	assertContainsString(t, text, "unknown prompt fingerprint kinds detected: classify the prompt/source kind before refreshing the baseline: future-kind")
}

func TestPrintSuggestionsReportsUnknownPromptAndSourceTagCounts(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "prompt-tag-counts",
		Added: []string{"prompt/future-prompt-surface=1", "source/future-source-surface=1"},
	}}}

	text := captureAuditStdout(t, func() {
		printSuggestions(diff)
	})

	assertContainsString(t, text, "unknown prompt tag counts detected: classify the prompt surface before refreshing the baseline: future-prompt-surface")
	assertContainsString(t, text, "unknown source tag counts detected: classify the bundled source surface before refreshing the baseline: future-source-surface")
}

func TestPrintSuggestionsReportsUnknownPromptTagCountKinds(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "prompt-tag-counts",
		Added: []string{"future-kind/guidance=1"},
	}}}

	text := captureAuditStdout(t, func() {
		printSuggestions(diff)
	})

	assertContainsString(t, text, "unknown prompt tag-count kinds detected: classify the prompt/source kind before refreshing the baseline: future-kind")
}

func TestPrintSuggestionsReportsPromptTagCountValueDrift(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "prompt-tag-counts",
		Added: []string{"prompt/guidance=23"},
	}}}

	text := captureAuditStdout(t, func() {
		printSuggestions(diff)
	})

	assertContainsString(t, text, "unknown prompt/source tag count values detected: compare exact prompt/source fingerprint counts against the Amp binary before refreshing the baseline: prompt/guidance=23")
}

func TestPrintSuggestionsReportsPromptKindCountValueDrift(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "prompt-kind-counts",
		Added: []string{"prompt=121"},
	}}}

	text := captureAuditStdout(t, func() {
		printSuggestions(diff)
	})

	assertContainsString(t, text, "unknown prompt/source kind count values detected: compare exact prompt/source fingerprint totals against the Amp binary before refreshing the baseline: prompt=121")
}

func TestPrintSuggestionsEveryDiffCategoryHasNextCheck(t *testing.T) {
	for _, category := range diffSnapshots(Snapshot{}, Snapshot{}).Categories {
		t.Run(category.Name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  category.Name,
				Added: []string{sampleDiffValue(category.Name)},
			}}}

			text := captureAuditStdout(t, func() {
				printSuggestions(diff)
			})

			assertContainsString(t, text, "next checks:")
			assertContainsString(t, text, "  - ")
		})
	}
}

func TestPrintSuggestionsPromptFingerprintsHaveNextChecks(t *testing.T) {
	cases := []struct {
		name    string
		prompts promptDiff
	}{
		{name: "prompt", prompts: promptDiff{Added: []PromptFingerprint{{
			SHA256: "prompt",
			Length: 240,
			Kind:   "prompt",
			Tags:   []string{"guidance"},
		}}}},
		{name: "source", prompts: promptDiff{Added: []PromptFingerprint{{
			SHA256: "source",
			Length: 820,
			Kind:   "source",
			Tags:   []string{"tools"},
		}}}},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			text := captureAuditStdout(t, func() {
				printSuggestions(auditDiff{Prompts: tt.prompts})
			})

			assertContainsString(t, text, "next checks:")
			assertContainsString(t, text, "  - ")
		})
	}
}

func TestDiffSnapshotsReportsAddedAndRemovedSignals(t *testing.T) {
	old := Snapshot{Source: SourceInfo{SHA256: "old-sha", SizeBytes: 100, Versions: []string{"0.0.1-gold"}, BuildStamps: []string{"2026-01-01T00:00:00Z"}, StringsScanned: 10}, Signals: Signals{
		Routes:              []string{"/api/thread-actors", "/threads"},
		RouteMethods:        []RouteMethods{{Name: "/api/thread-actors", Methods: []string{"POST"}, Scope: "local-runtime"}, {Name: "/threads", Methods: []string{"GET"}, Scope: "amp-owned"}},
		RouteCoverage:       []RouteCoverage{{Name: "/api/thread-actors", Scope: "local-runtime"}, {Name: "/threads", Scope: "amp-owned"}},
		ThreadDeltaEvents:   []string{"user:message"},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "user:message", Area: "user-message"}},
		ToolCancelReasons:   []string{"user:cancelled"},
		ToolCancelCoverage:  []ToolCancelCoverage{{Name: "user:cancelled", Area: "user-cancel"}},
		ToolRunStatuses:     []string{"done"},
		ToolRunCoverage:     []ToolRunCoverage{{Name: "done", Area: "terminal-success"}},
		ToolCatalog:         []string{"browser_navigate"},
		ToolCatalogCoverage: []ToolCatalogCoverage{{Name: "browser_navigate", Area: "browser"}},
		StreamJSONMarkers:   []string{"--stream-json"},
		StreamJSONCoverage:  []StreamJSONCoverage{{Name: "--stream-json", Area: "cli-flag"}},
		ModeSettingMarkers:  []string{"reasoning.effort"},
		ModeSettingCoverage: []ModeSettingCoverage{{Name: "reasoning.effort", Area: "thread-setting"}},
		ProviderProtocol:    []string{"anthropic-beta"},
		ProviderCoverage:    []ProviderCoverage{{Name: "anthropic-beta", Area: "anthropic-header"}},
		AgentModeProfiles: []AgentModeProfile{{
			Name:            "smart",
			PrimaryModel:    "CLAUDE_OPUS_4_6",
			ReasoningEffort: "high",
			ReasoningLevels: []string{"high", "max", "xhigh"},
			IncludeTools:    "D6",
			DeferredTools:   true,
			Visible:         true,
			VisibleInV2:     true,
		}},
		AgentModeRoutes: []AgentModeRoute{{
			Name:            "smart",
			Provider:        "anthropic",
			Model:           "claude-opus-4-6",
			PrimaryModel:    "CLAUDE_OPUS_4_6",
			ReasoningEffort: "high",
			ContextWindow:   332000,
			MaxOutputTokens: 32000,
		}},
		AgentModeCoverage: []AgentModeCoverage{{Name: "smart", Scope: "local-runtime"}},
		Settings:          []string{"painter.model"},
		SettingDefaults:   []SettingDefault{{Name: "painter.model", Value: "gpt-image-1", Scope: "local-runtime"}},
		SettingCoverage:   []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		Models:            []string{"gpt-5.4"},
		ModelLimits:       []ModelLimit{{Enum: "GPT_5_4", Name: "gpt-5.4", Provider: "openai", DisplayName: "GPT-5.4", ContextWindow: 272000, MaxOutputTokens: 64000}},
		LargeContextRules: []LargeContextRule{{PrimaryModel: "CLAUDE_OPUS_4_5", Alias: "claude-opus-4-5-1m", ContextWindow: 1000000, MaxOutputTokens: 32000, MaxInputTokens: 968000, RequiresEnableLargeContext: true}},
		AdaptiveThinking:  []AdaptiveThinkingRule{{ModelEnums: []string{"CLAUDE_OPUS_4_6"}, Models: []string{"claude-opus-4-6"}, EffortLevels: []string{"medium", "high"}, DefaultEffort: "medium", ThinkingType: "adaptive", Display: "summarized", UsesOutputConfig: true}},
		ProviderReasoning: []ProviderReasoningRule{{Provider: "anthropic", Sources: []string{"setting:reasoning.effort", "mode:reasoningEffort", "model-default"}, DefaultEffort: "high", SpecialModelEnum: "CLAUDE_OPUS_4_6", SpecialModel: "claude-opus-4-6", SpecialModelEffort: "medium"}},
		ProviderHeaders:   []ProviderHeaderRule{{Provider: "anthropic", FeatureHeader: "x-amp-feature", Feature: "amp.chat", ThreadIDHeader: "x-amp-thread-id", ThreadIDSource: "thread.id", MessageIDHeader: "x-amp-message-id", MessageIDSource: "message-id-argument", BetaHeader: "anthropic-beta", InterleavedBeta: "interleaved-thinking-old", ThinkingEnabledSetting: "anthropic.thinking.enabled", InterleavedThinkingSetting: "anthropic.interleavedThinking.enabled", SkipsAdaptiveThinkingModels: true, OverrideProviderHeader: "x-amp-override-provider", OverrideProviderSetting: "anthropic.provider", FastModeBeta: "fast-mode-old", FastModeSetting: "anthropic.speed", FastModeValue: "fast", FastModeOverrideProvider: "anthropic"}},
		ProviderFeatures:  []ProviderFeatureRule{{Feature: "amp.legacy", Header: "x-amp-feature", Provider: "anthropic", Callsite: "legacy-chat", Default: true}},
		ModelCoverage:     []ModelCoverage{{Name: "gpt-5.4", Provider: "openai", Family: "gpt"}},
		ActorRuntime:      []string{"threadActor"},
		ActorCoverage:     []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		PromptFingerprints: []PromptFingerprint{{
			SHA256: "old",
			Length: 200,
			Tags:   []string{"guidance"},
		}},
		PromptTagCounts: []PromptTagCount{{Tag: "guidance", Count: 1}},
	}}
	current := Snapshot{Source: SourceInfo{SHA256: "new-sha", SizeBytes: 200, Versions: []string{"0.0.2-gnew"}, BuildStamps: []string{"2026-02-01T00:00:00Z"}, StringsScanned: 20}, Signals: Signals{
		Routes:              []string{"/api/thread-actors", "/actors/metadata"},
		RouteMethods:        []RouteMethods{{Name: "/api/thread-actors", Methods: []string{"GET", "POST"}, Scope: "local-runtime"}, {Name: "/actors/metadata", Methods: []string{"GET"}, Scope: "local-runtime"}},
		RouteCoverage:       []RouteCoverage{{Name: "/api/thread-actors", Scope: "local-runtime"}, {Name: "/actors/metadata", Scope: "local-runtime"}},
		ThreadDeltaEvents:   []string{"user:message", "assistant:message-update"},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "assistant:message-update", Area: "assistant-message"}, {Name: "user:message", Area: "user-message"}},
		ToolCancelReasons:   []string{"system:non-terminal-tool-result", "user:interrupted"},
		ToolCancelCoverage:  []ToolCancelCoverage{{Name: "system:non-terminal-tool-result", Area: "restore-cleanup"}, {Name: "user:interrupted", Area: "user-interrupt"}},
		ToolRunStatuses:     []string{"blocked-on-user", "rejected-by-user"},
		ToolRunCoverage:     []ToolRunCoverage{{Name: "blocked-on-user", Area: "pending"}, {Name: "rejected-by-user", Area: "terminal-error"}},
		ToolCatalog:         []string{"browser_take_screenshot", "builtin:edit_file"},
		ToolCatalogCoverage: []ToolCatalogCoverage{{Name: "browser_take_screenshot", Area: "browser"}, {Name: "builtin:edit_file", Area: "file-edit"}},
		StreamJSONMarkers:   []string{"--stream-json", "agent_mode", "error_during_execution"},
		StreamJSONCoverage:  []StreamJSONCoverage{{Name: "--stream-json", Area: "cli-flag"}, {Name: "agent_mode", Area: "init-field"}, {Name: "error_during_execution", Area: "error-subtype"}},
		ModeSettingMarkers:  []string{"draftThreadSettings", "lastSpeedByMode", "reasoning.effort"},
		ModeSettingCoverage: []ModeSettingCoverage{{Name: "draftThreadSettings", Area: "draft-settings"}, {Name: "lastSpeedByMode", Area: "session-default"}, {Name: "reasoning.effort", Area: "thread-setting"}},
		ProviderProtocol:    []string{"anthropic-beta", "interleaved-thinking-2025-05-14", "x-amp-feature"},
		ProviderCoverage:    []ProviderCoverage{{Name: "anthropic-beta", Area: "anthropic-header"}, {Name: "interleaved-thinking-2025-05-14", Area: "anthropic-beta"}, {Name: "x-amp-feature", Area: "amp-provider-header"}},
		AgentModeProfiles: []AgentModeProfile{
			{
				Name:            "deep",
				PrimaryModel:    "GPT_5_5",
				ReasoningEffort: "medium",
				ReasoningLevels: []string{"low", "medium", "xhigh"},
				IncludeTools:    "Ab",
				DeferredTools:   true,
				Visible:         true,
				VisibleInV2:     true,
			},
			{
				Name:            "smart",
				PrimaryModel:    "CLAUDE_OPUS_4_7",
				ReasoningEffort: "high",
				ReasoningLevels: []string{"high", "max", "xhigh"},
				IncludeTools:    "D6",
				DeferredTools:   true,
				Visible:         true,
				VisibleInV2:     true,
			},
		},
		AgentModeRoutes: []AgentModeRoute{
			{
				Name:            "deep",
				Provider:        "openai",
				Model:           "gpt-5.5",
				PrimaryModel:    "GPT_5_5",
				ReasoningEffort: "medium",
				ContextWindow:   400000,
				MaxOutputTokens: 128000,
			},
			{
				Name:            "smart",
				Provider:        "anthropic",
				Model:           "claude-opus-4-7",
				PrimaryModel:    "CLAUDE_OPUS_4_7",
				ReasoningEffort: "high",
				ContextWindow:   332000,
				MaxOutputTokens: 32000,
			},
		},
		AgentModeCoverage: []AgentModeCoverage{{Name: "deep", Scope: "local-runtime"}, {Name: "smart", Scope: "local-runtime"}},
		Settings:          []string{"painter.model", "skills.path"},
		SettingDefaults:   []SettingDefault{{Name: "painter.model", Value: "gpt-image-2", Scope: "local-runtime"}, {Name: "skills.path", Value: "undefined", Scope: "local-runtime"}},
		SettingCoverage:   []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}, {Name: "skills.path", Scope: "local-runtime"}},
		Models:            []string{"amp-nostromo-v1", "gpt-5.5"},
		ModelLimits:       []ModelLimit{{Enum: "GPT_5_5", Name: "gpt-5.5", Provider: "openai", DisplayName: "GPT-5.5", ContextWindow: 400000, MaxOutputTokens: 128000}},
		LargeContextRules: []LargeContextRule{{PrimaryModel: "CLAUDE_OPUS_4_6", Alias: "claude-opus-4-6-1m", ContextWindow: 1000000, MaxOutputTokens: 32000, MaxInputTokens: 968000, RequiresEnableLargeContext: true}},
		AdaptiveThinking:  []AdaptiveThinkingRule{{ModelEnums: []string{"CLAUDE_OPUS_4_6", "CLAUDE_OPUS_4_7", "CLAUDE_OPUS_4_8"}, Models: []string{"claude-opus-4-6", "claude-opus-4-7", "claude-opus-4-8"}, EffortLevels: []string{"low", "medium", "high", "xhigh", "max"}, DefaultEffort: "medium", ThinkingType: "adaptive", Display: "summarized", UsesOutputConfig: true}},
		ProviderReasoning: []ProviderReasoningRule{{Provider: "anthropic", Sources: []string{"setting:reasoning.effort", "mode:reasoningEffort", "model-default"}, DefaultEffort: "high", SpecialModelEnum: "CLAUDE_OPUS_4_7", SpecialModel: "claude-opus-4-7", SpecialModelEffort: "medium"}},
		ProviderHeaders:   []ProviderHeaderRule{{Provider: "anthropic", FeatureHeader: "x-amp-feature", Feature: "amp.chat", ThreadIDHeader: "x-amp-thread-id", ThreadIDSource: "thread.id", MessageIDHeader: "x-amp-message-id", MessageIDSource: "message-id-argument", BetaHeader: "anthropic-beta", InterleavedBeta: "interleaved-thinking-2025-05-14", ThinkingEnabledSetting: "anthropic.thinking.enabled", InterleavedThinkingSetting: "anthropic.interleavedThinking.enabled", SkipsAdaptiveThinkingModels: true, OverrideProviderHeader: "x-amp-override-provider", OverrideProviderSetting: "anthropic.provider", FastModeBeta: "fast-mode-2026-02-01", FastModeSetting: "anthropic.speed", FastModeValue: "fast", FastModeOverrideProvider: "anthropic"}},
		ProviderFeatures: []ProviderFeatureRule{
			{Feature: "amp.chat", Header: "x-amp-feature", Provider: "anthropic", Callsite: "anthropic-chat", Default: true},
			{Feature: "amp.review", Header: "x-amp-feature", Provider: "google", Callsite: "code-review", Tool: "code_review"},
		},
		CompactionRules: []CompactionRule{{
			Name:                     "anthropic-tool-runner",
			Provider:                 "anthropic",
			Trigger:                  "observed-usage",
			Timing:                   "post-response",
			DefaultThresholdTokens:   100000,
			UsageFields:              []string{"input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "output_tokens"},
			SummaryPrompt:            "continuation-summary",
			HistoryReplacementRole:   "user",
			TrailingAssistantToolUse: "strip-tool-use-blocks",
			HelperHeader:             "x-stainless-helper",
			HelperHeaderValue:        "compaction",
		}},
		ModelCoverage: []ModelCoverage{{Name: "amp-nostromo-v1", Provider: "amp", Family: "amp-nostromo"}, {Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorRuntime:  []string{"rvt-token", "threadActor", "threadStatusUpdated"},
		ActorCoverage: []ActorCoverage{{Name: "rvt-token", Area: "actor-protocol"}, {Name: "threadActor", Area: "local-runtime"}, {Name: "threadStatusUpdated", Area: "local-runtime"}},
		PromptFingerprints: []PromptFingerprint{{
			SHA256: "new",
			Length: 220,
			Tags:   []string{"guidance"},
		}},
		PromptTagCounts: []PromptTagCount{{Tag: "guidance", Count: 2}, {Tag: "skills", Count: 1}},
	}}

	diff := diffSnapshots(old, current)

	if !hasDiff(diff) {
		t.Fatal("diff should be detected")
	}
	assertContains(t, diffCategory(t, diff, "binary-source").Added, "sha256=new-sha")
	assertContains(t, diffCategory(t, diff, "binary-source").Added, "size_bytes=200")
	assertContains(t, diffCategory(t, diff, "binary-source").Added, "strings_scanned=20")
	assertContains(t, diffCategory(t, diff, "binary-source").Added, "versions=0.0.2-gnew")
	assertContains(t, diffCategory(t, diff, "binary-source").Added, "build_stamps=2026-02-01T00:00:00Z")
	assertContains(t, diffCategory(t, diff, "binary-source").Removed, "sha256=old-sha")
	assertContains(t, diffCategory(t, diff, "binary-source").Removed, "size_bytes=100")
	assertContains(t, diffCategory(t, diff, "binary-source").Removed, "strings_scanned=10")
	assertContains(t, diffCategory(t, diff, "binary-source").Removed, "versions=0.0.1-gold")
	assertContains(t, diffCategory(t, diff, "binary-source").Removed, "build_stamps=2026-01-01T00:00:00Z")
	assertContains(t, diffCategory(t, diff, "routes").Added, "/actors/metadata")
	assertContains(t, diffCategory(t, diff, "routes").Removed, "/threads")
	assertContains(t, diffCategory(t, diff, "route-methods").Added, "/actors/metadata=GET=local-runtime")
	assertContains(t, diffCategory(t, diff, "route-methods").Added, "/api/thread-actors=GET,POST=local-runtime")
	assertContains(t, diffCategory(t, diff, "route-methods").Removed, "/api/thread-actors=POST=local-runtime")
	assertContains(t, diffCategory(t, diff, "route-methods").Removed, "/threads=GET=amp-owned")
	assertContains(t, diffCategory(t, diff, "route-coverage").Added, "/actors/metadata=local-runtime")
	assertContains(t, diffCategory(t, diff, "route-coverage").Removed, "/threads=amp-owned")
	assertContains(t, diffCategory(t, diff, "thread-delta-events").Added, "assistant:message-update")
	assertContains(t, diffCategory(t, diff, "thread-delta-coverage").Added, "assistant:message-update=assistant-message")
	assertContains(t, diffCategory(t, diff, "tool-cancel-reasons").Added, "system:non-terminal-tool-result")
	assertContains(t, diffCategory(t, diff, "tool-cancel-reasons").Added, "user:interrupted")
	assertContains(t, diffCategory(t, diff, "tool-cancel-reasons").Removed, "user:cancelled")
	assertContains(t, diffCategory(t, diff, "tool-cancel-coverage").Added, "system:non-terminal-tool-result=restore-cleanup")
	assertContains(t, diffCategory(t, diff, "tool-cancel-coverage").Added, "user:interrupted=user-interrupt")
	assertContains(t, diffCategory(t, diff, "tool-cancel-coverage").Removed, "user:cancelled=user-cancel")
	assertContains(t, diffCategory(t, diff, "tool-run-statuses").Added, "blocked-on-user")
	assertContains(t, diffCategory(t, diff, "tool-run-statuses").Added, "rejected-by-user")
	assertContains(t, diffCategory(t, diff, "tool-run-statuses").Removed, "done")
	assertContains(t, diffCategory(t, diff, "tool-run-coverage").Added, "blocked-on-user=pending")
	assertContains(t, diffCategory(t, diff, "tool-run-coverage").Added, "rejected-by-user=terminal-error")
	assertContains(t, diffCategory(t, diff, "tool-run-coverage").Removed, "done=terminal-success")
	assertContains(t, diffCategory(t, diff, "tool-catalog-markers").Added, "browser_take_screenshot")
	assertContains(t, diffCategory(t, diff, "tool-catalog-markers").Added, "builtin:edit_file")
	assertContains(t, diffCategory(t, diff, "tool-catalog-markers").Removed, "browser_navigate")
	assertContains(t, diffCategory(t, diff, "tool-catalog-coverage").Added, "browser_take_screenshot=browser")
	assertContains(t, diffCategory(t, diff, "tool-catalog-coverage").Added, "builtin:edit_file=file-edit")
	assertContains(t, diffCategory(t, diff, "tool-catalog-coverage").Removed, "browser_navigate=browser")
	assertContains(t, diffCategory(t, diff, "stream-json-markers").Added, "agent_mode")
	assertContains(t, diffCategory(t, diff, "stream-json-markers").Added, "error_during_execution")
	assertContains(t, diffCategory(t, diff, "stream-json-coverage").Added, "agent_mode=init-field")
	assertContains(t, diffCategory(t, diff, "stream-json-coverage").Added, "error_during_execution=error-subtype")
	assertContains(t, diffCategory(t, diff, "mode-setting-markers").Added, "draftThreadSettings")
	assertContains(t, diffCategory(t, diff, "mode-setting-markers").Added, "lastSpeedByMode")
	assertContains(t, diffCategory(t, diff, "mode-setting-coverage").Added, "draftThreadSettings=draft-settings")
	assertContains(t, diffCategory(t, diff, "mode-setting-coverage").Added, "lastSpeedByMode=session-default")
	assertContains(t, diffCategory(t, diff, "provider-protocol-markers").Added, "interleaved-thinking-2025-05-14")
	assertContains(t, diffCategory(t, diff, "provider-protocol-markers").Added, "x-amp-feature")
	assertContains(t, diffCategory(t, diff, "provider-protocol-coverage").Added, "interleaved-thinking-2025-05-14=anthropic-beta")
	assertContains(t, diffCategory(t, diff, "provider-protocol-coverage").Added, "x-amp-feature=amp-provider-header")
	assertContains(t, diffCategory(t, diff, "agent-mode-profiles").Added, "deep|primary=GPT_5_5|reasoning=medium|levels=low,medium,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false")
	assertContains(t, diffCategory(t, diff, "agent-mode-profiles").Added, "smart|primary=CLAUDE_OPUS_4_7|reasoning=high|levels=high,max,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false")
	assertContains(t, diffCategory(t, diff, "agent-mode-profiles").Removed, "smart|primary=CLAUDE_OPUS_4_6|reasoning=high|levels=high,max,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false")
	assertContains(t, diffCategory(t, diff, "agent-mode-routes").Added, "deep|provider=openai|model=gpt-5.5|primary=GPT_5_5|reasoning=medium|context=400000|max_out=128000")
	assertContains(t, diffCategory(t, diff, "agent-mode-routes").Added, "smart|provider=anthropic|model=claude-opus-4-7|primary=CLAUDE_OPUS_4_7|reasoning=high|context=332000|max_out=32000")
	assertContains(t, diffCategory(t, diff, "agent-mode-routes").Removed, "smart|provider=anthropic|model=claude-opus-4-6|primary=CLAUDE_OPUS_4_6|reasoning=high|context=332000|max_out=32000")
	assertContains(t, diffCategory(t, diff, "agent-mode-coverage").Added, "deep=local-runtime")
	assertContains(t, diffCategory(t, diff, "settings").Added, "skills.path")
	assertContains(t, diffCategory(t, diff, "setting-defaults").Added, "painter.model=gpt-image-2=local-runtime")
	assertContains(t, diffCategory(t, diff, "setting-defaults").Added, "skills.path=undefined=local-runtime")
	assertContains(t, diffCategory(t, diff, "setting-defaults").Removed, "painter.model=gpt-image-1=local-runtime")
	assertContains(t, diffCategory(t, diff, "setting-coverage").Added, "skills.path=local-runtime")
	assertContains(t, diffCategory(t, diff, "models").Added, "gpt-5.5")
	assertContains(t, diffCategory(t, diff, "models").Added, "amp-nostromo-v1")
	assertContains(t, diffCategory(t, diff, "models").Removed, "gpt-5.4")
	assertContains(t, diffCategory(t, diff, "model-limits").Added, "gpt-5.5|enum=GPT_5_5|provider=openai|display=GPT-5.5|context=400000|max_out=128000")
	assertContains(t, diffCategory(t, diff, "model-limits").Removed, "gpt-5.4|enum=GPT_5_4|provider=openai|display=GPT-5.4|context=272000|max_out=64000")
	assertContains(t, diffCategory(t, diff, "large-context-rules").Added, "CLAUDE_OPUS_4_6|alias=claude-opus-4-6-1m|context=1000000|max_out=32000|max_input=968000|requires_enable=true")
	assertContains(t, diffCategory(t, diff, "large-context-rules").Removed, "CLAUDE_OPUS_4_5|alias=claude-opus-4-5-1m|context=1000000|max_out=32000|max_input=968000|requires_enable=true")
	assertContains(t, diffCategory(t, diff, "adaptive-thinking-rules").Added, "CLAUDE_OPUS_4_6,CLAUDE_OPUS_4_7,CLAUDE_OPUS_4_8|models=claude-opus-4-6,claude-opus-4-7,claude-opus-4-8|levels=low,medium,high,xhigh,max|default=medium|type=adaptive|display=summarized|output_config=true")
	assertContains(t, diffCategory(t, diff, "adaptive-thinking-rules").Removed, "CLAUDE_OPUS_4_6|models=claude-opus-4-6|levels=medium,high|default=medium|type=adaptive|display=summarized|output_config=true")
	assertContains(t, diffCategory(t, diff, "provider-reasoning-rules").Added, "anthropic|sources=setting:reasoning.effort,mode:reasoningEffort,model-default|setting=|default=high|special=CLAUDE_OPUS_4_7/claude-opus-4-7:medium")
	assertContains(t, diffCategory(t, diff, "provider-reasoning-rules").Removed, "anthropic|sources=setting:reasoning.effort,mode:reasoningEffort,model-default|setting=|default=high|special=CLAUDE_OPUS_4_6/claude-opus-4-6:medium")
	assertContains(t, diffCategory(t, diff, "provider-header-rules").Added, "anthropic|feature=x-amp-feature:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-2026-02-01@anthropic.speed=fast=>anthropic")
	assertContains(t, diffCategory(t, diff, "provider-header-rules").Removed, "anthropic|feature=x-amp-feature:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-old@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-old@anthropic.speed=fast=>anthropic")
	assertContains(t, diffCategory(t, diff, "provider-feature-rules").Added, "amp.review|provider=google|callsite=code-review|tool=code_review|header=x-amp-feature|default=false")
	assertContains(t, diffCategory(t, diff, "provider-feature-rules").Removed, "amp.legacy|provider=anthropic|callsite=legacy-chat|tool=|header=x-amp-feature|default=true")
	assertContains(t, diffCategory(t, diff, "compaction-rules").Added, "anthropic-tool-runner|provider=anthropic|trigger=observed-usage|timing=post-response|threshold=100000|usage=input_tokens,cache_creation_input_tokens,cache_read_input_tokens,output_tokens|summary_prompt=continuation-summary|history_role=user|tail_assistant=strip-tool-use-blocks|helper=x-stainless-helper:compaction")
	assertContains(t, diffCategory(t, diff, "model-coverage").Added, "gpt-5.5=openai/gpt")
	assertContains(t, diffCategory(t, diff, "model-coverage").Added, "amp-nostromo-v1=amp/amp-nostromo")
	assertContains(t, diffCategory(t, diff, "model-coverage").Removed, "gpt-5.4=openai/gpt")
	assertContains(t, diffCategory(t, diff, "actor-runtime-markers").Added, "rvt-token")
	assertContains(t, diffCategory(t, diff, "actor-runtime-markers").Added, "threadStatusUpdated")
	assertContains(t, diffCategory(t, diff, "actor-runtime-coverage").Added, "rvt-token=actor-protocol")
	assertContains(t, diffCategory(t, diff, "actor-runtime-coverage").Added, "threadStatusUpdated=local-runtime")
	assertContains(t, diffCategory(t, diff, "prompt-tag-counts").Added, "prompt/guidance=2")
	assertContains(t, diffCategory(t, diff, "prompt-tag-counts").Added, "prompt/skills=1")
	assertContains(t, diffCategory(t, diff, "prompt-tag-counts").Removed, "prompt/guidance=1")
	if len(diff.Prompts.Added) != 1 || diff.Prompts.Added[0].SHA256 != "new" {
		t.Fatalf("prompt added diff = %#v", diff.Prompts.Added)
	}
	if len(diff.Prompts.Removed) != 1 || diff.Prompts.Removed[0].SHA256 != "old" {
		t.Fatalf("prompt removed diff = %#v", diff.Prompts.Removed)
	}
}

func TestDiffSnapshotsReportsPromptKindCounts(t *testing.T) {
	old := Snapshot{Signals: Signals{PromptFingerprints: []PromptFingerprint{{
		SHA256: "old",
		Length: 240,
		Kind:   "prompt",
		Tags:   []string{"guidance"},
	}}}}
	current := Snapshot{Signals: Signals{PromptFingerprints: []PromptFingerprint{
		{
			SHA256: "old",
			Length: 240,
			Kind:   "prompt",
			Tags:   []string{"guidance"},
		},
		{
			SHA256: "source",
			Length: 640,
			Kind:   "source",
			Tags:   []string{"tools"},
		},
	}}}

	diff := diffSnapshots(old, current)

	assertContains(t, diffCategory(t, diff, "prompt-kind-counts").Added, "source=1")
}

func TestDiffSnapshotsReportsPromptFingerprintMetadataDrift(t *testing.T) {
	old := Snapshot{Signals: Signals{PromptFingerprints: []PromptFingerprint{{
		SHA256: "same",
		Length: 240,
		Kind:   "prompt",
		Tags:   []string{"guidance"},
	}}}}
	current := Snapshot{Signals: Signals{PromptFingerprints: []PromptFingerprint{{
		SHA256: "same",
		Length: 260,
		Kind:   "source",
		Tags:   []string{"tools"},
	}}}}

	diff := diffSnapshots(old, current)

	assertContains(t, diffCategory(t, diff, "prompt-fingerprint-metadata").Added, "same|len=260|kind=source|tags=tools")
	assertContains(t, diffCategory(t, diff, "prompt-fingerprint-metadata").Removed, "same|len=240|kind=prompt|tags=guidance")
	if len(diff.Prompts.Added) != 0 || len(diff.Prompts.Removed) != 0 {
		t.Fatalf("prompt hash diff = %#v, want none for same hash", diff.Prompts)
	}
	if !strictAuditFailed(current, diff) {
		t.Fatal("strict audit should fail for prompt fingerprint metadata drift")
	}
}

func TestBuildSnapshotReadsBinaryLikeFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "amp")
	raw := []byte("prefix\x00version 0.0.1780217867-g22770a\x00/api/provider/openai/v1\",{method:\"POST\"}\x00assistant:message type:J.literal(\"message_added\") type:J.literal(\"thread_truncated\") type:J.literal(\"client_append_user_msg\") type:J.literal(\"executor_tool_result\")\x00\"listThreads\" read_messages message_stats\x00user:cancelled\x00case\"done\"\x00browser_navigate builtin:edit_file read_file\x00--stream-json agent_mode\x00reasoning.effort draftThreadSettings\x00anthropic-beta interleaved-thinking-2025-05-14 x-amp-feature amp.chat\x00threadActor threadStatusUpdated rvt-token\x00gpt-5.5 amp-nostromo-v1\x00GPT_5_5:{provider:K.OPENAI,name:\"gpt-5.5\",displayName:\"GPT-5.5\",contextWindow:400000,maxOutputTokens:128000}\x00IOR=\"claude-opus-4-6-1m\",JpT=1e6,QpT=32000;ZB=A9.CLAUDE_OPUS_4_6.name;if(UpT(R)||T?.enableLargeContext&&Sf(R)===ZB)return JpT\x00ZB=A9.CLAUDE_OPUS_4_6.name,YpT=A9.CLAUDE_OPUS_4_7.name,DpT=A9.CLAUDE_OPUS_4_8.name;function FOR(R){let T=Sf(R);return T===ZB||T===YpT||T===DpT}function RLT(R,T){if(FOR(R)){let e=[\"low\",\"medium\",\"high\",\"xhigh\",\"max\"].includes(T.reasoningEffort)?T.reasoningEffort:\"medium\";return{thinking:{type:\"adaptive\",display:\"summarized\"},...{output_config:{effort:e}}}}}\x00function yLT(R,T,e){let[i,r]=R.includes(\"/\")?R.split(\"/\",2):[\"\",R],a=e?ee(e)?.reasoningEffort:void 0,c=lLT(T,e);switch(i){case\"anthropic\":return oLT(c)??a??(r===A9.CLAUDE_OPUS_4_7.name?\"medium\":\"high\");case\"openai\":return sLT(c)??a??\"medium\";case\"vertexai\":return T[\"gemini.thinkingLevel\"]??a??\"medium\";default:return a??\"medium\"}}\x00DEEP:{key:\"deep\",primaryModel:Nr(\"GPT_5_5\"),includeTools:Ab,deferredTools:AD,visible:!0,visibleInV2:!0,reasoningEffort:\"medium\",reasoningEffortControl:{levels:[\"low\",\"medium\",\"xhigh\"]}},SMART:{key:\"smart\",primaryModel:Nr(\"CLAUDE_OPUS_4_7\"),includeTools:D6,visible:!0}\x00updates.mode:{value:\"auto\"}\x00\"painter.model\":{value:\"gpt-image-2\"}\x00")
	raw = append(raw, []byte("Vw=\"x-amp-feature\",QRT=\"x-amp-thread-id\",LU=\"x-amp-message-id\",ART=\"X-Amp-Client-Application\",RTT=\"X-Amp-Client-Type\",TTT=\"X-Amp-Client-Version\"; VpT=\"fast-mode-2026-02-01\"\x00function ZpT(R,T,e,i,r){let a=[];if((R[\"anthropic.thinking.enabled\"]??!0)&&R[\"anthropic.interleavedThinking.enabled\"]&&!FOR(e))a.push(\"interleaved-thinking-2025-05-14\");let c;if(R[\"anthropic.provider\"])c=R[\"anthropic.provider\"];if(XpT(e,R[\"anthropic.speed\"])===\"fast\")a.push(VpT),c=\"anthropic\";return{...OU(Hn()),...a.length>0?{\"anthropic-beta\":a.join(\",\")}:{},...c?{\"x-amp-override-provider\":c}:{},[Vw]:\"amp.chat\",...i!=null?{[LU]:String(i)}:{},...pU(T)}}\x00")...)
	if err := os.WriteFile(path, raw, 0o755); err != nil {
		t.Fatal(err)
	}

	snapshot, err := BuildSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}

	if snapshot.Schema != snapshotSchema {
		t.Fatalf("schema = %d", snapshot.Schema)
	}
	if snapshot.Source.SHA256 == "" {
		t.Fatal("missing source hash")
	}
	assertContains(t, snapshot.Source.Versions, "0.0.1780217867-g22770a")
	assertContains(t, snapshot.Signals.Routes, "/api/provider/openai/v1")
	assertContains(t, routeMethodStrings(snapshot.Signals.RouteMethods), "/api/provider/openai/v1=POST=local-runtime")
	assertContains(t, routeCoverageStrings(snapshot.Signals.RouteCoverage), "/api/provider/openai/v1=local-runtime")
	assertContains(t, snapshot.Signals.ThreadDeltaEvents, "assistant:message")
	assertContains(t, snapshot.Signals.ThreadDeltaEvents, "message_added")
	assertContains(t, snapshot.Signals.ThreadDeltaEvents, "thread_truncated")
	assertContains(t, threadDeltaCoverageStrings(snapshot.Signals.ThreadDeltaCoverage), "assistant:message=assistant-message")
	assertContains(t, threadDeltaCoverageStrings(snapshot.Signals.ThreadDeltaCoverage), "message_added=message")
	assertContains(t, threadDeltaCoverageStrings(snapshot.Signals.ThreadDeltaCoverage), "client_append_user_msg=client-command")
	assertContains(t, threadDeltaCoverageStrings(snapshot.Signals.ThreadDeltaCoverage), "executor_tool_result=executor-bridge")
	assertContains(t, snapshot.Signals.ThreadReaderMarkers, "listThreads")
	assertContains(t, threadReaderCoverageStrings(snapshot.Signals.ThreadReaderCoverage), "read_messages=message-reader-route")
	assertContains(t, snapshot.Signals.ToolCancelReasons, "user:cancelled")
	assertContains(t, toolCancelCoverageStrings(snapshot.Signals.ToolCancelCoverage), "user:cancelled=user-cancel")
	assertContains(t, snapshot.Signals.ToolRunStatuses, "done")
	assertContains(t, toolRunCoverageStrings(snapshot.Signals.ToolRunCoverage), "done=terminal-success")
	assertContains(t, snapshot.Signals.ToolCatalog, "browser_navigate")
	assertContains(t, toolCatalogCoverageStrings(snapshot.Signals.ToolCatalogCoverage), "builtin:edit_file=file-edit")
	assertContains(t, snapshot.Signals.StreamJSONMarkers, "--stream-json")
	assertContains(t, streamJSONCoverageStrings(snapshot.Signals.StreamJSONCoverage), "agent_mode=init-field")
	assertContains(t, snapshot.Signals.ModeSettingMarkers, "reasoningEffort")
	assertNotContains(t, snapshot.Signals.ModeSettingMarkers, "reasoning.effort")
	assertContains(t, modeSettingCoverageStrings(snapshot.Signals.ModeSettingCoverage), "draftThreadSettings=draft-settings")
	assertContains(t, snapshot.Signals.ProviderProtocol, "anthropic-beta")
	assertContains(t, providerCoverageStrings(snapshot.Signals.ProviderCoverage), "interleaved-thinking-2025-05-14=anthropic-beta")
	assertContains(t, providerCoverageStrings(snapshot.Signals.ProviderCoverage), "x-amp-feature=amp-provider-header")
	assertContains(t, agentModeProfileStrings(snapshot.Signals.AgentModeProfiles), "deep|primary=GPT_5_5|reasoning=medium|levels=low,medium,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false")
	assertContains(t, agentModeRouteStrings(snapshot.Signals.AgentModeRoutes), "deep|provider=openai|model=gpt-5.5|primary=GPT_5_5|reasoning=medium|context=400000|max_out=128000")
	assertContains(t, agentModeCoverageStrings(snapshot.Signals.AgentModeCoverage), "smart=local-runtime")
	assertContains(t, snapshot.Signals.Settings, "updates.mode")
	assertContains(t, settingDefaultStrings(snapshot.Signals.SettingDefaults), "updates.mode=auto=amp-owned")
	assertContains(t, settingDefaultStrings(snapshot.Signals.SettingDefaults), "painter.model=gpt-image-2=local-runtime")
	assertContains(t, settingCoverageStrings(snapshot.Signals.SettingCoverage), "updates.mode=amp-owned")
	assertContains(t, snapshot.Signals.Models, "gpt-5.5")
	assertContains(t, modelLimitStrings(snapshot.Signals.ModelLimits), "gpt-5.5|enum=GPT_5_5|provider=openai|display=GPT-5.5|context=400000|max_out=128000")
	assertContains(t, largeContextRuleStrings(snapshot.Signals.LargeContextRules), "CLAUDE_OPUS_4_6|alias=claude-opus-4-6-1m|context=1000000|max_out=32000|max_input=968000|requires_enable=true")
	assertContains(t, adaptiveThinkingRuleStrings(snapshot.Signals.AdaptiveThinking), "CLAUDE_OPUS_4_6,CLAUDE_OPUS_4_7,CLAUDE_OPUS_4_8|models=|levels=low,medium,high,xhigh,max|default=medium|type=adaptive|display=summarized|output_config=true")
	assertContains(t, providerReasoningRuleStrings(snapshot.Signals.ProviderReasoning), "vertexai|sources=setting:gemini.thinkingLevel,mode:reasoningEffort,provider-default|setting=gemini.thinkingLevel|default=medium")
	assertContains(t, providerHeaderRuleStrings(snapshot.Signals.ProviderHeaders), "anthropic|feature=x-amp-feature:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-2026-02-01@anthropic.speed=fast=>anthropic")
	assertContains(t, providerFeatureRuleStrings(snapshot.Signals.ProviderFeatures), "amp.chat|provider=anthropic|callsite=anthropic-chat|tool=|header=x-amp-feature|default=true")
	assertContains(t, snapshot.Signals.Models, "amp-nostromo-v1")
	assertContains(t, modelCoverageStrings(snapshot.Signals.ModelCoverage), "gpt-5.5=openai/gpt")
	assertContains(t, modelCoverageStrings(snapshot.Signals.ModelCoverage), "amp-nostromo-v1=amp/amp-nostromo")
	assertContains(t, snapshot.Signals.ActorRuntime, "threadStatusUpdated")
	assertContains(t, actorCoverageStrings(snapshot.Signals.ActorCoverage), "threadStatusUpdated=local-runtime")
}

func TestExtractAgentModeProfilesStopsAtPuckBoundary(t *testing.T) {
	raw := `REVIEW:{key:"review",primaryModel:Nr("GPT_5_5"),includeTools:NB,reasoningEffort:"medium"},PUCK:{key:"puck",primaryModel:Nr("GPT_5_6_SOL"),includeTools:BB,reasoningEffort:"none",serverOnly:!0},LOW:{key:"low",primaryModel:Nr("AMP_GLM_5_2"),includeTools:VB,reasoningEffort:"medium"}`
	profiles := agentModeProfileStrings(extractAgentModeProfiles(raw))
	assertContains(t, profiles, "review|primary=GPT_5_5|reasoning=medium|levels=|include=present|deferred=false|visible=false|visibleInV2=false|serverOnly=false")
	assertContains(t, profiles, "puck|primary=GPT_5_6_SOL|reasoning=none|levels=|include=present|deferred=false|visible=false|visibleInV2=false|serverOnly=true")
}

func TestLifecycleChecklistMapsChangedSignalsToParityAreas(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		Routes:            []string{"/api/internal", "/api/threads/find?", "/threads"},
		ThreadDeltaEvents: []string{"assistant:message-update", "user:message-queue:enqueue"},
		Settings:          []string{"painter.model", "showCosts", "skills.path"},
		SettingCoverage: []SettingCoverage{
			{Name: "painter.model", Scope: "local-runtime"},
			{Name: "showCosts", Scope: "remote-web"},
			{Name: "skills.path", Scope: "local-runtime"},
		},
		Models: []string{"claude-opus-4-8"},
		PromptFingerprints: []PromptFingerprint{{
			SHA256: "prompt",
			Length: 240,
			Tags:   []string{"code-review", "compaction"},
		}},
	}}
	diff := auditDiff{
		Categories: []categoryDiff{
			{Name: "routes", Added: []string{"/api/internal"}},
			{Name: "thread-delta-events", Added: []string{"user:message-queue:enqueue"}},
			{Name: "thread-reader-markers", Added: []string{"read_messages"}},
			{Name: "tool-cancel-reasons", Added: []string{"user:interrupted"}},
			{Name: "tool-run-statuses", Added: []string{"blocked-on-user"}},
			{Name: "tool-catalog-markers", Added: []string{"builtin:edit_file"}},
			{Name: "stream-json-markers", Added: []string{"error_during_execution"}},
			{Name: "mode-setting-markers", Added: []string{"lastSpeedByMode"}},
			{Name: "provider-protocol-markers", Added: []string{"interleaved-thinking-2025-05-14"}},
			{Name: "agent-mode-profiles", Added: []string{"smart|primary=CLAUDE_OPUS_4_8|reasoning=high|levels=high,max,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false"}},
			{Name: "settings", Added: []string{"painter.model"}},
			{Name: "setting-defaults", Added: []string{"painter.model=gpt-image-2=local-runtime"}},
			{Name: "models", Added: []string{"claude-opus-4-8"}},
			{Name: "model-limits", Added: []string{"claude-opus-4-8|enum=CLAUDE_OPUS_4_8|provider=anthropic|display=Claude Opus 4.8|context=332000|max_out=32000"}},
			{Name: "large-context-rules", Added: []string{"CLAUDE_OPUS_4_6|alias=claude-opus-4-6-1m|context=1000000|max_out=32000|max_input=968000|requires_enable=true"}},
			{Name: "adaptive-thinking-rules", Added: []string{"CLAUDE_OPUS_4_6,CLAUDE_OPUS_4_7,CLAUDE_OPUS_4_8|models=claude-opus-4-6,claude-opus-4-7,claude-opus-4-8|levels=low,medium,high,xhigh,max|default=medium|type=adaptive|display=summarized|output_config=true"}},
			{Name: "provider-reasoning-rules", Added: []string{"vertexai|sources=setting:gemini.thinkingLevel,mode:reasoningEffort,provider-default|setting=gemini.thinkingLevel|default=medium"}},
			{Name: "compaction-rules", Added: []string{"anthropic-tool-runner|provider=anthropic|trigger=observed-usage|timing=post-response|threshold=100000|usage=input_tokens,cache_creation_input_tokens,cache_read_input_tokens,output_tokens|summary_prompt=continuation-summary|history_role=user|tail_assistant=strip-tool-use-blocks|helper=x-stainless-helper:compaction"}},
		},
		Prompts: promptDiff{Added: []PromptFingerprint{{
			SHA256: "prompt",
			Length: 240,
			Tags:   []string{"code-review", "compaction"},
		}}},
	}

	checks := lifecycleChecklist(snapshot, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "actor route and websocket bridge")
	assertContains(t, areas, "thread delta reducer")
	assertContains(t, areas, "queue, steering, and interruption")
	assertContains(t, areas, "tool cancellation and restore cleanup")
	assertContains(t, areas, "tool run status mapping")
	assertContains(t, areas, "stream-json execute lifecycle")
	assertContains(t, areas, "provider protocol headers and betas")
	assertContains(t, areas, "streaming assistant and tool edits")
	assertContains(t, areas, "compaction and continuation prompts")
	assertContains(t, areas, "tools, code review, skills, and images")
	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "upstream-owned thread read and search")
	assertContains(t, areas, "remote web control surface")
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `bun dev/amp-prompt-family-audit.mjs`)
	assertContains(t, lifecycleCheckByArea(t, checks, "tools, code review, skills, and images").Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
	assertContains(t, lifecycleCheckByArea(t, checks, "tools, code review, skills, and images").Commands, `bun dev/amp-prompt-family-audit.mjs`)
}

type lifecycleChecklistDiffCase struct {
	name    string
	value   string
	prompts promptDiff
}

func TestLifecycleChecklistEveryDiffCategoryHasChecklistPath(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		Routes:   []string{"/api/internal", "/api/provider/openai/v1", "/threads"},
		Settings: []string{"painter.model", "showCosts", "skills.path"},
		SettingCoverage: []SettingCoverage{
			{Name: "painter.model", Scope: "local-runtime"},
			{Name: "showCosts", Scope: "remote-web"},
			{Name: "skills.path", Scope: "local-runtime"},
		},
		PromptFingerprints: []PromptFingerprint{{
			SHA256: "prompt",
			Length: 240,
			Kind:   "prompt",
			Tags:   []string{"guidance", "tools"},
		}},
	}}
	cases := []lifecycleChecklistDiffCase{
		{name: "binary-source", value: "version=0.0.9999999999-gabcdef"},
		{name: "cli-command-literals", value: "apps"},
		{name: "cli-control-surfaces", value: "apps=amp-owned"},
		{name: "routes", value: "/api/provider/openai/v1"},
		{name: "route-methods", value: "/metadata=GET=local-runtime"},
		{name: "route-coverage", value: "/api/provider/openai/v1=local-runtime"},
		{name: "thread-delta-events", value: "message_added"},
		{name: "thread-delta-coverage", value: "message_added=message"},
		{name: "thread-reader-markers", value: "read_messages"},
		{name: "thread-reader-coverage", value: "read_messages=message-reader-route"},
		{name: "tool-cancel-reasons", value: "user:interrupted"},
		{name: "tool-cancel-coverage", value: "user:interrupted=user-interrupt"},
		{name: "tool-run-statuses", value: "blocked-on-user"},
		{name: "tool-run-coverage", value: "blocked-on-user=pending"},
		{name: "tool-catalog-markers", value: "builtin:edit_file"},
		{name: "tool-catalog-coverage", value: "builtin:edit_file=file-edit"},
		{name: "stream-json-markers", value: "stream-json"},
		{name: "stream-json-coverage", value: "stream-json=execute-mode"},
		{name: "mode-setting-markers", value: "reasoningEffort"},
		{name: "mode-setting-coverage", value: "reasoningEffort=thread-metadata"},
		{name: "provider-protocol-markers", value: "anthropic-beta"},
		{name: "provider-protocol-coverage", value: "anthropic-beta=anthropic-header"},
		{name: "review-contract-markers", value: "review-cli-appends-check-findings"},
		{name: "agent-mode-profiles", value: "smart|primary=CLAUDE_OPUS_4_7|reasoning=high|levels=high,max,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false"},
		{name: "agent-mode-routes", value: "smart|provider=anthropic|model=claude-opus-4-7|primary=CLAUDE_OPUS_4_7|reasoning=high|context=332000|max_out=32000"},
		{name: "agent-mode-coverage", value: "smart=local-runtime"},
		{name: "settings", value: "painter.model"},
		{name: "setting-defaults", value: "painter.model=gpt-image-2=local-runtime"},
		{name: "setting-coverage", value: "showCosts=remote-web"},
		{name: "models", value: "claude-opus-4-8"},
		{name: "model-limits", value: "claude-opus-4-8|enum=CLAUDE_OPUS_4_8|provider=anthropic|display=Claude Opus 4.8|context=332000|max_out=32000"},
		{name: "large-context-rules", value: "CLAUDE_OPUS_4_6|alias=claude-opus-4-6-1m|context=1000000|max_out=32000|max_input=968000|requires_enable=true"},
		{name: "adaptive-thinking-rules", value: "CLAUDE_OPUS_4_6,CLAUDE_OPUS_4_7,CLAUDE_OPUS_4_8|models=claude-opus-4-6,claude-opus-4-7,claude-opus-4-8|levels=low,medium,high,xhigh,max|default=medium|type=adaptive|display=summarized|output_config=true"},
		{name: "provider-reasoning-rules", value: "vertexai|sources=setting:gemini.thinkingLevel,mode:reasoningEffort,provider-default|setting=gemini.thinkingLevel|default=medium"},
		{name: "provider-header-rules", value: "anthropic|feature=x-amp-feature:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-2026-02-01@anthropic.speed=fast=>anthropic"},
		{name: "provider-feature-rules", value: "amp.read-thread|provider=google|callsite=thread-reader|tool=read_thread|header=x-amp-feature|default=false"},
		{name: "compaction-rules", value: "anthropic-tool-runner|provider=anthropic|trigger=observed-usage|timing=post-response|threshold=100000|usage=input_tokens,cache_creation_input_tokens,cache_read_input_tokens,output_tokens|summary_prompt=continuation-summary|history_role=user|tail_assistant=strip-tool-use-blocks|helper=x-stainless-helper:compaction"},
		{name: "model-coverage", value: "gpt-5.5=openai/gpt"},
		{name: "actor-runtime-markers", value: "threadActor"},
		{name: "actor-runtime-coverage", value: "threadActor=local-runtime"},
		{name: "prompt-fingerprint-metadata", value: "prompt|len=240|kind=prompt|tags=guidance,tools"},
		{name: "prompt-kind-counts", value: "prompt=121"},
		{name: "prompt-tag-counts", value: "prompt/guidance=23"},
		{name: "prompt-fingerprints", prompts: promptDiff{Added: []PromptFingerprint{{
			SHA256: "prompt",
			Length: 240,
			Kind:   "prompt",
			Tags:   []string{"guidance", "tools"},
		}}}},
	}

	diffNames := diffSnapshotCategoryNamesForTest()
	diffNames = append(diffNames, "prompt-fingerprints")
	assertStringSetsEqual(t, "diff category checklist cases", lifecycleChecklistDiffCaseNames(cases), diffNames)

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			diff := auditDiff{Prompts: tt.prompts}
			if tt.value != "" {
				diff.Categories = []categoryDiff{{Name: tt.name, Added: []string{tt.value}}}
			}
			checks := lifecycleChecklist(snapshot, diff, false)
			if len(checks) == 0 {
				t.Fatalf("%s did not trigger any lifecycle checklist area", tt.name)
			}
		})
	}
}

func TestDiffSnapshotsCoversEverySignalField(t *testing.T) {
	signalDiffCategories := map[string][]string{
		"cli_command_literals":       {"cli-command-literals"},
		"cli_control_surfaces":       {"cli-control-surfaces"},
		"routes":                     {"routes"},
		"route_methods":              {"route-methods"},
		"route_coverage":             {"route-coverage"},
		"thread_delta_events":        {"thread-delta-events"},
		"thread_delta_coverage":      {"thread-delta-coverage"},
		"thread_reader_markers":      {"thread-reader-markers"},
		"thread_reader_coverage":     {"thread-reader-coverage"},
		"tool_cancel_reasons":        {"tool-cancel-reasons"},
		"tool_cancel_coverage":       {"tool-cancel-coverage"},
		"tool_run_statuses":          {"tool-run-statuses"},
		"tool_run_coverage":          {"tool-run-coverage"},
		"tool_catalog_markers":       {"tool-catalog-markers"},
		"tool_catalog_coverage":      {"tool-catalog-coverage"},
		"stream_json_markers":        {"stream-json-markers"},
		"stream_json_coverage":       {"stream-json-coverage"},
		"mode_setting_markers":       {"mode-setting-markers"},
		"mode_setting_coverage":      {"mode-setting-coverage"},
		"provider_protocol_markers":  {"provider-protocol-markers"},
		"provider_protocol_coverage": {"provider-protocol-coverage"},
		"review_contract_markers":    {"review-contract-markers"},
		"agent_mode_profiles":        {"agent-mode-profiles"},
		"agent_mode_routes":          {"agent-mode-routes"},
		"agent_mode_coverage":        {"agent-mode-coverage"},
		"settings":                   {"settings"},
		"setting_defaults":           {"setting-defaults"},
		"setting_coverage":           {"setting-coverage"},
		"models":                     {"models"},
		"model_limits":               {"model-limits"},
		"large_context_rules":        {"large-context-rules"},
		"adaptive_thinking_rules":    {"adaptive-thinking-rules"},
		"provider_reasoning_rules":   {"provider-reasoning-rules"},
		"provider_header_rules":      {"provider-header-rules"},
		"provider_feature_rules":     {"provider-feature-rules"},
		"compaction_rules":           {"compaction-rules"},
		"model_coverage":             {"model-coverage"},
		"actor_runtime_markers":      {"actor-runtime-markers"},
		"actor_runtime_coverage":     {"actor-runtime-coverage"},
		"prompt_fingerprints":        {"prompt-fingerprint-metadata", "prompt-kind-counts", "prompt-fingerprints"},
		"prompt_tag_counts":          {"prompt-tag-counts"},
	}

	assertStringSetsEqual(t, "signal field diff mapping", sortedStringSliceMapKeys(signalDiffCategories), signalJSONFieldNamesForTest())

	emittedCategories := sliceSet(diffSnapshotCategoryNamesForTest())
	virtualCategories := map[string]struct{}{
		"prompt-fingerprints": {},
	}
	for field, categories := range signalDiffCategories {
		t.Run(field, func(t *testing.T) {
			if len(categories) == 0 {
				t.Fatal("missing diff category mapping")
			}
			for _, category := range categories {
				if _, ok := emittedCategories[category]; ok {
					continue
				}
				if _, ok := virtualCategories[category]; ok {
					continue
				}
				t.Fatalf("signal field %q maps to non-emitted diff category %q", field, category)
			}
		})
	}
}

func TestLifecycleChecklistCommittedBaselineGuardsCoverEveryDiffCategory(t *testing.T) {
	expected := diffSnapshotCategoryNamesForTest()
	expected = append(expected, "prompt-fingerprints")

	assertStringSetsEqual(t, "committed baseline checklist guard categories", committedBaselineChecklistGuardCategoriesForTest(), expected)
}

func committedBaselineChecklistGuardCategoriesForTest() []string {
	return sortedStrings([]string{
		"binary-source",
		"cli-command-literals",
		"cli-control-surfaces",
		"routes",
		"route-methods",
		"route-coverage",
		"thread-delta-events",
		"thread-delta-coverage",
		"thread-reader-markers",
		"thread-reader-coverage",
		"tool-cancel-reasons",
		"tool-cancel-coverage",
		"tool-run-statuses",
		"tool-run-coverage",
		"tool-catalog-markers",
		"tool-catalog-coverage",
		"stream-json-markers",
		"stream-json-coverage",
		"mode-setting-markers",
		"mode-setting-coverage",
		"provider-protocol-markers",
		"provider-protocol-coverage",
		"review-contract-markers",
		"agent-mode-profiles",
		"agent-mode-routes",
		"agent-mode-coverage",
		"settings",
		"setting-defaults",
		"setting-coverage",
		"models",
		"model-limits",
		"large-context-rules",
		"adaptive-thinking-rules",
		"provider-reasoning-rules",
		"provider-header-rules",
		"provider-feature-rules",
		"compaction-rules",
		"model-coverage",
		"actor-runtime-markers",
		"actor-runtime-coverage",
		"prompt-fingerprint-metadata",
		"prompt-kind-counts",
		"prompt-tag-counts",
		"prompt-fingerprints",
	})
}

func TestLifecycleChecklistTreatsKnownSourceChunksAsFocusedChanges(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		PromptFingerprints: []PromptFingerprint{{
			SHA256: "source",
			Length: 800,
			Kind:   "source",
			Tags:   []string{"skills", "tools"},
		}},
	}}
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "source",
		Length: 800,
		Kind:   "source",
		Tags:   []string{"skills", "tools"},
	}}}}

	areas := lifecycleCheckAreas(lifecycleChecklist(snapshot, diff, false))

	assertContains(t, areas, "tools, code review, skills, and images")
	assertNotContains(t, areas, "compaction and continuation prompts")
	assertNotContains(t, areas, "unknown signal triage")
}

func TestLifecycleChecklistRemoteWebIncludesProductionSmoke(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		Routes: []string{"/api/internal", "/api/attachments"},
	}}
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "routes",
		Added: []string{"/api/attachments"},
	}}}

	checks := lifecycleChecklist(snapshot, diff, false)
	check := lifecycleCheckByArea(t, checks, "remote web control surface")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui check`)
	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
	assertContains(t, check.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, check.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
	assertContains(t, check.Files, "cmd/amp_runtime_drift_scan")
	assertContains(t, check.Files, "dev/neo-remote-ui/server.ts")
	assertContains(t, check.Files, "dev/neo-remote-ui/vite.config.ts")
	assertContains(t, check.Files, "dev/neo-remote-ui/smoke.mjs")
}

func TestLifecycleChecklistRemoteWebRunsForProductionProxyPrefixChanges(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		Routes: []string{"/metadata", "/actors", "/gateway/", "/threads"},
	}}
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "routes",
		Added: []string{"/metadata"},
	}}}

	checks := lifecycleChecklist(snapshot, diff, false)

	check := lifecycleCheckByArea(t, checks, "remote web control surface")
	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
}

func TestLifecycleChecklistThreadLifecycleChangeRunsRemoteWeb(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "thread-delta-events",
		Added: []string{"queued_message_added"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	check := lifecycleCheckByArea(t, checks, "remote web control surface")
	threadDeltaCheck := lifecycleCheckByArea(t, checks, "thread delta reducer")
	queueCheck := lifecycleCheckByArea(t, checks, "queue, steering, and interruption")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
	assertContains(t, threadDeltaCheck.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, threadDeltaCheck.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
	assertContains(t, queueCheck.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, queueCheck.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
}

func TestLifecycleChecklistToolRunStatusChangeRunsRemoteWeb(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "tool-run-statuses",
		Added: []string{"blocked-on-user"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	check := lifecycleCheckByArea(t, checks, "remote web control surface")
	toolRunCheck := lifecycleCheckByArea(t, checks, "tool run status mapping")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
	assertContains(t, toolRunCheck.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, toolRunCheck.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
	assertContains(t, lifecycleCheckAreas(checks), "streaming assistant and tool edits")
}

func TestLifecycleChecklistToolCatalogChangeRunsStreamingChecks(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "tool-catalog-markers",
		Added: []string{"builtin:edit_file"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	check := lifecycleCheckByArea(t, checks, "streaming assistant and tool edits")
	remote := lifecycleCheckByArea(t, checks, "remote web control surface")

	assertContains(t, check.Commands, `go test -count=1 -run 'TestNeoRuntimeWebSocketStreaming|TestInferNeo.*Stream|TestForwardResponsesStream|TestRewriteStreamChunk' ./internal/api/modules/amp ./sdk/api/handlers/openai`)
	assertContains(t, check.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, check.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
	assertContains(t, remote.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
}

func TestLifecycleChecklistToolCancelChangeRunsRemoteWeb(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "tool-cancel-reasons",
		Added: []string{"system:restore-cleanup"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	check := lifecycleCheckByArea(t, checks, "remote web control surface")
	cancelCheck := lifecycleCheckByArea(t, checks, "tool cancellation and restore cleanup")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
	assertContains(t, cancelCheck.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, cancelCheck.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
}

func TestLifecycleChecklistAgentModeChangeRunsRemoteWeb(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "agent-mode-profiles",
		Added: []string{"smart|primary=CLAUDE_OPUS_4_8|reasoning=high|levels=high,max,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "remote web control surface")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
}

func TestLifecycleChecklistReasoningModeChangeRunsRemoteWeb(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "mode-setting-coverage",
		Added: []string{"lastReasoningEffortByMode=session-default"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "remote web control surface")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
}

func TestLifecycleChecklistRemovedThreadRouteRunsThreadReadSearch(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:    "routes",
		Removed: []string{"/api/threads/find?"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "upstream-owned thread read and search")

	assertContains(t, check.Commands, threadReadSearchTestCommand)
}

func TestLifecycleChecklistThreadReaderMarkerChangeRunsThreadReadSearch(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "thread-reader-markers",
		Added: []string{"search_messages"},
	}}}

	areas := lifecycleCheckAreas(lifecycleChecklist(Snapshot{}, diff, false))

	assertContains(t, areas, "upstream-owned thread read and search")
	assertContains(t, areas, "remote web control surface")
}

func TestLifecycleChecklistReadThreadProviderFeatureChangeRunsThreadReadSearch(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "provider-feature-rules",
		Added: []string{"amp.read-thread|provider=google|callsite=thread-reader|tool=read_thread|header=x-amp-feature|default=false"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "upstream-owned thread read and search")

	assertContains(t, check.Commands, threadReadSearchTestCommand)
}

func TestLifecycleChecklistToolSourceChangeRunsThreadReadSearch(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "tool-source",
		Length: 640,
		Kind:   "source",
		Tags:   []string{"tools"},
	}}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "upstream-owned thread read and search")

	assertContains(t, check.Commands, threadReadSearchTestCommand)
}

func TestLifecycleChecklistToolSourceTagCountChangeRunsThreadReadSearch(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "prompt-tag-counts",
		Added: []string{"source/tools=2"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "upstream-owned thread read and search")

	assertContains(t, check.Commands, threadReadSearchTestCommand)
}

func TestLifecycleChecklistRemovedRemoteWebRouteRunsRemoteWeb(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:    "route-methods",
		Removed: []string{"/api/internal=POST=remote-web"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "remote web control surface")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
}

func TestLifecycleChecklistRemovedRemoteWebSettingCoverageRunsRemoteWeb(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:    "setting-coverage",
		Removed: []string{"showCosts=remote-web"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "remote web control surface")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
}

func TestLifecycleChecklistRemovedRemoteWebSettingDefaultRunsRemoteWeb(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:    "setting-defaults",
		Removed: []string{"showCosts=true=remote-web"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "remote web control surface")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
}

func TestLifecycleChecklistRemovedToolSettingRunsToolChecks(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:    "setting-defaults",
		Removed: []string{"skills.path=undefined=local-runtime"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "tools, code review, skills, and images")

	assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
	assertContains(t, check.Commands, `bun dev/amp-prompt-family-audit.mjs`)
}

func TestLifecycleChecklistUnknownSettingDefaultInternalsRunTriage(t *testing.T) {
	cases := map[string]string{
		"local default":  "painter.model=gpt-image-3=local-runtime",
		"remote default": "showCosts=false=remote-web",
		"tool default":   "tools.disable=[\"read_file\"]=local-runtime",
		"scope":          "painter.model=gpt-image-2=remote-web",
		"missing value":  "painter.model==local-runtime",
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "setting-defaults",
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestRouteDiffHelpersMatchRawAndEncodedRoutes(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{
		{Name: "route-methods", Added: []string{"/api/internal?=POST=remote-web"}},
		{Name: "route-coverage", Removed: []string{"/api/threads/find?=amp-owned"}},
		{Name: "route-coverage", Removed: []string{"/actors?actor_ids==local-runtime"}},
	}}

	if !hasAnyRoute([]string{"/api/internal?listThreads"}, "/api/internal") {
		t.Fatal("raw query route did not match prefix")
	}
	if !diffHasAnyRoute(diff, "/api/internal") {
		t.Fatal("encoded route-method diff did not match prefix")
	}
	if !diffHasAnyRoute(diff, "/api/threads") {
		t.Fatal("encoded route-coverage diff did not match parent prefix")
	}
	if !diffHasAnyRoute(diff, "/actors") {
		t.Fatal("encoded route-coverage diff with query equals did not match parent prefix")
	}
	if diffHasAnyRoute(auditDiff{Categories: []categoryDiff{{Name: "routes", Added: []string{"/api/internalized"}}}}, "/api/internal") {
		t.Fatal("route prefix matched unrelated route")
	}
	if diffHasAnyRoute(auditDiff{Categories: []categoryDiff{{Name: "routes", Added: []string{"/api/thread"}}}}, "/api/threads") {
		t.Fatal("route prefix matched partial segment")
	}
}

func TestRouteMethodDiffPartsPreservesQueryEquals(t *testing.T) {
	name, methods, scope := routeMethodDiffParts("/actors?actor_ids==GET=local-runtime")

	if name != "/actors?actor_ids=" || methods != "GET" || scope != "local-runtime" {
		t.Fatalf("route method parts = %q %q %q", name, methods, scope)
	}
}

func TestKnownRouteOwnershipTablesAreInternallyConsistent(t *testing.T) {
	for route := range knownRouteValues {
		t.Run("route/"+route, func(t *testing.T) {
			if scope := routeScope(route); scope == "" {
				t.Fatalf("known route %q has no ownership scope", route)
			}
		})
	}

	for value := range knownRouteMethodValues {
		t.Run("method/"+value, func(t *testing.T) {
			name, methods, scope := routeMethodDiffParts(value)
			if strings.TrimSpace(name) == "" || strings.TrimSpace(methods) == "" || strings.TrimSpace(scope) == "" {
				t.Fatalf("route method value %q parsed as name=%q methods=%q scope=%q", value, name, methods, scope)
			}
			if !knownRouteValue(name) {
				t.Fatalf("route method %q references route %q that is missing from known routes", value, name)
			}
			if expected := routeScope(name); scope != expected {
				t.Fatalf("route method %q scope = %q, want %q", value, scope, expected)
			}
		})
	}
}

func TestKnownLifecycleClassificationTablesAreInternallyConsistent(t *testing.T) {
	for _, event := range []string{"executor_notepad_operation", "executor_notepad_operation_result"} {
		if _, ok := knownRawThreadDeltaEventValues[event]; !ok {
			t.Fatalf("notepad event %q is missing from raw thread deltas", event)
		}
		assertContains(t, knownThreadProtocolEvents, event)
		if area := threadDeltaArea(event); area != "executor-bridge" {
			t.Fatalf("notepad event %q area = %q, want executor-bridge", event, area)
		}
	}
	for event := range knownRawThreadDeltaEventValues {
		t.Run("thread-delta/"+event, func(t *testing.T) {
			if area := threadDeltaArea(event); area == "" {
				t.Fatalf("thread delta %q has no lifecycle area", event)
			}
		})
	}
	for marker := range expectedThreadReaderMarkerValues {
		t.Run("thread-reader/"+marker, func(t *testing.T) {
			if area := threadReaderMarkers[marker]; area == "" {
				t.Fatalf("thread reader marker %q has no area", marker)
			}
		})
	}
	for reason := range knownToolCancelReasonValues {
		t.Run("tool-cancel/"+reason, func(t *testing.T) {
			if area := toolCancelReasonArea(reason); area == "" {
				t.Fatalf("tool cancellation reason %q has no area", reason)
			}
		})
	}
	for status := range knownToolRunStatusValues {
		t.Run("tool-run/"+status, func(t *testing.T) {
			if area := toolRunStatusAreas[status]; area == "" {
				t.Fatalf("tool run status %q has no area", status)
			}
		})
	}
	for marker := range knownToolCatalogMarkerValues {
		t.Run("tool-catalog/"+marker, func(t *testing.T) {
			if area := toolCatalogMarkers[marker]; area == "" {
				t.Fatalf("tool catalog marker %q has no area", marker)
			}
		})
	}
	for marker := range knownActorMarkerValues {
		t.Run("actor/"+marker, func(t *testing.T) {
			if area := actorMarkerAreas[marker]; area == "" {
				t.Fatalf("actor marker %q has no area", marker)
			}
		})
	}
}

func TestAuditModelClassifiersRejectAmpNostromoNearMisses(t *testing.T) {
	for _, model := range []string{"amp-nostromo", "amp-nostromo-v1-preview", "amp-nostromo-v2"} {
		t.Run(model, func(t *testing.T) {
			provider, family := modelProviderAndFamily(model)
			if provider != "unknown" || family != "unknown" {
				t.Fatalf("modelProviderAndFamily(%q) = %s/%s, want unknown/unknown", model, provider, family)
			}
			if knownModelLimitNameShape("amp", model) {
				t.Fatalf("knownModelLimitNameShape accepted amp near miss %q", model)
			}
		})
	}
}

func TestAuditModelPatternRejectsNumericOnlyOSeriesSuffixes(t *testing.T) {
	if got := modelPattern.FindString("o4-1"); got != "" {
		t.Fatalf("modelPattern matched arithmetic token %q", got)
	}
	if got := modelPattern.FindString("o3-mini"); got != "o3-mini" {
		t.Fatalf("modelPattern matched %q, want o3-mini", got)
	}
	for _, model := range []string{"o1-2024-12-17", "o3-2025-04-16", "o3-2025-04-16-preview", "o3-2025-04-16.preview"} {
		if got := modelPattern.FindString(model); got != model {
			t.Fatalf("modelPattern matched %q, want %q", got, model)
		}
	}
}

func TestKnownModelSettingProviderTablesAreInternallyConsistent(t *testing.T) {
	knownScopes := map[string]struct{}{
		"amp-owned":     {},
		"local-runtime": {},
		"remote-web":    {},
	}
	knownAgentModeScopes := map[string]struct{}{
		"local-compatibility": {},
		"local-runtime":       {},
		"server-only":         {},
	}
	for setting, scope := range settingScopes {
		t.Run("setting/"+setting, func(t *testing.T) {
			if _, ok := knownScopes[scope]; !ok {
				t.Fatalf("setting %q scope = %q, want known ownership scope", setting, scope)
			}
		})
	}
	for setting := range knownSettingDefaultValues {
		t.Run("setting-default/"+setting, func(t *testing.T) {
			if settingScopes[setting] == "" {
				t.Fatalf("setting default %q has no ownership scope", setting)
			}
		})
	}
	for mode, scope := range agentModeScopes {
		t.Run("agent-mode/"+mode, func(t *testing.T) {
			if _, ok := knownAgentModeScopes[scope]; !ok {
				t.Fatalf("agent mode %q scope = %q, want known ownership scope", mode, scope)
			}
		})
	}
	for value := range knownAgentModeProfileValues {
		t.Run("agent-mode-profile/"+value, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{Name: "agent-mode-profiles", Added: []string{value}}}}
			if unknown := unknownAgentModeInternalsFromCoverage(diff); len(unknown) > 0 {
				t.Fatalf("known agent mode profile %q produced unknown internals: %#v", value, unknown)
			}
		})
	}
	for value := range knownAgentModeRouteValues {
		t.Run("agent-mode-route/"+value, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{Name: "agent-mode-routes", Added: []string{value}}}}
			if unknown := unknownAgentModeInternalsFromCoverage(diff); len(unknown) > 0 {
				t.Fatalf("known agent mode route %q produced unknown internals: %#v", value, unknown)
			}
		})
	}
	for model := range knownRawModelValues {
		t.Run("raw-model/"+model, func(t *testing.T) {
			provider, family := modelProviderAndFamily(model)
			if provider == "unknown" || family == "unknown" {
				t.Fatalf("raw model %q classified as %s/%s", model, provider, family)
			}
			if !knownModelLimitNameShape(provider, model) {
				t.Fatalf("raw model %q has invalid shape for provider %q", model, provider)
			}
		})
	}
	for name, expected := range knownModelLimitValues {
		t.Run("model-limit/"+name, func(t *testing.T) {
			if !knownModelLimitProvider(expected.Provider) {
				t.Fatalf("model limit %q provider = %q, want known provider", name, expected.Provider)
			}
			if !knownModelLimitEnum(expected.Provider, expected.Enum) {
				t.Fatalf("model limit %q enum = %q, want valid enum for provider %q", name, expected.Enum, expected.Provider)
			}
			if !knownModelLimitNameShape(expected.Provider, name) {
				t.Fatalf("model limit %q has invalid shape for provider %q", name, expected.Provider)
			}
			if strings.TrimSpace(expected.DisplayName) == "" {
				t.Fatalf("model limit %q has empty display name", name)
			}
			if !knownModelLimitContext(expected.ContextWindow) {
				t.Fatalf("model limit %q context = %d, want known context window", name, expected.ContextWindow)
			}
			if !knownModelLimitMaxOutput(expected.MaxOutputTokens) {
				t.Fatalf("model limit %q max output = %d, want known max output", name, expected.MaxOutputTokens)
			}
			if expected.ContextWindow < expected.MaxOutputTokens {
				t.Fatalf("model limit %q context %d is less than max output %d", name, expected.ContextWindow, expected.MaxOutputTokens)
			}
		})
	}
	for category, values := range map[string]map[string]struct{}{
		"provider-reasoning-rules": knownProviderReasoningRuleValues,
		"provider-header-rules":    knownProviderHeaderRuleValues,
		"provider-feature-rules":   knownProviderFeatureRuleValues,
	} {
		for value := range values {
			t.Run(category+"/"+value, func(t *testing.T) {
				diff := auditDiff{Categories: []categoryDiff{{Name: category, Added: []string{value}}}}
				if unknown := unknownProviderProtocolsFromCoverage(diff); len(unknown) > 0 {
					t.Fatalf("known %s value %q produced unknown internals: %#v", category, value, unknown)
				}
			})
		}
	}
}

func TestRouteScopePreservesSpecificOwnershipBeforeBroadPrefixes(t *testing.T) {
	cases := map[string]string{
		"/api/internal":                    "remote-web",
		"/api/internal?listThreads":        "remote-web",
		"/api/internal/github-proxy/repos": "amp-owned",
		"/api/provider/openai/v1":          "local-runtime",
		"/api/threads/find?q=local":        "amp-owned",
		"/threads/T-local":                 "amp-owned",
	}

	for route, want := range cases {
		t.Run(route, func(t *testing.T) {
			if got := routeScope(route); got != want {
				t.Fatalf("routeScope(%q) = %q, want %q", route, got, want)
			}
		})
	}
}

func TestSettingDiffHelpersMatchExactNamesAndScopes(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{
		{Name: "setting-defaults", Removed: []string{"tools.enable=[\"read_file\"]=local-runtime"}},
		{Name: "setting-coverage", Removed: []string{"showCosts=remote-web"}},
	}}

	if !diffHasAnySetting(diff, "tools.enable") {
		t.Fatal("encoded setting default diff did not match exact setting name")
	}
	if diffHasAnySetting(auditDiff{Categories: []categoryDiff{{Name: "setting-defaults", Added: []string{"tools.enabled=true=local-runtime"}}}}, "tools.enable") {
		t.Fatal("setting helper matched partial setting name")
	}
	if !diffHasSettingScope(diff, "remote-web") {
		t.Fatal("setting scope helper did not match removed remote-web coverage")
	}
	if !diffHasSettingScope(auditDiff{Categories: []categoryDiff{{Name: "setting-defaults", Removed: []string{"showCosts=true=remote-web"}}}}, "remote-web") {
		t.Fatal("setting scope helper did not match removed remote-web default")
	}
	if diffHasSettingScope(auditDiff{Categories: []categoryDiff{{Name: "setting-coverage", Added: []string{"showCosts=remote-web-preview"}}}}, "remote-web") {
		t.Fatal("setting scope helper matched partial scope")
	}
}

func TestProviderFeatureDiffHelperMatchesExactFeatures(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:    "provider-feature-rules",
		Removed: []string{"amp.read-thread|provider=google|callsite=thread-reader|tool=read_thread|header=x-amp-feature|default=false"},
	}}}

	if !diffHasAnyProviderFeature(diff, "amp.read-thread") {
		t.Fatal("provider feature helper did not match removed amp.read-thread feature")
	}
	if diffHasAnyProviderFeature(auditDiff{Categories: []categoryDiff{{Name: "provider-feature-rules", Added: []string{"amp.read-thread-preview|provider=google|callsite=thread-reader"}}}}, "amp.read-thread") {
		t.Fatal("provider feature helper matched partial feature")
	}
}

func TestLifecycleChecklistFullModeIncludesBaselineTriage(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		Settings:        []string{"new.setting"},
		SettingCoverage: []SettingCoverage{{Name: "new.setting", Scope: "unknown"}},
	}}

	checks := lifecycleChecklist(snapshot, auditDiff{}, true)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "unknown signal triage")
	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
}

func TestLifecycleChecklistLocalAndRemoteRowsRunRuntimeDriftScan(t *testing.T) {
	checks := lifecycleChecklist(Snapshot{}, auditDiff{}, true)

	for _, check := range checks {
		if check.Scope != "local-runtime" && check.Scope != "remote-web" {
			continue
		}
		assertContains(t, check.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
		assertContains(t, check.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
	}
}

func TestLifecycleChecklistBehaviorChangesRunReleaseWorkflow(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "thread-delta-events",
		Added: []string{"assistant:message-update"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "server build and private release workflow")

	if check.Scope != "release" {
		t.Fatalf("release workflow scope = %q, want release", check.Scope)
	}
	assertContains(t, check.Commands, `bun dev/amp-parity-gate.mjs`)
	assertContains(t, check.Commands, `go build -o /tmp/cliproxyapi-parity-check ./cmd/server`)
	assertContains(t, check.Commands, `git status --short --branch`)
	assertContains(t, check.Commands, `git remote get-url private`)
	assertContains(t, check.Files, "dev/amp-parity-gate.mjs")
}

func TestLifecycleChecklistAgentModeRouteChangeRunsModelRouting(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "agent-mode-routes",
		Added: []string{"smart|provider=anthropic|model=claude-opus-4-8|primary=CLAUDE_OPUS_4_8|reasoning=high|context=332000|max_out=32000"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "compaction and continuation prompts")
	assertContains(t, areas, "streaming assistant and tool edits")
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `bun dev/amp-prompt-family-audit.mjs`)
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `go test -count=1 -run 'TestNeo.*Compaction|TestInferNeo.*Compaction|TestOpenAIResponsesCompact|TestResponsesWebsocketCompaction|TestInputContainsFullTranscriptDetectsCompactionItem' ./internal/api/modules/amp ./sdk/api/handlers/openai`)
}

func TestLifecycleChecklistUnknownAgentModeRouteModelRunsTriage(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "agent-mode-routes",
		Added: []string{"smart|provider=future|model=future-model|primary=FUTURE_MODEL|reasoning=high|context=332000|max_out=32000"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "unknown signal triage")
	assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
}

func TestLifecycleChecklistUnknownAgentModeProfileInternalsRunTriage(t *testing.T) {
	base := "smart|primary=CLAUDE_OPUS_4_7|reasoning=high|levels=high,max,xhigh|include=present|deferred=true|visible=true|visibleInV2=true|serverOnly=false"
	cases := map[string]string{
		"primary":         strings.Replace(base, "primary=CLAUDE_OPUS_4_7", "primary=FUTURE_MODEL", 1),
		"primary exact":   strings.Replace(base, "primary=CLAUDE_OPUS_4_7", "primary=CLAUDE_OPUS_4_6", 1),
		"reasoning":       strings.Replace(base, "reasoning=high", "reasoning=extreme", 1),
		"reasoning exact": strings.Replace(base, "reasoning=high", "reasoning=medium", 1),
		"level":           strings.Replace(base, "levels=high,max,xhigh", "levels=high,extreme", 1),
		"levels exact":    strings.Replace(base, "levels=high,max,xhigh", "levels=high,xhigh", 1),
		"include tools":   strings.Replace(base, "include=present", "include=futureTools", 1),
		"include exact":   strings.Replace(base, "include=present", "include=eH", 1),
		"boolean":         strings.Replace(base, "deferred=true", "deferred=maybe", 1),
		"visibility":      strings.Replace(base, "visibleInV2=true", "visibleInV2=false", 1),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "agent-mode-profiles",
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "model routing, modes, and reasoning")
			assertContains(t, areas, "remote web control surface")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownAgentModeRouteInternalsRunTriage(t *testing.T) {
	base := "large|provider=anthropic|model=claude-opus-4-6|primary=CLAUDE_OPUS_4_6|reasoning=|context=332000|max_out=32000|effective_context=1000000|effective_max_input=968000|large_alias=claude-opus-4-6-1m"
	cases := map[string]string{
		"provider model mismatch": strings.Replace(base, "provider=anthropic", "provider=openai", 1),
		"model":                   strings.Replace(base, "model=claude-opus-4-6", "model=future-model", 1),
		"model exact":             strings.Replace(base, "model=claude-opus-4-6", "model=claude-opus-4-7", 1),
		"primary":                 strings.Replace(base, "primary=CLAUDE_OPUS_4_6", "primary=FUTURE_MODEL", 1),
		"primary exact":           strings.Replace(base, "primary=CLAUDE_OPUS_4_6", "primary=CLAUDE_OPUS_4_7", 1),
		"reasoning":               strings.Replace(base, "reasoning=", "reasoning=extreme", 1),
		"reasoning exact":         strings.Replace(base, "reasoning=", "reasoning=high", 1),
		"context":                 strings.Replace(base, "context=332000", "context=333000", 1),
		"context exact":           strings.Replace(base, "context=332000", "context=200000", 1),
		"max output":              strings.Replace(base, "max_out=32000", "max_out=12345", 1),
		"max output exact":        strings.Replace(base, "max_out=32000", "max_out=64000", 1),
		"effective context":       strings.Replace(base, "effective_context=1000000", "effective_context=1200000", 1),
		"effective max input":     strings.Replace(base, "effective_max_input=968000", "effective_max_input=936000", 1),
		"large alias":             strings.Replace(base, "large_alias=claude-opus-4-6-1m", "large_alias=claude-opus-4-7-1m", 1),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "agent-mode-routes",
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "model routing, modes, and reasoning")
			assertContains(t, areas, "compaction and continuation prompts")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistModelLimitChangeRunsCompactionChecks(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "model-limits",
		Added: []string{"claude-opus-4-8|enum=CLAUDE_OPUS_4_8|provider=anthropic|display=Claude Opus 4.8|context=332000|max_out=32000"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "compaction and continuation prompts")
	assertContains(t, lifecycleCheckByArea(t, checks, "model routing, modes, and reasoning").Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, lifecycleCheckByArea(t, checks, "model routing, modes, and reasoning").Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `bun dev/amp-prompt-family-audit.mjs`)
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `go test -count=1 -run 'TestNeo.*Compaction|TestInferNeo.*Compaction|TestOpenAIResponsesCompact|TestResponsesWebsocketCompaction|TestInputContainsFullTranscriptDetectsCompactionItem' ./internal/api/modules/amp ./sdk/api/handlers/openai`)
}

func TestLifecycleChecklistUnknownModelLimitRunsTriage(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{name: "unknown provider", value: "future-model|enum=FUTURE_MODEL|provider=future|display=Future|context=332000|max_out=32000"},
		{name: "unknown openai family", value: "future-model|enum=FUTURE_MODEL|provider=openai|display=Future|context=332000|max_out=32000"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "model-limits",
				Added: []string{tt.value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "model routing, modes, and reasoning")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownModelLimitInternalsRunTriage(t *testing.T) {
	base := "claude-opus-4-8|enum=CLAUDE_OPUS_4_8|provider=anthropic|display=Claude Opus 4.8|context=332000|max_out=32000"
	cases := map[string]string{
		"enum":                 strings.Replace(base, "enum=CLAUDE_OPUS_4_8", "enum=FUTURE_OPUS_4_8", 1),
		"display":              strings.Replace(base, "display=Claude Opus 4.8", "display=", 1),
		"display exact":        strings.Replace(base, "display=Claude Opus 4.8", "display=Claude Opus Future", 1),
		"context":              strings.Replace(base, "context=332000", "context=333000", 1),
		"context exact":        strings.Replace(base, "context=332000", "context=200000", 1),
		"max output":           strings.Replace(base, "max_out=32000", "max_out=12345", 1),
		"max output exact":     strings.Replace(base, "max_out=32000", "max_out=64000", 1),
		"token math":           strings.Replace(base, "context=332000|max_out=32000", "context=128000|max_out=128001", 1),
		"name shape":           "accounts/other/models/glm-4p6|enum=FIREWORKS_GLM_4P6|provider=fireworks|display=GLM 4P6|context=162752|max_out=40000",
		"new known shape name": "claude-opus-4-9|enum=CLAUDE_OPUS_4_9|provider=anthropic|display=Claude Opus 4.9|context=332000|max_out=32000",
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "model-limits",
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "model routing, modes, and reasoning")
			assertContains(t, areas, "compaction and continuation prompts")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownLargeContextAliasRunsTriage(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "large-context-rules",
		Added: []string{"FUTURE_MODEL|alias=future-model-1m|context=1000000|max_out=32000|max_input=968000|requires_enable=true"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "compaction and continuation prompts")
	assertContains(t, areas, "unknown signal triage")
	assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
}

func TestLifecycleChecklistUnknownLargeContextInternalsRunTriage(t *testing.T) {
	base := "CLAUDE_OPUS_4_6|alias=claude-opus-4-6-1m|context=1000000|max_out=32000|max_input=968000|requires_enable=true"
	cases := map[string]string{
		"primary":         strings.Replace(base, "CLAUDE_OPUS_4_6", "CLAUDE_OPUS_4_7", 1),
		"alias":           strings.Replace(base, "alias=claude-opus-4-6-1m", "alias=claude-opus-4-7-1m", 1),
		"context":         strings.Replace(base, "context=1000000", "context=1200000", 1),
		"max out":         strings.Replace(base, "max_out=32000", "max_out=64000", 1),
		"max input":       strings.Replace(base, "max_input=968000", "max_input=936000", 1),
		"requires enable": strings.Replace(base, "requires_enable=true", "requires_enable=conditional", 1),
		"missing gate":    strings.Replace(base, "|requires_enable=true", "", 1),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "large-context-rules",
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "model routing, modes, and reasoning")
			assertContains(t, areas, "compaction and continuation prompts")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownCompactionRuleInternalsRunTriage(t *testing.T) {
	base := "anthropic-tool-runner|provider=anthropic|trigger=observed-usage|timing=post-response|threshold=100000|usage=input_tokens,cache_creation_input_tokens,cache_read_input_tokens,output_tokens|summary_prompt=continuation-summary|history_role=user|tail_assistant=strip-tool-use-blocks|helper=x-stainless-helper:compaction"
	cases := map[string]string{
		"name":           strings.Replace(base, "anthropic-tool-runner", "future-tool-runner", 1),
		"provider":       strings.Replace(base, "provider=anthropic", "provider=future", 1),
		"trigger":        strings.Replace(base, "trigger=observed-usage", "trigger=preflight", 1),
		"timing":         strings.Replace(base, "timing=post-response", "timing=pre-response", 1),
		"threshold":      strings.Replace(base, "threshold=100000", "threshold=90000", 1),
		"usage":          strings.Replace(base, "output_tokens", "future_tokens", 1),
		"usage exact":    strings.Replace(base, ",output_tokens", "", 1),
		"summary prompt": strings.Replace(base, "summary_prompt=continuation-summary", "summary_prompt=future-summary", 1),
		"history role":   strings.Replace(base, "history_role=user", "history_role=assistant", 1),
		"tail handling":  strings.Replace(base, "tail_assistant=strip-tool-use-blocks", "tail_assistant=keep-tool-use-blocks", 1),
		"helper header":  strings.Replace(base, "helper=x-stainless-helper:compaction", "helper=x-future-helper:compaction", 1),
		"helper value":   strings.Replace(base, "helper=x-stainless-helper:compaction", "helper=x-stainless-helper:future-compaction", 1),
		"helper syntax":  strings.Replace(base, "helper=x-stainless-helper:compaction", "helper=x-stainless-helper", 1),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "compaction-rules",
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "compaction and continuation prompts")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistAdaptiveThinkingChangeRunsProviderAndModelChecks(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "adaptive-thinking-rules",
		Added: []string{"CLAUDE_OPUS_4_6,CLAUDE_OPUS_4_7,CLAUDE_OPUS_4_8|models=claude-opus-4-6,claude-opus-4-7,claude-opus-4-8|levels=low,medium,high,xhigh,max|default=medium|type=adaptive|display=summarized|output_config=true"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "provider protocol headers and betas")
	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "streaming assistant and tool edits")
	assertContains(t, areas, "remote web control surface")
	assertContains(t, lifecycleCheckByArea(t, checks, "streaming assistant and tool edits").Commands, `go test -count=1 -run 'TestNeoRuntimeWebSocketStreaming|TestInferNeo.*Stream|TestForwardResponsesStream|TestRewriteStreamChunk' ./internal/api/modules/amp ./sdk/api/handlers/openai`)
}

func TestLifecycleChecklistUnknownAdaptiveThinkingModelRunsTriage(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "adaptive-thinking-rules",
		Added: []string{"FUTURE_MODEL|models=future-model|levels=medium,high|default=medium|type=adaptive|display=summarized|output_config=true"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "provider protocol headers and betas")
	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "unknown signal triage")
	assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
}

func TestLifecycleChecklistUnknownAdaptiveThinkingInternalsRunTriage(t *testing.T) {
	base := "CLAUDE_OPUS_4_6,CLAUDE_OPUS_4_7,CLAUDE_OPUS_4_8|models=claude-opus-4-6,claude-opus-4-7,claude-opus-4-8|levels=low,medium,high,xhigh,max|default=medium|type=adaptive|display=summarized|output_config=true"
	cases := map[string]string{
		"model set":      strings.Replace(base, "CLAUDE_OPUS_4_6,CLAUDE_OPUS_4_7,CLAUDE_OPUS_4_8|models=claude-opus-4-6,claude-opus-4-7,claude-opus-4-8", "CLAUDE_OPUS_4_8|models=claude-opus-4-8", 1),
		"level":          strings.Replace(base, "levels=low,medium,high,xhigh,max", "levels=low,medium,extreme", 1),
		"level set":      strings.Replace(base, "levels=low,medium,high,xhigh,max", "levels=low,medium,high,xhigh", 1),
		"default":        strings.Replace(base, "default=medium", "default=extreme", 1),
		"type":           strings.Replace(base, "type=adaptive", "type=future-adaptive", 1),
		"display":        strings.Replace(base, "display=summarized", "display=verbose", 1),
		"output config":  strings.Replace(base, "output_config=true", "output_config=false", 1),
		"missing output": strings.Replace(base, "|output_config=true", "", 1),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "adaptive-thinking-rules",
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "provider protocol headers and betas")
			assertContains(t, areas, "model routing, modes, and reasoning")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistProviderReasoningChangeRunsProviderAndModelChecks(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "provider-reasoning-rules",
		Added: []string{"vertexai|sources=setting:gemini.thinkingLevel,mode:reasoningEffort,provider-default|setting=gemini.thinkingLevel|default=medium"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "provider protocol headers and betas")
	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "streaming assistant and tool edits")
	assertContains(t, lifecycleCheckByArea(t, checks, "streaming assistant and tool edits").Commands, `go test -count=1 -run 'TestNeoRuntimeWebSocketStreaming|TestInferNeo.*Stream|TestForwardResponsesStream|TestRewriteStreamChunk' ./internal/api/modules/amp ./sdk/api/handlers/openai`)
}

func TestLifecycleChecklistCommittedModelModeReasoningRulesMapToFocusedChecks(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}

	expected := []struct {
		category string
		values   []string
		areas    []string
	}{
		{
			category: "models",
			values:   baseline.Signals.Models,
			areas:    []string{"model routing, modes, and reasoning", "streaming assistant and tool edits"},
		},
		{
			category: "model-coverage",
			values:   modelCoverageStrings(baseline.Signals.ModelCoverage),
			areas:    []string{"model routing, modes, and reasoning"},
		},
		{
			category: "agent-mode-profiles",
			values:   agentModeProfileStrings(baseline.Signals.AgentModeProfiles),
			areas:    []string{"model routing, modes, and reasoning", "remote web control surface"},
		},
		{
			category: "agent-mode-routes",
			values:   agentModeRouteStrings(baseline.Signals.AgentModeRoutes),
			areas:    []string{"model routing, modes, and reasoning", "compaction and continuation prompts", "streaming assistant and tool edits", "remote web control surface"},
		},
		{
			category: "agent-mode-coverage",
			values:   agentModeCoverageStrings(baseline.Signals.AgentModeCoverage),
			areas:    []string{"model routing, modes, and reasoning", "remote web control surface"},
		},
		{
			category: "model-limits",
			values:   modelLimitStrings(baseline.Signals.ModelLimits),
			areas:    []string{"model routing, modes, and reasoning", "compaction and continuation prompts"},
		},
		{
			category: "large-context-rules",
			values:   largeContextRuleStrings(baseline.Signals.LargeContextRules),
			areas:    []string{"model routing, modes, and reasoning", "compaction and continuation prompts"},
		},
		{
			category: "adaptive-thinking-rules",
			values:   adaptiveThinkingRuleStrings(baseline.Signals.AdaptiveThinking),
			areas:    []string{"provider protocol headers and betas", "model routing, modes, and reasoning", "streaming assistant and tool edits", "remote web control surface"},
		},
		{
			category: "provider-reasoning-rules",
			values:   providerReasoningRuleStrings(baseline.Signals.ProviderReasoning),
			areas:    []string{"provider protocol headers and betas", "model routing, modes, and reasoning", "streaming assistant and tool edits"},
		},
	}

	for _, group := range expected {
		if len(group.values) == 0 {
			if retiredLifecycleCategories[group.category] {
				continue
			}
			t.Fatalf("committed baseline has no %s", group.category)
		}
		for _, value := range group.values {
			value := value
			t.Run(group.category+"/"+value, func(t *testing.T) {
				diff := auditDiff{Categories: []categoryDiff{{
					Name:  group.category,
					Added: []string{value},
				}}}
				areas := lifecycleCheckAreas(lifecycleChecklist(baseline, diff, false))

				for _, area := range group.areas {
					assertContains(t, areas, area)
				}
				assertNotContains(t, areas, "unknown signal triage")
			})
		}
	}
}

func TestLifecycleChecklistUnknownProviderReasoningSpecialModelRunsTriage(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "provider-reasoning-rules",
		Added: []string{"anthropic|sources=setting:reasoning.effort,mode:reasoningEffort,model-default|setting=|default=high|special=FUTURE_MODEL/future-model:medium"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "provider protocol headers and betas")
	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "unknown signal triage")
	assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
}

func TestLifecycleChecklistUnknownProviderReasoningInternalsRunTriage(t *testing.T) {
	base := "anthropic|sources=setting:reasoning.effort,mode:reasoningEffort,model-default|setting=reasoning.effort|default=high|special=CLAUDE_OPUS_4_7/claude-opus-4-7:medium"
	cases := map[string]string{
		"source setting":   strings.Replace(base, "setting:reasoning.effort", "setting:reasoning.futureEffort", 1),
		"source token":     strings.Replace(base, "model-default", "future-default", 1),
		"setting":          strings.Replace(base, "setting=reasoning.effort", "setting=reasoning.futureEffort", 1),
		"default effort":   strings.Replace(base, "default=high", "default=extreme", 1),
		"special effort":   strings.Replace(base, ":medium", ":extreme", 1),
		"special no colon": strings.Replace(base, ":medium", "", 1),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "provider-reasoning-rules",
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "provider protocol headers and betas")
			assertContains(t, areas, "model routing, modes, and reasoning")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownProviderRuleProviderRunsTriage(t *testing.T) {
	cases := map[string]string{
		"provider-reasoning-rules": "future|sources=setting:reasoning.effort,mode:reasoningEffort,provider-default|setting=|default=medium",
		"provider-header-rules":    "future|feature=x-amp-feature:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-future.provider|fast=fast-mode-2026-02-01@future.speed=fast=>future",
		"provider-feature-rules":   "amp.future|provider=future|callsite=future-chat|tool=|header=x-amp-feature|default=false",
	}
	for categoryName, value := range cases {
		t.Run(categoryName, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  categoryName,
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "provider protocol headers and betas")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownProviderRuleFeatureRunsTriage(t *testing.T) {
	cases := map[string]string{
		"provider-header-rules":  "anthropic|feature=x-amp-feature:amp.future|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-2026-02-01@anthropic.speed=fast=>anthropic",
		"provider-feature-rules": "amp.future|provider=google|callsite=future-tool|tool=future_tool|header=x-amp-feature|default=false",
	}
	for categoryName, value := range cases {
		t.Run(categoryName, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  categoryName,
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "provider protocol headers and betas")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownProviderRuleHeaderRunsTriage(t *testing.T) {
	cases := map[string]string{
		"provider-header-rules":  "anthropic|feature=x-amp-future:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-2026-02-01@anthropic.speed=fast=>anthropic",
		"provider-feature-rules": "amp.review|provider=google|callsite=code-review|tool=code_review|header=x-amp-future|default=false",
	}
	for categoryName, value := range cases {
		t.Run(categoryName, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  categoryName,
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "provider protocol headers and betas")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownProviderRuleToolRunsTriage(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "provider-feature-rules",
		Added: []string{"amp.review|provider=google|callsite=code-review|tool=future_tool|header=x-amp-feature|default=false"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "provider protocol headers and betas")
	assertContains(t, areas, "unknown signal triage")
	assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
}

func TestLifecycleChecklistKnownProviderRuleExactDriftRunsTriage(t *testing.T) {
	cases := map[string]string{
		"provider-reasoning-rules": "anthropic|sources=setting:reasoning.effort,mode:reasoningEffort,model-default|setting=|default=medium|special=CLAUDE_OPUS_4_7/claude-opus-4-7:medium",
		"provider-header-rules":    "anthropic|feature=x-amp-feature:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=false|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-2026-02-01@anthropic.speed=fast=>anthropic",
		"provider-feature-rules":   "amp.review|provider=openai|callsite=code-review|tool=code_review|header=x-amp-feature|default=false",
	}
	for categoryName, value := range cases {
		t.Run(categoryName, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  categoryName,
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "provider protocol headers and betas")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownProviderHeaderRuleInternalsRunTriage(t *testing.T) {
	base := "anthropic|feature=x-amp-feature:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-2026-02-01@anthropic.speed=fast=>anthropic"
	cases := map[string]string{
		"thread header":          strings.Replace(base, "thread_id=x-amp-thread-id<-thread.id", "thread_id=x-amp-future-thread-id<-thread.id", 1),
		"message source":         strings.Replace(base, "message_id=x-amp-message-id<-message-id-argument", "message_id=x-amp-message-id<-future-message-id", 1),
		"beta header":            strings.Replace(base, "beta_header=anthropic-beta", "beta_header=anthropic-future-beta", 1),
		"interleaved beta":       strings.Replace(base, "interleaved=interleaved-thinking-2025-05-14@", "interleaved=interleaved-thinking-2026-01-01@", 1),
		"interleaved setting":    strings.Replace(base, "anthropic.thinking.enabled+anthropic.interleavedThinking.enabled", "anthropic.futureThinking.enabled+anthropic.interleavedThinking.enabled", 1),
		"override setting":       strings.Replace(base, "override=x-amp-override-provider<-anthropic.provider", "override=x-amp-override-provider<-anthropic.futureProvider", 1),
		"fast beta":              strings.Replace(base, "fast=fast-mode-2026-02-01@", "fast=fast-mode-2026-06-01@", 1),
		"fast setting":           strings.Replace(base, "fast=fast-mode-2026-02-01@anthropic.speed=", "fast=fast-mode-2026-02-01@anthropic.futureSpeed=", 1),
		"fast value":             strings.Replace(base, "anthropic.speed=fast=>", "anthropic.speed=turbo=>", 1),
		"fast override provider": strings.Replace(base, "=>anthropic", "=>future", 1),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "provider-header-rules",
				Added: []string{value},
			}}}

			checks := lifecycleChecklist(Snapshot{}, diff, false)
			areas := lifecycleCheckAreas(checks)

			assertContains(t, areas, "provider protocol headers and betas")
			assertContains(t, areas, "unknown signal triage")
			assertContains(t, lifecycleCheckByArea(t, checks, "unknown signal triage").Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistProviderHeaderChangeRunsProviderChecks(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "provider-header-rules",
		Added: []string{"anthropic|feature=x-amp-feature:amp.chat|thread_id=x-amp-thread-id<-thread.id|message_id=x-amp-message-id<-message-id-argument|beta_header=anthropic-beta|interleaved=interleaved-thinking-2025-05-14@anthropic.thinking.enabled+anthropic.interleavedThinking.enabled+skip_adaptive=true|override=x-amp-override-provider<-anthropic.provider|fast=fast-mode-2026-02-01@anthropic.speed=fast=>anthropic"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	areas := lifecycleCheckAreas(checks)

	assertContains(t, areas, "provider protocol headers and betas")
	assertContains(t, areas, "compaction and continuation prompts")
	assertContains(t, lifecycleCheckByArea(t, checks, "provider protocol headers and betas").Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, lifecycleCheckByArea(t, checks, "provider protocol headers and betas").Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
	assertContains(t, lifecycleCheckByArea(t, checks, "compaction and continuation prompts").Commands, `go test -count=1 -run 'TestNeo.*Compaction|TestInferNeo.*Compaction|TestOpenAIResponsesCompact|TestResponsesWebsocketCompaction|TestInputContainsFullTranscriptDetectsCompactionItem' ./internal/api/modules/amp ./sdk/api/handlers/openai`)
}

func TestLifecycleChecklistToolProviderFeatureChangesRunToolChecks(t *testing.T) {
	cases := []string{
		"amp.review|provider=google|callsite=code-review|tool=code_review|header=x-amp-feature|default=false",
		"amp.painter|provider=openai|callsite=painter-openai-image|tool=painter|header=x-amp-feature|default=false",
		"amp.image-generation|provider=openai|callsite=openai-image-generation-default|tool=|header=x-amp-feature|default=true",
	}
	for _, rule := range cases {
		feature, _, _ := strings.Cut(rule, "|")
		t.Run(feature, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "provider-feature-rules",
				Added: []string{rule},
			}}}

			check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "tools, code review, skills, and images")

			assertContains(t, check.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
			assertContains(t, check.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
			assertContains(t, check.Commands, `go test -count=1 -run 'TestNeo.*Tool|TestNeo.*CodeReview|TestNeo.*Skill|TestNeo.*Image|TestImages' ./internal/api/modules/amp ./sdk/api/handlers/openai`)
		})
	}
}

func TestLifecycleChecklistCommittedProviderFeatureRulesMapToFocusedChecks(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}
	if len(baseline.Signals.ProviderFeatures) == 0 {
		t.Skip("committed baseline has no active provider feature rules")
	}

	for _, rule := range baseline.Signals.ProviderFeatures {
		rule := rule
		t.Run(rule.Feature+"/"+rule.Provider+"/"+rule.Callsite, func(t *testing.T) {
			values := providerFeatureRuleStrings([]ProviderFeatureRule{rule})
			if len(values) != 1 {
				t.Fatalf("provider feature rule string = %#v, want one value", values)
			}
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "provider-feature-rules",
				Added: values,
			}}}

			areas := lifecycleCheckAreas(lifecycleChecklist(baseline, diff, false))

			for _, area := range []string{
				"provider protocol headers and betas",
				"streaming assistant and tool edits",
				"compaction and continuation prompts",
				"tools, code review, skills, and images",
				"model routing, modes, and reasoning",
			} {
				assertContains(t, areas, area)
			}
			if rule.Feature == "amp.read-thread" {
				assertContains(t, areas, "upstream-owned thread read and search")
			}
			assertNotContains(t, areas, "unknown signal triage")
		})
	}
}

func TestLifecycleChecklistCommittedProviderProtocolAndCompactionRulesMapToFocusedChecks(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}

	expected := []struct {
		category string
		values   []string
		areas    []string
	}{
		{
			category: "provider-protocol-markers",
			values:   baseline.Signals.ProviderProtocol,
			areas:    []string{"provider protocol headers and betas", "streaming assistant and tool edits", "compaction and continuation prompts", "tools, code review, skills, and images", "model routing, modes, and reasoning"},
		},
		{
			category: "provider-protocol-coverage",
			values:   providerCoverageStrings(baseline.Signals.ProviderCoverage),
			areas:    []string{"provider protocol headers and betas", "streaming assistant and tool edits", "compaction and continuation prompts", "tools, code review, skills, and images", "model routing, modes, and reasoning"},
		},
		{
			category: "provider-header-rules",
			values:   providerHeaderRuleStrings(baseline.Signals.ProviderHeaders),
			areas:    []string{"provider protocol headers and betas", "streaming assistant and tool edits", "compaction and continuation prompts", "tools, code review, skills, and images", "model routing, modes, and reasoning"},
		},
		{
			category: "compaction-rules",
			values:   compactionRuleStrings(baseline.Signals.CompactionRules),
			areas:    []string{"compaction and continuation prompts"},
		},
	}

	for _, group := range expected {
		if len(group.values) == 0 {
			if retiredLifecycleCategories[group.category] {
				continue
			}
			t.Fatalf("committed baseline has no %s", group.category)
		}
		for _, value := range group.values {
			value := value
			t.Run(group.category+"/"+value, func(t *testing.T) {
				diff := auditDiff{Categories: []categoryDiff{{
					Name:  group.category,
					Added: []string{value},
				}}}
				areas := lifecycleCheckAreas(lifecycleChecklist(baseline, diff, false))

				for _, area := range group.areas {
					assertContains(t, areas, area)
				}
				assertNotContains(t, areas, "unknown signal triage")
			})
		}
	}
}

func TestLifecycleChecklistGoTestCommandsMatchExistingTests(t *testing.T) {
	checks := lifecycleChecklist(Snapshot{}, auditDiff{}, true)
	namesByPattern := map[string][]string{}

	for _, check := range checks {
		for _, command := range check.Commands {
			runPattern, packagePatterns, ok := parseLifecycleGoTestCommandForTest(command)
			if !ok {
				continue
			}
			re, err := regexp.Compile(runPattern)
			if err != nil {
				t.Fatalf("%s command has invalid -run pattern %q: %v", check.Area, runPattern, err)
			}
			var matches []string
			matchesByAlternative := map[string][]string{}
			for _, alternative := range splitLifecycleRunAlternativesForTest(runPattern) {
				matchesByAlternative[alternative] = nil
			}
			for _, packagePattern := range packagePatterns {
				names, exists := namesByPattern[packagePattern]
				if !exists {
					names = lifecycleTestNamesForPackagePattern(t, packagePattern)
					namesByPattern[packagePattern] = names
				}
				for _, name := range names {
					if re.MatchString(name) {
						matches = append(matches, packagePattern+":"+name)
					}
					for alternative := range matchesByAlternative {
						alternativeRe, errAlt := regexp.Compile(alternative)
						if errAlt != nil {
							t.Fatalf("%s command has invalid -run alternative %q: %v", check.Area, alternative, errAlt)
						}
						if alternativeRe.MatchString(name) {
							matchesByAlternative[alternative] = append(matchesByAlternative[alternative], packagePattern+":"+name)
						}
					}
				}
			}
			if len(matches) == 0 {
				t.Fatalf("%s command matches no tests: %s", check.Area, command)
			}
			for alternative, alternativeMatches := range matchesByAlternative {
				if len(alternativeMatches) == 0 {
					t.Fatalf("%s command alternative %q matches no tests: %s", check.Area, alternative, command)
				}
			}
		}
	}
}

func TestLifecycleChecklistNonGoTestCommandsResolve(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	checks := lifecycleChecklist(Snapshot{}, auditDiff{}, true)

	for _, check := range checks {
		for _, command := range check.Commands {
			if _, _, ok := parseLifecycleGoTestCommandForTest(command); ok {
				continue
			}
			if target, ok := parseLifecycleGoRunCommandForTest(command); ok {
				targetDir := filepath.Join(root, strings.TrimPrefix(target, "./"))
				if info, err := os.Stat(targetDir); err != nil || !info.IsDir() {
					t.Fatalf("%s command targets missing go run directory %q: %s", check.Area, target, command)
				}
				matches, err := filepath.Glob(filepath.Join(targetDir, "*.go"))
				if err != nil || len(matches) == 0 {
					t.Fatalf("%s command targets go run directory without Go files %q: %s", check.Area, target, command)
				}
				continue
			}
			if target, ok := parseLifecycleGoBuildCommandForTest(command); ok {
				targetDir := filepath.Join(root, strings.TrimPrefix(target, "./"))
				if info, err := os.Stat(targetDir); err != nil || !info.IsDir() {
					t.Fatalf("%s command targets missing go build directory %q: %s", check.Area, target, command)
				}
				continue
			}
			if cwd, script, ok := parseLifecycleBunRunCommandForTest(command); ok {
				scripts := lifecyclePackageScriptsForTest(t, filepath.Join(root, cwd, "package.json"))
				if strings.TrimSpace(scripts[script]) == "" {
					t.Fatalf("%s command references missing bun script %q in %s: %s", check.Area, script, cwd, command)
				}
				continue
			}
			if target, ok := parseLifecycleBunScriptCommandForTest(command); ok {
				if info, err := os.Stat(filepath.Join(root, target)); err != nil || info.IsDir() {
					t.Fatalf("%s command targets missing bun script %q: %s", check.Area, target, command)
				}
				continue
			}
			if isLifecycleGitCommandForTest(command) {
				continue
			}
			t.Fatalf("%s command uses an unverified command shape: %s", check.Area, command)
		}
	}
}

func TestLifecycleChecklistFilesExist(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	checks := lifecycleChecklist(Snapshot{}, auditDiff{}, true)

	for _, check := range checks {
		for _, file := range check.Files {
			path := filepath.Join(root, file)
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("%s checklist references missing path %q: %v", check.Area, file, err)
			}
		}
	}
}

func TestLifecycleChecklistBinarySourceDriftRunsFullParityChecklist(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:    "binary-source",
		Added:   []string{"sha256=new"},
		Removed: []string{"sha256=old"},
	}}}

	areas := lifecycleCheckAreas(lifecycleChecklist(Snapshot{}, diff, false))

	if len(areas) < 12 {
		t.Fatalf("checklist areas = %#v, want full parity checklist", areas)
	}
	for _, check := range lifecycleChecklist(Snapshot{}, diff, false) {
		if check.Trigger != "Amp binary source changed; run the full parity review even though extracted lifecycle signals did not change" {
			t.Fatalf("check %q trigger = %q", check.Area, check.Trigger)
		}
	}
	assertContains(t, areas, "actor route and websocket bridge")
	assertContains(t, areas, "thread delta reducer")
	assertContains(t, areas, "queue, steering, and interruption")
	assertContains(t, areas, "tool cancellation and restore cleanup")
	assertContains(t, areas, "tool run status mapping")
	assertContains(t, areas, "stream-json execute lifecycle")
	assertContains(t, areas, "provider protocol headers and betas")
	assertContains(t, areas, "streaming assistant and tool edits")
	assertContains(t, areas, "compaction and continuation prompts")
	assertContains(t, areas, "tools, code review, skills, and images")
	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "upstream-owned thread read and search")
	assertContains(t, areas, "remote web control surface")
}

func TestLifecycleChecklistCoverageOnlyChangesRunFocusedChecks(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		PromptFingerprints: []PromptFingerprint{{
			SHA256: "prompt",
			Length: 240,
			Tags:   []string{"compaction", "skills"},
		}},
	}}
	diff := auditDiff{Categories: []categoryDiff{
		{Name: "actor-runtime-coverage", Added: []string{"threadStatusUpdated=local-runtime"}},
		{Name: "thread-delta-coverage", Added: []string{"assistant:message-update=assistant-message"}},
		{Name: "tool-cancel-coverage", Added: []string{"user:interrupted=user-interrupt"}},
		{Name: "tool-run-coverage", Added: []string{"blocked-on-user=pending"}},
		{Name: "prompt-tag-counts", Added: []string{"prompt/compaction=13", "prompt/skills=17"}},
	}}

	areas := lifecycleCheckAreas(lifecycleChecklist(snapshot, diff, false))

	assertContains(t, areas, "actor route and websocket bridge")
	assertContains(t, areas, "remote web control surface")
	assertContains(t, areas, "thread delta reducer")
	assertContains(t, areas, "queue, steering, and interruption")
	assertContains(t, areas, "streaming assistant and tool edits")
	assertContains(t, areas, "tool cancellation and restore cleanup")
	assertContains(t, areas, "tool run status mapping")
	assertContains(t, areas, "tools, code review, skills, and images")
	assertContains(t, areas, "compaction and continuation prompts")
}

func TestLifecycleChecklistCommittedThreadToolLifecycleCoverageMapsToFocusedChecks(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}

	expected := []struct {
		category string
		values   []string
		areas    []string
	}{
		{
			category: "thread-delta-events",
			values:   baseline.Signals.ThreadDeltaEvents,
			areas:    []string{"thread delta reducer", "queue, steering, and interruption", "streaming assistant and tool edits", "remote web control surface"},
		},
		{
			category: "thread-delta-coverage",
			values:   threadDeltaCoverageStrings(baseline.Signals.ThreadDeltaCoverage),
			areas:    []string{"thread delta reducer", "queue, steering, and interruption", "streaming assistant and tool edits", "remote web control surface"},
		},
		{
			category: "tool-cancel-reasons",
			values:   baseline.Signals.ToolCancelReasons,
			areas:    []string{"tool cancellation and restore cleanup", "streaming assistant and tool edits", "tools, code review, skills, and images", "remote web control surface"},
		},
		{
			category: "tool-cancel-coverage",
			values:   toolCancelCoverageStrings(baseline.Signals.ToolCancelCoverage),
			areas:    []string{"tool cancellation and restore cleanup", "streaming assistant and tool edits", "tools, code review, skills, and images", "remote web control surface"},
		},
		{
			category: "tool-run-statuses",
			values:   baseline.Signals.ToolRunStatuses,
			areas:    []string{"tool run status mapping", "streaming assistant and tool edits", "tools, code review, skills, and images", "remote web control surface"},
		},
		{
			category: "tool-run-coverage",
			values:   toolRunCoverageStrings(baseline.Signals.ToolRunCoverage),
			areas:    []string{"tool run status mapping", "streaming assistant and tool edits", "tools, code review, skills, and images", "remote web control surface"},
		},
		{
			category: "stream-json-markers",
			values:   baseline.Signals.StreamJSONMarkers,
			areas:    []string{"stream-json execute lifecycle"},
		},
		{
			category: "stream-json-coverage",
			values:   streamJSONCoverageStrings(baseline.Signals.StreamJSONCoverage),
			areas:    []string{"stream-json execute lifecycle"},
		},
	}

	for _, group := range expected {
		if len(group.values) == 0 {
			if retiredLifecycleCategories[group.category] {
				continue
			}
			t.Fatalf("committed baseline has no %s", group.category)
		}
		for _, value := range group.values {
			value := value
			t.Run(group.category+"/"+value, func(t *testing.T) {
				diff := auditDiff{Categories: []categoryDiff{{
					Name:  group.category,
					Added: []string{value},
				}}}
				areas := lifecycleCheckAreas(lifecycleChecklist(baseline, diff, false))

				for _, area := range group.areas {
					assertContains(t, areas, area)
				}
				assertNotContains(t, areas, "unknown signal triage")
			})
		}
	}
}

func TestLifecycleChecklistCommittedRouteSettingsActorToolCatalogCoverageMapsToFocusedChecks(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}

	expected := []struct {
		category string
		values   []string
		areas    []string
	}{
		{
			category: "routes",
			values:   baseline.Signals.Routes,
			areas:    []string{"actor route and websocket bridge", "streaming assistant and tool edits", "upstream-owned thread read and search", "remote web control surface"},
		},
		{
			category: "route-methods",
			values:   routeMethodStrings(baseline.Signals.RouteMethods),
			areas:    []string{"actor route and websocket bridge", "streaming assistant and tool edits", "upstream-owned thread read and search", "remote web control surface"},
		},
		{
			category: "route-coverage",
			values:   routeCoverageStrings(baseline.Signals.RouteCoverage),
			areas:    []string{"actor route and websocket bridge", "streaming assistant and tool edits", "upstream-owned thread read and search", "remote web control surface"},
		},
		{
			category: "thread-reader-markers",
			values:   baseline.Signals.ThreadReaderMarkers,
			areas:    []string{"upstream-owned thread read and search", "remote web control surface"},
		},
		{
			category: "thread-reader-coverage",
			values:   threadReaderCoverageStrings(baseline.Signals.ThreadReaderCoverage),
			areas:    []string{"upstream-owned thread read and search", "remote web control surface"},
		},
		{
			category: "tool-catalog-markers",
			values:   baseline.Signals.ToolCatalog,
			areas:    []string{"streaming assistant and tool edits", "tools, code review, skills, and images", "remote web control surface"},
		},
		{
			category: "tool-catalog-coverage",
			values:   toolCatalogCoverageStrings(baseline.Signals.ToolCatalogCoverage),
			areas:    []string{"streaming assistant and tool edits", "tools, code review, skills, and images", "remote web control surface"},
		},
		{
			category: "settings",
			values:   baseline.Signals.Settings,
			areas:    []string{"model routing, modes, and reasoning", "streaming assistant and tool edits", "tools, code review, skills, and images", "remote web control surface"},
		},
		{
			category: "setting-defaults",
			values:   settingDefaultStrings(baseline.Signals.SettingDefaults),
			areas:    []string{"model routing, modes, and reasoning", "tools, code review, skills, and images", "remote web control surface"},
		},
		{
			category: "setting-coverage",
			values:   settingCoverageStrings(baseline.Signals.SettingCoverage),
			areas:    []string{"model routing, modes, and reasoning", "tools, code review, skills, and images", "remote web control surface"},
		},
		{
			category: "mode-setting-markers",
			values:   baseline.Signals.ModeSettingMarkers,
			areas:    []string{"model routing, modes, and reasoning", "remote web control surface"},
		},
		{
			category: "mode-setting-coverage",
			values:   modeSettingCoverageStrings(baseline.Signals.ModeSettingCoverage),
			areas:    []string{"model routing, modes, and reasoning", "remote web control surface"},
		},
		{
			category: "actor-runtime-markers",
			values:   baseline.Signals.ActorRuntime,
			areas:    []string{"actor route and websocket bridge", "remote web control surface"},
		},
		{
			category: "actor-runtime-coverage",
			values:   actorCoverageStrings(baseline.Signals.ActorCoverage),
			areas:    []string{"actor route and websocket bridge", "remote web control surface"},
		},
	}

	for _, group := range expected {
		if len(group.values) == 0 {
			if retiredLifecycleCategories[group.category] {
				continue
			}
			t.Fatalf("committed baseline has no %s", group.category)
		}
		for _, value := range group.values {
			value := value
			t.Run(group.category+"/"+value, func(t *testing.T) {
				diff := auditDiff{Categories: []categoryDiff{{
					Name:  group.category,
					Added: []string{value},
				}}}
				areas := lifecycleCheckAreas(lifecycleChecklist(baseline, diff, false))

				for _, area := range group.areas {
					assertContains(t, areas, area)
				}
				assertNotContains(t, areas, "unknown signal triage")
			})
		}
	}
}

func TestLifecycleChecklistSourceTagCountChangesRunFocusedChecks(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name: "prompt-tag-counts",
		Added: []string{
			fmt.Sprintf("source/skills=%d", knownPromptTagCountValues["source/skills"]),
			fmt.Sprintf("source/compaction=%d", knownPromptTagCountValues["source/compaction"]),
		},
	}}}

	areas := lifecycleCheckAreas(lifecycleChecklist(Snapshot{}, diff, false))

	assertContains(t, areas, "tools, code review, skills, and images")
	assertContains(t, areas, "compaction and continuation prompts")
	assertNotContains(t, areas, "unknown signal triage")
}

func TestLifecycleChecklistCommittedPromptTagCountsMapToFocusedChecks(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}
	expectedAreas := map[string][]string{
		"prompt/artifacts":     {"remote web control surface"},
		"prompt/code-review":   {"tools, code review, skills, and images"},
		"prompt/compaction":    {"compaction and continuation prompts"},
		"prompt/guidance":      {"streaming assistant and tool edits", "compaction and continuation prompts"},
		"prompt/painter":       {"tools, code review, skills, and images"},
		"prompt/settings":      {"model routing, modes, and reasoning", "remote web control surface"},
		"prompt/skills":        {"tools, code review, skills, and images"},
		"prompt/system-prompt": {"streaming assistant and tool edits", "compaction and continuation prompts"},
		"prompt/tools":         {"streaming assistant and tool edits", "tools, code review, skills, and images"},
		"source/artifacts":     {"remote web control surface"},
		"source/code-review":   {"tools, code review, skills, and images"},
		"source/compaction":    {"compaction and continuation prompts"},
		"source/guidance":      {"streaming assistant and tool edits", "compaction and continuation prompts"},
		"source/painter":       {"tools, code review, skills, and images"},
		"source/settings":      {"model routing, modes, and reasoning", "remote web control surface"},
		"source/skills":        {"tools, code review, skills, and images"},
		"source/system-prompt": {"streaming assistant and tool edits", "compaction and continuation prompts"},
		"source/tools":         {"streaming assistant and tool edits", "tools, code review, skills, and images", "remote web control surface"},
	}
	checked := 0
	for _, count := range baseline.Signals.PromptTagCounts {
		key := promptTagCountKey(count.Kind, count.Tag)
		wantAreas, ok := expectedAreas[key]
		if !ok {
			t.Fatalf("prompt/source tag count %q has no focused checklist expectation", key)
		}
		checked++
		t.Run(key, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "prompt-tag-counts",
				Added: []string{fmt.Sprintf("%s=%d", key, count.Count)},
			}}}
			areas := lifecycleCheckAreas(lifecycleChecklist(baseline, diff, false))

			for _, area := range wantAreas {
				assertContains(t, areas, area)
			}
			assertNotContains(t, areas, "unknown signal triage")
		})
	}
	if checked != len(expectedAreas) {
		t.Fatalf("checked %d prompt/source tag counts, want %d", checked, len(expectedAreas))
	}
}

func TestLifecycleChecklistCommittedPromptFingerprintsMapToFocusedChecks(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}
	if len(baseline.Signals.PromptFingerprints) == 0 {
		t.Fatal("committed baseline has no prompt/source fingerprints")
	}

	for _, fingerprint := range baseline.Signals.PromptFingerprints {
		fingerprint := fingerprint
		name := promptKindOrDefault(fingerprint.Kind) + "/" + shortHash(fingerprint.SHA256) + "/" + strings.Join(fingerprint.Tags, ",")
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{fingerprint}}}
			areas := lifecycleCheckAreas(lifecycleChecklist(baseline, diff, false))
			wantAreas := expectedPromptFingerprintChecklistAreasForTest(fingerprint)
			if len(wantAreas) == 0 {
				t.Fatalf("prompt/source fingerprint %s has no focused checklist expectation", name)
			}

			for _, area := range wantAreas {
				assertContains(t, areas, area)
			}
			assertNotContains(t, areas, "unknown signal triage")
		})
	}
}

func TestLifecycleChecklistCommittedPromptFingerprintMetadataMapsToFocusedChecks(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}
	if len(baseline.Signals.PromptFingerprints) == 0 {
		t.Fatal("committed baseline has no prompt/source fingerprints")
	}

	for _, fingerprint := range baseline.Signals.PromptFingerprints {
		fingerprint := fingerprint
		metadata := promptFingerprintMetadataStrings([]PromptFingerprint{fingerprint})
		if len(metadata) != 1 {
			t.Fatalf("prompt/source fingerprint metadata for %s = %#v, want one value", fingerprint.SHA256, metadata)
		}
		name := promptKindOrDefault(fingerprint.Kind) + "/" + shortHash(fingerprint.SHA256) + "/" + strings.Join(fingerprint.Tags, ",")
		t.Run(name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "prompt-fingerprint-metadata",
				Added: metadata,
			}}}
			areas := lifecycleCheckAreas(lifecycleChecklist(baseline, diff, false))
			wantAreas := expectedPromptFingerprintChecklistAreasForTest(fingerprint)
			if len(wantAreas) == 0 {
				t.Fatalf("prompt/source fingerprint metadata %s has no focused checklist expectation", name)
			}

			for _, area := range wantAreas {
				assertContains(t, areas, area)
			}
			assertNotContains(t, areas, "unknown signal triage")
		})
	}
}

func expectedPromptFingerprintChecklistAreasForTest(fingerprint PromptFingerprint) []string {
	areas := map[string]struct{}{}
	add := func(values ...string) {
		for _, value := range values {
			areas[value] = struct{}{}
		}
	}
	for _, tag := range fingerprint.Tags {
		switch tag {
		case "artifacts":
			add("remote web control surface")
		case "code-review", "painter", "skills":
			add("tools, code review, skills, and images")
		case "compaction":
			add("compaction and continuation prompts")
		case "guidance", "system-prompt":
			add("streaming assistant and tool edits", "compaction and continuation prompts")
		case "settings":
			add("model routing, modes, and reasoning", "remote web control surface")
		case "tools":
			add("streaming assistant and tool edits", "tools, code review, skills, and images", "upstream-owned thread read and search", "remote web control surface")
		}
	}
	return sortedKeys(areas)
}

func TestLifecycleChecklistToolPromptSourceChangeRunsStreamingChecks(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "tool-source",
		Length: 640,
		Kind:   "source",
		Tags:   []string{"tools"},
	}}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "streaming assistant and tool edits")

	assertContains(t, check.Commands, `go test -count=1 -run 'TestNeoRuntimeWebSocketStreaming|TestInferNeo.*Stream|TestForwardResponsesStream|TestRewriteStreamChunk' ./internal/api/modules/amp ./sdk/api/handlers/openai`)
}

func TestLifecycleChecklistToolPromptSourceChangeRunsRemoteWeb(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "tool-source",
		Length: 640,
		Kind:   "source",
		Tags:   []string{"tools"},
	}}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "remote web control surface")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
}

func TestLifecycleChecklistGuidanceTagCountChangeRunsStreamingChecks(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "prompt-tag-counts",
		Added: []string{"prompt/guidance=22"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "streaming assistant and tool edits")

	assertContains(t, check.Commands, `go test -count=1 -run 'TestNeoRuntimeWebSocketStreaming|TestInferNeo.*Stream|TestForwardResponsesStream|TestRewriteStreamChunk' ./internal/api/modules/amp ./sdk/api/handlers/openai`)
}

func TestLifecycleChecklistSourceSettingChangeRunsSettingsAndRemoteWeb(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "settings-source",
		Length: 640,
		Kind:   "source",
		Tags:   []string{"settings"},
	}}}}

	areas := lifecycleCheckAreas(lifecycleChecklist(Snapshot{}, diff, false))

	assertContains(t, areas, "model routing, modes, and reasoning")
	assertContains(t, areas, "remote web control surface")
	assertNotContains(t, areas, "unknown signal triage")
}

func TestLifecycleChecklistSourceArtifactChangeRunsRemoteWeb(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "artifact-source",
		Length: 640,
		Kind:   "source",
		Tags:   []string{"artifacts"},
	}}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "remote web control surface")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
}

func TestLifecycleChecklistModelCoverageChangeRunsModelRouting(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "model-coverage",
		Added: []string{"claude-opus-4-8=anthropic/claude-opus"},
	}}}

	areas := lifecycleCheckAreas(lifecycleChecklist(Snapshot{}, diff, false))

	assertContains(t, areas, "model routing, modes, and reasoning")
}

func TestLifecycleChecklistRemoteWebRunsForSettingCoverageChanges(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		SettingCoverage: []SettingCoverage{{Name: "showCosts", Scope: "remote-web"}},
	}}
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "setting-coverage",
		Added: []string{"showCosts=remote-web"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(snapshot, diff, false), "remote web control surface")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
}

func TestLifecycleChecklistRemoteWebRunsForActorRuntimeMarkerChanges(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "actor-runtime-markers",
		Added: []string{"threadStatusUpdated"},
	}}}

	checks := lifecycleChecklist(Snapshot{}, diff, false)
	check := lifecycleCheckByArea(t, checks, "remote web control surface")
	actorCheck := lifecycleCheckByArea(t, checks, "actor route and websocket bridge")

	assertContains(t, check.Commands, `bun run --cwd dev/neo-remote-ui smoke`)
	assertContains(t, actorCheck.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -summary`)
	assertContains(t, actorCheck.Commands, `go run ./cmd/amp_runtime_drift_scan -since-homebrew-runtime -json`)
}

func TestLifecycleChecklistEveryDiffCategoryHasAReviewPath(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		Routes: []string{"/api/internal", "/api/attachments", "/actors", "/gateway", "/metadata", "/threads", "/api/threads/find"},
		Settings: []string{
			"painter.model",
			"skills.path",
			"showCosts",
		},
		SettingCoverage: []SettingCoverage{{Name: "showCosts", Scope: "remote-web"}},
	}}
	for _, category := range diffSnapshots(Snapshot{}, Snapshot{}).Categories {
		t.Run(category.Name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  category.Name,
				Added: []string{sampleDiffValue(category.Name)},
			}}}

			if checks := lifecycleChecklist(snapshot, diff, false); len(checks) == 0 {
				t.Fatalf("category %q produced no lifecycle checks", category.Name)
			}
		})
	}
}

func TestLifecycleChecklistEveryRemovedDiffCategoryHasAReviewPath(t *testing.T) {
	for _, category := range diffSnapshots(Snapshot{}, Snapshot{}).Categories {
		t.Run(category.Name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:    category.Name,
				Removed: []string{sampleDiffValue(category.Name)},
			}}}

			if checks := lifecycleChecklist(Snapshot{}, diff, false); len(checks) == 0 {
				t.Fatalf("removed category %q produced no lifecycle checks", category.Name)
			}
		})
	}
}

func TestLifecycleChecklistUntaggedPromptDiffRunsTriage(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "prompt",
		Length: 240,
		Kind:   "prompt",
	}}}}

	areas := lifecycleCheckAreas(lifecycleChecklist(Snapshot{}, diff, false))

	assertContains(t, areas, "unknown signal triage")
}

func TestLifecycleChecklistUnknownPromptKindRunsTriage(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "future",
		Length: 240,
		Kind:   "future-kind",
		Tags:   []string{"guidance"},
	}}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

	assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
}

func TestLifecycleChecklistUnknownPromptTagRunsTriage(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "prompt",
		Length: 240,
		Kind:   "prompt",
		Tags:   []string{"future-prompt-surface"},
	}}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

	assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
}

func TestLifecycleChecklistUnknownSourceTagRunsTriage(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "source",
		Length: 800,
		Kind:   "source",
		Tags:   []string{"future-source-surface"},
	}}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

	assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
}

func TestLifecycleChecklistKnownSourceTagsDoNotRunTriage(t *testing.T) {
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "source",
		Length: 800,
		Kind:   "source",
		Tags:   []string{"artifacts", "settings"},
	}}}}

	areas := lifecycleCheckAreas(lifecycleChecklist(Snapshot{}, diff, false))

	assertNotContains(t, areas, "unknown signal triage")
}

func TestLifecycleChecklistUnknownSourceTagCountRunsTriage(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "prompt-tag-counts",
		Added: []string{"source/future-source-surface=1"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

	assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
}

func TestLifecycleChecklistUnknownPromptTagCountRunsTriage(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "prompt-tag-counts",
		Added: []string{"prompt/future-prompt-surface=1"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

	assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
}

func TestLifecycleChecklistUnknownPromptTagCountKindRunsTriage(t *testing.T) {
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "prompt-tag-counts",
		Added: []string{"future-kind/guidance=1"},
	}}}

	check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

	assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
}

func TestLifecycleChecklistPromptTagCountValueDriftRunsTriage(t *testing.T) {
	cases := []struct {
		name     string
		category categoryDiff
	}{
		{name: "added unexpected count", category: categoryDiff{Name: "prompt-tag-counts", Added: []string{"prompt/guidance=23"}}},
		{name: "removed expected count", category: categoryDiff{Name: "prompt-tag-counts", Removed: []string{"source/tools=200"}}},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{tt.category}}

			check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

			assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
		})
	}
}

func TestLifecycleChecklistPromptKindCountValueDriftRunsTriage(t *testing.T) {
	cases := []struct {
		name     string
		category categoryDiff
	}{
		{name: "added unexpected count", category: categoryDiff{Name: "prompt-kind-counts", Added: []string{"prompt=121"}}},
		{name: "removed expected count", category: categoryDiff{Name: "prompt-kind-counts", Removed: []string{"source=241"}}},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{tt.category}}

			check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

			assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistCommittedPromptTagCountDriftRunsTriage(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}
	values := promptTagCountStrings(baseline.Signals.PromptTagCounts)
	if len(values) == 0 {
		t.Fatal("committed baseline has no prompt/source tag counts")
	}

	for _, value := range values {
		value := value
		t.Run("removed/"+value, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:    "prompt-tag-counts",
				Removed: []string{value},
			}}}
			check := lifecycleCheckByArea(t, lifecycleChecklist(baseline, diff, false), "unknown signal triage")

			assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
		})
		t.Run("incremented/"+value, func(t *testing.T) {
			key, count, ok := promptTagCountValueParts(value)
			if !ok {
				t.Fatalf("prompt/source tag count value %q did not parse", value)
			}
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "prompt-tag-counts",
				Added: []string{fmt.Sprintf("%s=%d", key, count+1)},
			}}}
			check := lifecycleCheckByArea(t, lifecycleChecklist(baseline, diff, false), "unknown signal triage")

			assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -prompt-diff-excerpts`)
		})
	}
}

func TestLifecycleChecklistCommittedPromptKindCountDriftRunsTriage(t *testing.T) {
	root := repoRootForAuditChecklistTest(t)
	baseline, err := readSnapshotFile(filepath.Join(root, "dev", "amp-binary-parity-baseline.json"))
	if err != nil {
		t.Fatalf("read committed baseline: %v", err)
	}
	values := promptKindCountStrings(baseline.Signals.PromptFingerprints)
	if len(values) == 0 {
		t.Fatal("committed baseline has no prompt/source kind counts")
	}

	for _, value := range values {
		value := value
		t.Run("removed/"+value, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:    "prompt-kind-counts",
				Removed: []string{value},
			}}}
			check := lifecycleCheckByArea(t, lifecycleChecklist(baseline, diff, false), "unknown signal triage")

			assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
		t.Run("incremented/"+value, func(t *testing.T) {
			kind, count, ok := promptKindCountValueParts(value)
			if !ok {
				t.Fatalf("prompt/source kind count value %q did not parse", value)
			}
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  "prompt-kind-counts",
				Added: []string{fmt.Sprintf("%s=%d", kind, count+1)},
			}}}
			check := lifecycleCheckByArea(t, lifecycleChecklist(baseline, diff, false), "unknown signal triage")

			assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownCoverageDiffRunsTriage(t *testing.T) {
	cases := []struct {
		name     string
		category string
		value    string
	}{
		{name: "route coverage", category: "route-coverage", value: "/future=unknown"},
		{name: "route exact prefix", category: "routes", value: "/api/provider/new"},
		{name: "route coverage exact prefix", category: "route-coverage", value: "/api/provider/new=local-runtime"},
		{name: "route method coverage", category: "route-methods", value: "/future=POST=unknown"},
		{name: "thread delta coverage", category: "thread-delta-coverage", value: "future:event=unknown"},
		{name: "tool cancel coverage", category: "tool-cancel-coverage", value: "future-cancel=unknown"},
		{name: "tool run coverage", category: "tool-run-coverage", value: "future-status=unknown"},
		{name: "tool catalog coverage", category: "tool-catalog-coverage", value: "future_tool=unknown"},
		{name: "stream json coverage", category: "stream-json-coverage", value: "future_stream_field=unknown"},
		{name: "mode setting coverage", category: "mode-setting-coverage", value: "futureSetting=unknown"},
		{name: "provider protocol coverage", category: "provider-protocol-coverage", value: "future-provider-header=unknown"},
		{name: "agent mode coverage", category: "agent-mode-coverage", value: "future-mode=unknown"},
		{name: "setting coverage", category: "setting-coverage", value: "future.setting=unknown"},
		{name: "setting defaults", category: "setting-defaults", value: "future.setting=value=unknown"},
		{name: "model coverage", category: "model-coverage", value: "future-model=unknown/unknown"},
		{name: "actor coverage", category: "actor-runtime-coverage", value: "futureActor=unknown"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  tt.category,
				Added: []string{tt.value},
			}}}

			check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

			if !strings.Contains(check.Trigger, "unknown route") || !strings.Contains(check.Trigger, "prompt/source taxonomy") {
				t.Fatalf("unknown triage trigger = %q", check.Trigger)
			}
			assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -checklist`)

			removed := auditDiff{Categories: []categoryDiff{{
				Name:    tt.category,
				Removed: []string{tt.value},
			}}}

			removedCheck := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, removed, false), "unknown signal triage")

			assertContains(t, removedCheck.Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnexpectedCoverageAreaRunsTriage(t *testing.T) {
	cases := []struct {
		name     string
		category string
		value    string
	}{
		{name: "thread delta area", category: "thread-delta-coverage", value: "assistant:message-update=queue"},
		{name: "thread delta familiar prefix", category: "thread-delta-events", value: "client_future_command"},
		{name: "tool cancel area", category: "tool-cancel-coverage", value: "user:interrupted=system-safety"},
		{name: "tool run area", category: "tool-run-coverage", value: "done=running"},
		{name: "tool catalog area", category: "tool-catalog-coverage", value: "builtin:edit_file=file-read"},
		{name: "tool catalog exact marker", category: "tool-catalog-markers", value: "applyPatchFreeform"},
		{name: "tool catalog exact coverage", category: "tool-catalog-coverage", value: "applyPatchFreeform=file-edit"},
		{name: "stream json area", category: "stream-json-coverage", value: "agent_mode=result-field"},
		{name: "mode setting area", category: "mode-setting-coverage", value: "reasoningEffort=provider-speed"},
		{name: "route method verbs", category: "route-methods", value: "/api/internal=GET=remote-web"},
		{name: "route method query verbs", category: "route-methods", value: "/actors?actor_ids==POST=local-runtime"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  tt.category,
				Added: []string{tt.value},
			}}}

			check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

			assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistOwnershipCoverageMismatchRunsTriage(t *testing.T) {
	cases := []struct {
		name     string
		category string
		value    string
	}{
		{name: "route coverage", category: "route-coverage", value: "/api/provider=amp-owned"},
		{name: "route method coverage", category: "route-methods", value: "/api/internal=GET=amp-owned"},
		{name: "setting coverage", category: "setting-coverage", value: "painter.model=amp-owned"},
		{name: "provider protocol coverage", category: "provider-protocol-coverage", value: "x-amp-feature=amp-client-header"},
		{name: "agent mode coverage", category: "agent-mode-coverage", value: "deep=server-only"},
		{name: "model coverage provider", category: "model-coverage", value: "gpt-5.5=anthropic/gpt"},
		{name: "model coverage family", category: "model-coverage", value: "claude-opus-4-8=anthropic/claude-sonnet"},
		{name: "actor coverage", category: "actor-runtime-coverage", value: "threadActor=amp-owned"},
		{name: "actor exact marker", category: "actor-runtime-markers", value: "durable-thread-workers"},
		{name: "actor exact coverage", category: "actor-runtime-coverage", value: "durable-thread-workers=amp-owned"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  tt.category,
				Added: []string{tt.value},
			}}}

			check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

			assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestLifecycleChecklistUnknownRawSignalDiffRunsTriage(t *testing.T) {
	cases := []struct {
		name     string
		category string
		value    string
	}{
		{name: "setting", category: "settings", value: "future.setting"},
		{name: "route", category: "routes", value: "/future"},
		{name: "thread delta", category: "thread-delta-events", value: "future:event"},
		{name: "tool cancel", category: "tool-cancel-reasons", value: "future-cancel"},
		{name: "tool run status", category: "tool-run-statuses", value: "future-status"},
		{name: "tool catalog marker", category: "tool-catalog-markers", value: "future_tool"},
		{name: "stream json marker", category: "stream-json-markers", value: "future_stream_field"},
		{name: "mode setting marker", category: "mode-setting-markers", value: "futureSetting"},
		{name: "provider protocol marker", category: "provider-protocol-markers", value: "future-provider-header"},
		{name: "agent mode profile", category: "agent-mode-profiles", value: "future-mode|primary=FUTURE|reasoning=high"},
		{name: "agent mode route", category: "agent-mode-routes", value: "future-mode|provider=anthropic|model=future-model"},
		{name: "model", category: "models", value: "future-model"},
		{name: "actor marker", category: "actor-runtime-markers", value: "futureActor"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			diff := auditDiff{Categories: []categoryDiff{{
				Name:  tt.category,
				Added: []string{tt.value},
			}}}

			check := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, diff, false), "unknown signal triage")

			assertContains(t, check.Commands, `go run ./cmd/amp_binary_audit -checklist`)

			removed := auditDiff{Categories: []categoryDiff{{
				Name:    tt.category,
				Removed: []string{tt.value},
			}}}

			removedCheck := lifecycleCheckByArea(t, lifecycleChecklist(Snapshot{}, removed, false), "unknown signal triage")

			assertContains(t, removedCheck.Commands, `go run ./cmd/amp_binary_audit -checklist`)
		})
	}
}

func TestStrictAuditFailedRequiresClassifiedOwnership(t *testing.T) {
	clean := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if strictAuditFailed(clean, auditDiff{}) {
		t.Fatal("strict audit failed for classified snapshot without diffs")
	}

	unknownRoute := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/new-surface", Scope: "unknown"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownRoute, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown route ownership")
	}

	unknownSetting := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "new.setting", Scope: "unknown"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownSetting, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown setting ownership")
	}

	unknownModel := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "future-model-1", Provider: "unknown", Family: "unknown"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownModel, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown model provider")
	}

	unknownActor := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "futureActorToken", Area: "unknown"}},
	}}
	if !strictAuditFailed(unknownActor, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown actor marker area")
	}

	unknownThreadDelta := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "workspace:replace", Area: "unknown"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownThreadDelta, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown thread delta lifecycle area")
	}

	unknownToolCancel := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		ToolCancelCoverage:  []ToolCancelCoverage{{Name: "system:future-cleanup", Area: "unknown"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownToolCancel, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown tool cancellation reason area")
	}

	unknownToolRun := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		ToolCancelCoverage:  []ToolCancelCoverage{{Name: "user:cancelled", Area: "user-cancel"}},
		ToolRunCoverage:     []ToolRunCoverage{{Name: "future-status", Area: "unknown"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownToolRun, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown tool run status area")
	}

	unknownToolCatalog := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		ToolCancelCoverage:  []ToolCancelCoverage{{Name: "user:cancelled", Area: "user-cancel"}},
		ToolRunCoverage:     []ToolRunCoverage{{Name: "done", Area: "terminal-success"}},
		ToolCatalogCoverage: []ToolCatalogCoverage{{Name: "future_tool", Area: "unknown"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownToolCatalog, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown tool catalog marker area")
	}

	unknownStreamJSON := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		ToolCancelCoverage:  []ToolCancelCoverage{{Name: "user:cancelled", Area: "user-cancel"}},
		ToolRunCoverage:     []ToolRunCoverage{{Name: "done", Area: "terminal-success"}},
		StreamJSONCoverage:  []StreamJSONCoverage{{Name: "future_stream_field", Area: "unknown"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownStreamJSON, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown stream-json marker area")
	}

	unknownModeSetting := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		ToolCancelCoverage:  []ToolCancelCoverage{{Name: "user:cancelled", Area: "user-cancel"}},
		ToolRunCoverage:     []ToolRunCoverage{{Name: "done", Area: "terminal-success"}},
		StreamJSONCoverage:  []StreamJSONCoverage{{Name: "--stream-json", Area: "cli-flag"}},
		ModeSettingCoverage: []ModeSettingCoverage{{Name: "futureModeDefault", Area: "unknown"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownModeSetting, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown mode setting marker area")
	}

	unknownProviderProtocol := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		ProviderCoverage:    []ProviderCoverage{{Name: "future-provider-header", Area: "unknown"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownProviderProtocol, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown provider protocol marker area")
	}

	unknownAgentMode := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		AgentModeCoverage:   []AgentModeCoverage{{Name: "frontier", Scope: "unknown"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	if !strictAuditFailed(unknownAgentMode, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown agent mode")
	}

	unknownPromptTag := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		PromptTagCounts:     []PromptTagCount{{Kind: "prompt", Tag: "future-prompt-surface", Count: 1}},
	}}
	if !strictAuditFailed(unknownPromptTag, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown prompt tags")
	}

	unknownSourceTag := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		PromptTagCounts:     []PromptTagCount{{Kind: "source", Tag: "future-source-surface", Count: 1}},
	}}
	if !strictAuditFailed(unknownSourceTag, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown source tags")
	}

	unknownPromptKind := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		PromptFingerprints:  []PromptFingerprint{{SHA256: "future", Kind: "future-kind", Tags: []string{"guidance"}}},
		PromptTagCounts:     []PromptTagCount{{Kind: "future-kind", Tag: "guidance", Count: 1}},
	}}
	if !strictAuditFailed(unknownPromptKind, auditDiff{}) {
		t.Fatal("strict audit should fail for unknown prompt kinds")
	}

	unexpectedPromptTagCount := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		PromptTagCounts:     []PromptTagCount{{Kind: "prompt", Tag: "guidance", Count: 23}},
	}}
	if !strictAuditFailed(unexpectedPromptTagCount, auditDiff{}) {
		t.Fatal("strict audit should fail for unexpected prompt tag counts")
	}

	unexpectedPromptKindCount := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		PromptFingerprints:  []PromptFingerprint{{SHA256: "prompt", Kind: "prompt", Tags: []string{"guidance"}}},
	}}
	if !strictAuditFailed(unexpectedPromptKindCount, auditDiff{}) {
		t.Fatal("strict audit should fail for unexpected prompt kind counts")
	}

	diff := auditDiff{Categories: []categoryDiff{{Name: "routes", Added: []string{"/api/provider/new"}}}}
	if !strictAuditFailed(clean, diff) {
		t.Fatal("strict audit should fail for signal diffs")
	}
}

func TestStrictAuditFailedOnBinarySourceDrift(t *testing.T) {
	clean := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	diff := auditDiff{Categories: []categoryDiff{{
		Name:    "binary-source",
		Added:   []string{"sha256=new"},
		Removed: []string{"sha256=old"},
	}}}
	if !strictAuditFailed(clean, diff) {
		t.Fatal("strict audit should fail for binary source drift")
	}
}

func TestStrictAuditFailedOnPromptFingerprintDrift(t *testing.T) {
	clean := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "prompt",
		Length: 240,
		Kind:   "prompt",
		Tags:   []string{"guidance"},
	}}}}
	if !strictAuditFailed(clean, diff) {
		t.Fatal("strict audit should fail for prompt fingerprint drift")
	}
}

func TestStrictAuditFailedOnSourceFingerprintDrift(t *testing.T) {
	clean := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "source",
		Length: 800,
		Kind:   "source",
		Tags:   []string{"tools"},
	}}}}
	if !strictAuditFailed(clean, diff) {
		t.Fatal("strict audit should fail for source fingerprint drift")
	}
}

func TestBaselineWriteProblemsRejectsUnknownCoverageByDefault(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/new-surface", Scope: "unknown"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "workspace:replace", Area: "unknown"}},
		ToolCancelCoverage:  []ToolCancelCoverage{{Name: "system:future-cleanup", Area: "unknown"}},
		ToolRunCoverage:     []ToolRunCoverage{{Name: "future-status", Area: "unknown"}},
		ToolCatalogCoverage: []ToolCatalogCoverage{{Name: "future_tool", Area: "unknown"}},
		StreamJSONCoverage:  []StreamJSONCoverage{{Name: "future_stream_field", Area: "unknown"}},
		ModeSettingCoverage: []ModeSettingCoverage{{Name: "futureModeDefault", Area: "unknown"}},
		ProviderCoverage:    []ProviderCoverage{{Name: "future-provider-header", Area: "unknown"}},
		AgentModeCoverage:   []AgentModeCoverage{{Name: "frontier", Scope: "unknown"}},
		SettingCoverage:     []SettingCoverage{{Name: "new.setting", Scope: "unknown"}},
		ModelCoverage:       []ModelCoverage{{Name: "future-model-1", Provider: "unknown", Family: "unknown"}},
		ActorCoverage:       []ActorCoverage{{Name: "futureActorToken", Area: "unknown"}},
	}}

	problems := baselineWriteProblems(snapshot, auditDiff{}, false, false)

	assertContains(t, problems, "unknown routes: /api/new-surface")
	assertContains(t, problems, "unknown thread deltas: workspace:replace")
	assertContains(t, problems, "unknown tool cancellation reasons: system:future-cleanup")
	assertContains(t, problems, "unknown tool run statuses: future-status")
	assertContains(t, problems, "unknown tool catalog markers: future_tool")
	assertContains(t, problems, "unknown stream-json markers: future_stream_field")
	assertContains(t, problems, "unknown mode setting markers: futureModeDefault")
	assertContains(t, problems, "unknown provider protocol markers: future-provider-header")
	assertContains(t, problems, "unknown agent modes: frontier")
	assertContains(t, problems, "unknown settings: new.setting")
	assertContains(t, problems, "unknown models: future-model-1")
	assertContains(t, problems, "unknown actor markers: futureActorToken")
	if allowed := baselineWriteProblems(snapshot, auditDiff{}, true, false); len(allowed) != 0 {
		t.Fatalf("allow unknown baseline problems = %#v, want none", allowed)
	}
}

func TestBaselineWriteProblemsRejectsUnknownPromptSourceTags(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		PromptTagCounts: []PromptTagCount{
			{Kind: "prompt", Tag: "future-prompt-surface", Count: 1},
			{Kind: "source", Tag: "future-source-surface", Count: 1},
		},
	}}

	problems := baselineWriteProblems(snapshot, auditDiff{}, false, false)

	assertContains(t, problems, "unknown prompt tags: future-prompt-surface")
	assertContains(t, problems, "unknown source tags: future-source-surface")
	if allowed := baselineWriteProblems(snapshot, auditDiff{}, true, false); len(allowed) != 2 {
		t.Fatalf("allow unknown baseline problems = %#v, want prompt/source taxonomy problems", allowed)
	}
}

func TestBaselineWriteProblemsRejectsUnknownPromptKinds(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		PromptFingerprints:  []PromptFingerprint{{SHA256: "future", Kind: "future-kind", Tags: []string{"guidance"}}},
		PromptTagCounts:     []PromptTagCount{{Kind: "future-kind", Tag: "guidance", Count: 1}},
	}}

	problems := baselineWriteProblems(snapshot, auditDiff{}, false, false)

	assertContains(t, problems, "unknown prompt kinds: future-kind")
	if allowed := baselineWriteProblems(snapshot, auditDiff{}, true, false); len(allowed) != 1 {
		t.Fatalf("allow unknown baseline problems = %#v, want prompt kind taxonomy problem", allowed)
	}
}

func TestBaselineWriteProblemsRejectsUnexpectedPromptTagCounts(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		PromptTagCounts:     []PromptTagCount{{Kind: "source", Tag: "tools", Count: 199}},
	}}

	problems := baselineWriteProblems(snapshot, auditDiff{}, false, false)

	assertContains(t, problems, "unexpected prompt/source tag counts: source/tools=199")
	if allowed := baselineWriteProblems(snapshot, auditDiff{}, true, false); len(allowed) != 1 {
		t.Fatalf("allow unknown baseline problems = %#v, want prompt/source count problem", allowed)
	}
}

func TestBaselineWriteProblemsRejectsUnexpectedPromptKindCounts(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		PromptFingerprints:  []PromptFingerprint{{SHA256: "source", Kind: "source", Tags: []string{"tools"}}},
	}}

	problems := baselineWriteProblems(snapshot, auditDiff{}, false, false)

	assertContains(t, problems, "unexpected prompt/source kind counts: source=1")
	if allowed := baselineWriteProblems(snapshot, auditDiff{}, true, false); len(allowed) != 1 {
		t.Fatalf("allow unknown baseline problems = %#v, want prompt/source kind count problem", allowed)
	}
}

func TestBaselineWriteProblemsRejectsMissingPromptFingerprintData(t *testing.T) {
	snapshot := Snapshot{
		Source: SourceInfo{Path: "/tmp/amp", SHA256: strings.Repeat("1", 64), SizeBytes: minReleaseBinarySizeBytes, Versions: []string{"0.0.1780273161-g9c84ba"}},
		Signals: Signals{
			RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
			ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
			SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
			ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
			ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
		},
	}

	problems := baselineWriteProblems(snapshot, auditDiff{}, false, false)

	assertContains(t, problems, "unexpected prompt/source kind counts: missing")
	assertContains(t, problems, "unexpected prompt/source tag counts: missing")
	if !strictAuditFailed(snapshot, auditDiff{}) {
		t.Fatal("strict audit should fail for missing source-backed prompt fingerprint data")
	}
}

func TestBaselineWriteProblemsRejectsMissingReleaseSignalCategories(t *testing.T) {
	snapshot := Snapshot{
		Source:  SourceInfo{Path: "/tmp/amp", SHA256: strings.Repeat("1", 64), SizeBytes: minReleaseBinarySizeBytes, Versions: []string{"0.0.1780273161-g9c84ba"}},
		Signals: Signals{},
	}

	problems := baselineWriteProblems(snapshot, auditDiff{}, true, false)
	categoryProblem := ""
	for _, problem := range problems {
		if strings.HasPrefix(problem, "missing release signal categories:") {
			categoryProblem = problem
			break
		}
	}

	assertContainsString(t, categoryProblem, "routes")
	assertContainsString(t, categoryProblem, "route_methods")
	if !strictAuditFailed(snapshot, auditDiff{}) {
		t.Fatal("strict audit should fail for missing release signal categories")
	}
}

func TestBaselineWriteProblemsRejectsMissingReleaseSourceMetadata(t *testing.T) {
	snapshot := Snapshot{
		Source: SourceInfo{Path: "/tmp/amp", SizeBytes: minReleaseBinarySizeBytes},
	}

	problems := baselineWriteProblems(snapshot, auditDiff{}, true, false)
	text := strings.Join(problems, "\n")
	categoryProblem := ""
	for _, problem := range problems {
		if strings.HasPrefix(problem, "missing release signal categories:") {
			categoryProblem = problem
			break
		}
	}

	assertContainsString(t, text, "missing release source metadata: sha256, versions, strings_scanned")
	assertContainsString(t, categoryProblem, "routes")
	if !strictAuditFailed(snapshot, auditDiff{}) {
		t.Fatal("strict audit should fail for missing release source metadata")
	}
}

func TestBaselineWriteProblemsRejectsUnexpectedRawReleaseSignals(t *testing.T) {
	snapshot := Snapshot{
		Source: SourceInfo{Path: "/tmp/amp", SHA256: strings.Repeat("1", 64), SizeBytes: minReleaseBinarySizeBytes, Versions: []string{"0.0.1780273161-g9c84ba"}},
		Signals: Signals{
			Routes:              []string{"/future"},
			ThreadDeltaEvents:   []string{"future:event"},
			ThreadReaderMarkers: []string{"futureReader"},
			ToolCancelReasons:   []string{"future-cancel"},
			ToolRunStatuses:     []string{"future-status"},
			ToolCatalog:         []string{"future_tool"},
			StreamJSONMarkers:   []string{"future_stream_field"},
			ModeSettingMarkers:  []string{"futureModeDefault"},
			ProviderProtocol:    []string{"future-provider-header"},
			ReviewContract:      []string{"future-review-contract"},
			Settings:            []string{"future.setting"},
			Models:              []string{"future-model"},
			ActorRuntime:        []string{"futureActor"},
		},
	}

	problems := baselineWriteProblems(snapshot, auditDiff{}, true, false)
	text := strings.Join(problems, "\n")

	assertContainsString(t, text, "unexpected raw release signals:")
	assertContainsString(t, text, "routes:/future")
	assertContainsString(t, text, "thread_delta_events:future:event")
	assertContainsString(t, text, "thread_reader_markers:futureReader")
	assertContainsString(t, text, "tool_catalog_markers:future_tool")
	assertContainsString(t, text, "review_contract_markers:future-review-contract")
	assertContainsString(t, text, "settings:future.setting")
	assertContainsString(t, text, "actor_runtime_markers:futureActor")
	if !strictAuditFailed(snapshot, auditDiff{}) {
		t.Fatal("strict audit should fail for unexpected raw release signals")
	}
}

func TestBaselineWriteProblemsRejectsMissingExpectedRawReleaseSignals(t *testing.T) {
	snapshot := Snapshot{
		Source: SourceInfo{Path: "/tmp/amp", SHA256: strings.Repeat("1", 64), SizeBytes: minReleaseBinarySizeBytes, Versions: []string{"0.0.1780273161-g9c84ba"}},
		Signals: Signals{
			Routes:             []string{"/api/provider/openai/v1"},
			ToolCancelReasons:  []string{"user:cancelled"},
			ToolRunStatuses:    []string{"done"},
			ToolCatalog:        []string{"builtin:edit_file"},
			StreamJSONMarkers:  []string{"--stream-json"},
			ModeSettingMarkers: []string{"reasoning.effort"},
			ProviderProtocol:   []string{"x-amp-feature"},
			Settings:           []string{"painter.model"},
			Models:             []string{"gpt-5.5"},
			ActorRuntime:       []string{"threadActor"},
		},
	}

	missing := missingExpectedRawReleaseSignals(snapshot)

	assertContains(t, missing, "routes:/api/internal")
	assertContains(t, missing, "route_methods:/api/internal=POST=remote-web")
	assertContains(t, missing, "thread_delta_events:thread_truncated")
	assertContains(t, missing, "thread_reader_markers:read_messages")
	assertContains(t, missing, "tool_catalog_markers:read_file")
	assertContains(t, missing, "settings:skills.path")
	assertContains(t, missing, "models:claude-opus-4-8")
	problems := baselineWriteProblems(snapshot, auditDiff{}, true, false)
	assertContainsString(t, strings.Join(problems, "\n"), "missing expected raw release signals:")
	if !strictAuditFailed(snapshot, auditDiff{}) {
		t.Fatal("strict audit should fail for missing expected raw release signals")
	}
}

func TestBaselineWriteProblemsRejectsMissingExpectedExactReleaseValues(t *testing.T) {
	snapshot := Snapshot{
		Source:  SourceInfo{Path: "/tmp/amp", SHA256: strings.Repeat("1", 64), SizeBytes: minReleaseBinarySizeBytes, Versions: []string{"0.0.1780273161-g9c84ba"}},
		Signals: Signals{},
	}

	missing := missingExpectedExactReleaseValues(snapshot)

	assertContains(t, missing, "agent_mode_profiles:smart profile")
	assertContains(t, missing, "agent_mode_routes:smart route")
	assertContains(t, missing, "setting_defaults:skills.path")
	assertContains(t, missing, "model_limits:claude-opus-4-8")

	problems := baselineWriteProblems(snapshot, auditDiff{}, true, true)
	assertContainsString(t, strings.Join(problems, "\n"), "missing expected exact release values:")
	if !strictAuditFailed(snapshot, auditDiff{}) {
		t.Fatal("strict audit should fail for missing expected exact release values")
	}
}

func TestBaselineWriteProblemsRejectsMissingExpectedCoverageValues(t *testing.T) {
	snapshot := Snapshot{
		Source:  SourceInfo{Path: "/tmp/amp", SHA256: strings.Repeat("1", 64), SizeBytes: minReleaseBinarySizeBytes, Versions: []string{"0.0.1780273161-g9c84ba"}},
		Signals: Signals{},
	}

	missing := missingExpectedCoverageValues(snapshot)

	assertContains(t, missing, "route_coverage:/api/internal=remote-web")
	assertContains(t, missing, "thread_delta_coverage:thread_truncated=history")
	assertContains(t, missing, "thread_reader_coverage:read_messages=message-reader-route")
	assertContains(t, missing, "tool_run_coverage:blocked-on-user=pending")
	assertContains(t, missing, "tool_catalog_coverage:read_file=file-read")
	assertContains(t, missing, "stream_json_coverage:agent_mode=init-field")
	assertContains(t, missing, "mode_setting_coverage:reasoningEffort=thread-metadata")
	assertContains(t, missing, "provider_protocol_coverage:x-amp-user=amp-provider-header")
	assertContains(t, missing, "agent_mode_coverage:smart=local-runtime")
	assertContains(t, missing, "setting_coverage:skills.path=local-runtime")
	assertContains(t, missing, "model_coverage:claude-opus-4-8=anthropic/claude-opus")
	assertContains(t, missing, "actor_runtime_coverage:threadActor=local-runtime")

	problems := baselineWriteProblems(snapshot, auditDiff{}, true, true)
	assertContainsString(t, strings.Join(problems, "\n"), "missing expected coverage values:")
	if !strictAuditFailed(snapshot, auditDiff{}) {
		t.Fatal("strict audit should fail for missing expected coverage values")
	}
}

func TestBaselineWriteProblemsRejectsDuplicateSnapshotValues(t *testing.T) {
	snapshot := Snapshot{
		Source: SourceInfo{
			Versions:    []string{"0.0.1780273161-g9c84ba", "0.0.1780273161-g9c84ba"},
			BuildStamps: []string{"2026-06-01T00:24:05.462Z", "2026-06-01T00:24:05.462Z"},
		},
		Signals: Signals{
			Routes:              []string{"/api/provider/openai/v1", "/api/provider/openai/v1"},
			RouteMethods:        []RouteMethods{{Name: "/api/internal", Methods: []string{"POST"}, Scope: "remote-web"}, {Name: "/api/internal", Methods: []string{"POST"}, Scope: "remote-web"}, {Name: "/api/thread-actors", Methods: []string{"POST", "POST"}, Scope: "local-runtime"}},
			ThreadDeltaEvents:   []string{"thread_truncated", "thread_truncated"},
			ThreadReaderMarkers: []string{"read_messages", "read_messages"},
			ToolCatalog:         []string{"read_file", "read_file"},
			StreamJSONMarkers:   []string{"agent_mode", "agent_mode"},
			ModeSettingMarkers:  []string{"reasoningEffort", "reasoningEffort"},
			ProviderProtocol:    []string{"x-amp-feature", "x-amp-feature"},
			AgentModeProfiles:   []AgentModeProfile{{Name: "smart", ReasoningLevels: []string{"high", "high"}}},
			Settings:            []string{"painter.model", "painter.model"},
			Models:              []string{"claude-opus-4-8", "claude-opus-4-8"},
			ActorRuntime:        []string{"threadActor", "threadActor"},
			AdaptiveThinking: []AdaptiveThinkingRule{{
				ModelEnums:   []string{"CLAUDE_OPUS_4_8", "CLAUDE_OPUS_4_8"},
				Models:       []string{"claude-opus-4-8", "claude-opus-4-8"},
				EffortLevels: []string{"medium", "medium"},
			}},
			ProviderReasoning: []ProviderReasoningRule{{Provider: "anthropic", Sources: []string{"model-default", "model-default"}}},
			CompactionRules:   []CompactionRule{{Name: "anthropic-tool-runner", UsageFields: []string{"input_tokens", "input_tokens"}}},
			PromptFingerprints: []PromptFingerprint{
				{SHA256: "same-hash", Kind: "prompt", Tags: []string{"guidance", "guidance"}},
				{SHA256: "same-hash", Kind: "prompt", Tags: []string{"guidance"}},
			},
			PromptTagCounts: []PromptTagCount{
				{Kind: "prompt", Tag: "guidance", Count: 22},
				{Kind: "prompt", Tag: "guidance", Count: 22},
			},
		}}

	problems := baselineWriteProblems(snapshot, auditDiff{}, true, true)
	text := strings.Join(problems, "\n")
	duplicates := duplicateSnapshotValues(snapshot)

	assertContainsString(t, text, "duplicate snapshot values:")
	assertContains(t, duplicates, "source_versions:0.0.1780273161-g9c84ba")
	assertContains(t, duplicates, "source_build_stamps:2026-06-01T00:24:05.462Z")
	assertContains(t, duplicates, "routes:/api/provider/openai/v1")
	assertContains(t, duplicates, "route_methods:/api/internal=POST=remote-web")
	assertContains(t, duplicates, "route_method_methods:/api/thread-actors:POST")
	assertContains(t, duplicates, "thread_delta_events:thread_truncated")
	assertContains(t, duplicates, "thread_reader_markers:read_messages")
	assertContains(t, duplicates, "tool_catalog_markers:read_file")
	assertContains(t, duplicates, "agent_mode_profile_reasoning_levels:smart:high")
	assertContains(t, duplicates, "adaptive_thinking_model_enums:CLAUDE_OPUS_4_8,CLAUDE_OPUS_4_8:CLAUDE_OPUS_4_8")
	assertContains(t, duplicates, "adaptive_thinking_models:CLAUDE_OPUS_4_8,CLAUDE_OPUS_4_8:claude-opus-4-8")
	assertContains(t, duplicates, "adaptive_thinking_effort_levels:CLAUDE_OPUS_4_8,CLAUDE_OPUS_4_8:medium")
	assertContains(t, duplicates, "provider_reasoning_sources:anthropic:model-default")
	assertContains(t, duplicates, "compaction_usage_fields:anthropic-tool-runner:input_tokens")
	assertContains(t, duplicates, "prompt_fingerprints:same-hash")
	assertContains(t, duplicates, "prompt_fingerprint_tags:same-hash:guidance")
	assertContains(t, duplicates, "prompt_tag_counts:prompt/guidance")
	if !strictAuditFailed(snapshot, auditDiff{}) {
		t.Fatal("strict audit should fail for duplicate snapshot values")
	}
}

func TestBaselineWriteProblemsRejectsCoverageMismatches(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteMethods:        []RouteMethods{{Name: "/api/internal", Methods: []string{"GET"}, Scope: "remote-web"}},
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "amp-owned"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "assistant:message-update", Area: "queue"}},
		ToolCancelCoverage:  []ToolCancelCoverage{{Name: "user:interrupted", Area: "system-safety"}},
		ToolRunCoverage:     []ToolRunCoverage{{Name: "done", Area: "running"}},
		ToolCatalogCoverage: []ToolCatalogCoverage{{Name: "builtin:edit_file", Area: "file-read"}},
		StreamJSONCoverage:  []StreamJSONCoverage{{Name: "agent_mode", Area: "result-field"}},
		ModeSettingCoverage: []ModeSettingCoverage{{Name: "reasoningEffort", Area: "provider-speed"}},
		ProviderCoverage:    []ProviderCoverage{{Name: "x-amp-feature", Area: "amp-client-header"}},
		AgentModeCoverage:   []AgentModeCoverage{{Name: "deep", Scope: "server-only"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "amp-owned"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "anthropic", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "amp-owned"}},
	}}

	problems := baselineWriteProblems(snapshot, auditDiff{}, true, false)
	text := strings.Join(problems, "\n")

	assertContainsString(t, text, "unexpected route methods: /api/internal=GET=remote-web want /api/internal=POST=remote-web")
	assertContainsString(t, text, "unexpected route coverage: /api/provider/openai/v1=amp-owned want local-runtime")
	assertContainsString(t, text, "unexpected thread delta coverage: assistant:message-update=queue want assistant-message")
	assertContainsString(t, text, "unexpected tool cancellation coverage: user:interrupted=system-safety want user-interrupt")
	assertContainsString(t, text, "unexpected tool run coverage: done=running want terminal-success")
	assertContainsString(t, text, "unexpected tool catalog coverage: builtin:edit_file=file-read want file-edit")
	assertContainsString(t, text, "unexpected stream-json coverage: agent_mode=result-field want init-field")
	assertContainsString(t, text, "unexpected mode setting coverage: reasoningEffort=provider-speed want thread-metadata")
	assertContainsString(t, text, "unexpected provider protocol coverage: x-amp-feature=amp-client-header want amp-provider-header")
	assertContainsString(t, text, "unexpected agent mode coverage: deep=server-only want local-runtime")
	assertContainsString(t, text, "unexpected setting coverage: painter.model=amp-owned want local-runtime")
	assertContainsString(t, text, "unexpected model coverage: gpt-5.5=anthropic/gpt want openai/gpt")
	assertContainsString(t, text, "unexpected actor coverage: threadActor=amp-owned want local-runtime")
	if !strictAuditFailed(snapshot, auditDiff{}) {
		t.Fatal("strict audit should fail for coverage mismatches")
	}
}

func TestBaselineWriteProblemsRejectsExactValueMismatches(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		// smart's known values now pin CLAUDE_OPUS_4_8, so an opus-4-7 profile/route is a
		// deliberate mismatch the writer must reject.
		AgentModeProfiles: []AgentModeProfile{{
			Name:            "smart",
			PrimaryModel:    "CLAUDE_OPUS_4_7",
			ReasoningEffort: "high",
			ReasoningLevels: []string{"high", "max", "xhigh"},
			IncludeTools:    "hP",
			DeferredTools:   true,
			Visible:         true,
			VisibleInV2:     true,
		}},
		AgentModeRoutes: []AgentModeRoute{{
			Name:            "smart",
			Provider:        "anthropic",
			Model:           "claude-opus-4-7",
			PrimaryModel:    "CLAUDE_OPUS_4_7",
			ReasoningEffort: "high",
			ContextWindow:   332000,
			MaxOutputTokens: 32000,
		}},
		SettingDefaults: []SettingDefault{{Name: "painter.model", Value: "gpt-image-1", Scope: "local-runtime"}},
		ModelLimits: []ModelLimit{{
			Name:            "gpt-5.5",
			Enum:            "GPT_5_5",
			Provider:        "openai",
			DisplayName:     "GPT-5.5",
			ContextWindow:   272000,
			MaxOutputTokens: 128000,
		}},
		LargeContextRules: []LargeContextRule{{
			PrimaryModel:               "CLAUDE_OPUS_4_6",
			Alias:                      "claude-opus-4-7-1m",
			ContextWindow:              1000000,
			MaxOutputTokens:            32000,
			MaxInputTokens:             968000,
			RequiresEnableLargeContext: true,
		}},
		AdaptiveThinking: []AdaptiveThinkingRule{{
			ModelEnums:       []string{"CLAUDE_OPUS_4_6", "CLAUDE_OPUS_4_7", "CLAUDE_OPUS_4_8"},
			Models:           []string{"claude-opus-4-6", "claude-opus-4-7", "claude-opus-4-8"},
			EffortLevels:     []string{"low", "medium", "high", "xhigh", "max"},
			DefaultEffort:    "high",
			ThinkingType:     "adaptive",
			Display:          "summarized",
			UsesOutputConfig: true,
		}},
		ProviderReasoning: []ProviderReasoningRule{{
			Provider:      "anthropic",
			Sources:       []string{"setting:reasoning.effort", "mode:reasoningEffort", "model-default"},
			DefaultEffort: "medium",
		}},
		ProviderHeaders: []ProviderHeaderRule{{
			Provider: "anthropic",
		}},
		ProviderFeatures: []ProviderFeatureRule{{
			Feature:  "amp.review",
			Provider: "google",
			Callsite: "code-review",
			Tool:     "code_review",
			Header:   "x-amp-feature",
			Default:  true,
		}},
		CompactionRules: []CompactionRule{{
			Name:                     "anthropic-tool-runner",
			Provider:                 "anthropic",
			Trigger:                  "observed-usage",
			Timing:                   "post-response",
			DefaultThresholdTokens:   90000,
			UsageFields:              []string{"input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "output_tokens"},
			SummaryPrompt:            "continuation-summary",
			HistoryReplacementRole:   "user",
			TrailingAssistantToolUse: "strip-tool-use-blocks",
			HelperHeader:             "x-stainless-helper",
			HelperHeaderValue:        "compaction",
		}},
	}}

	problems := baselineWriteProblems(snapshot, auditDiff{}, true, false)
	text := strings.Join(problems, "\n")

	assertContainsString(t, text, "unexpected agent mode profiles: smart profile")
	assertContainsString(t, text, "unexpected agent mode routes: smart route")
	assertContainsString(t, text, "unexpected setting defaults: painter.model=gpt-image-1 want gpt-image-2")
	assertContainsString(t, text, "unexpected model limits: gpt-5.5|enum=GPT_5_5|provider=openai|display=GPT-5.5|context=272000|max_out=128000 want gpt-5.5|enum=GPT_5_5|provider=openai|display=GPT-5.5|context=400000|max_out=128000")
	assertContainsString(t, text, "unexpected large-context rules: CLAUDE_OPUS_4_6 large-context")
	assertContainsString(t, text, "unexpected adaptive-thinking rules: CLAUDE_OPUS_4_6,CLAUDE_OPUS_4_7,CLAUDE_OPUS_4_8 adaptive")
	assertContainsString(t, text, "unexpected provider reasoning rules: anthropic")
	assertContainsString(t, text, "unexpected provider header rules: anthropic")
	assertContainsString(t, text, "unexpected provider feature rules: amp.review/google")
	assertContainsString(t, text, "unexpected compaction rules: anthropic-tool-runner compaction")
	if !strictAuditFailed(snapshot, auditDiff{}) {
		t.Fatal("strict audit should fail for exact value mismatches")
	}
}

func TestBaselineWriteProblemsAllowsClassifiedCoverage(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}

	if problems := baselineWriteProblems(snapshot, auditDiff{}, false, false); len(problems) != 0 {
		t.Fatalf("classified baseline problems = %#v, want none", problems)
	}
}

func TestBaselineWriteProblemsRequiresExplicitDiffAcceptance(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "models",
		Added: []string{"claude-opus-4-8"},
	}}}

	problems := baselineWriteProblems(snapshot, diff, false, false)

	assertContains(t, problems, "baseline differs from current Amp binary: run the parity audit checklist before refreshing the baseline")
	if accepted := baselineWriteProblems(snapshot, diff, false, true); len(accepted) != 0 {
		t.Fatalf("accepted baseline diff problems = %#v, want none", accepted)
	}
}

func TestBaselineWriteProblemsRequiresExplicitPromptDiffAcceptance(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	diff := auditDiff{Prompts: promptDiff{Removed: []PromptFingerprint{{
		SHA256: "old-prompt",
		Length: 300,
		Kind:   "prompt",
		Tags:   []string{"compaction"},
	}}}}

	problems := baselineWriteProblems(snapshot, diff, false, false)

	assertContains(t, problems, "baseline differs from current Amp binary: run the parity audit checklist before refreshing the baseline")
	if accepted := baselineWriteProblems(snapshot, diff, false, true); len(accepted) != 0 {
		t.Fatalf("accepted prompt diff problems = %#v, want none", accepted)
	}
}

func TestBaselineWriteProblemsRequiresExplicitSourceDiffAcceptance(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/provider/openai/v1", Scope: "local-runtime"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	diff := auditDiff{Prompts: promptDiff{Added: []PromptFingerprint{{
		SHA256: "source",
		Length: 800,
		Kind:   "source",
		Tags:   []string{"tools"},
	}}}}

	problems := baselineWriteProblems(snapshot, diff, false, false)

	assertContains(t, problems, "baseline differs from current Amp binary: run the parity audit checklist before refreshing the baseline")
	if accepted := baselineWriteProblems(snapshot, diff, false, true); len(accepted) != 0 {
		t.Fatalf("accepted source diff problems = %#v, want none", accepted)
	}
}

func TestBaselineWriteAcceptDiffDoesNotAllowUnknownCoverage(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/new-surface", Scope: "unknown"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "models",
		Added: []string{"claude-opus-4-8"},
	}}}

	problems := baselineWriteProblems(snapshot, diff, false, true)

	assertContains(t, problems, "unknown routes: /api/new-surface")
	assertNotContains(t, problems, "baseline differs from current Amp binary: run the parity audit checklist before refreshing the baseline")
}

func TestBaselineWriteAllowUnknownDoesNotAcceptDiff(t *testing.T) {
	snapshot := Snapshot{Signals: Signals{
		RouteCoverage:       []RouteCoverage{{Name: "/api/new-surface", Scope: "unknown"}},
		ThreadDeltaCoverage: []ThreadDeltaCoverage{{Name: "agent-mode", Area: "settings"}},
		SettingCoverage:     []SettingCoverage{{Name: "painter.model", Scope: "local-runtime"}},
		ModelCoverage:       []ModelCoverage{{Name: "gpt-5.5", Provider: "openai", Family: "gpt"}},
		ActorCoverage:       []ActorCoverage{{Name: "threadActor", Area: "local-runtime"}},
	}}
	diff := auditDiff{Categories: []categoryDiff{{
		Name:  "models",
		Added: []string{"claude-opus-4-8"},
	}}}

	problems := baselineWriteProblems(snapshot, diff, true, false)

	assertContains(t, problems, "baseline differs from current Amp binary: run the parity audit checklist before refreshing the baseline")
	assertNotContains(t, problems, "unknown routes: /api/new-surface")
}

func TestRunAuditWriteBaselineRejectsExistingDiff(t *testing.T) {
	binaryPath := writeAuditFixtureBinary(t)
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	baseline := snapshot
	baseline.Source.SHA256 = strings.Repeat("0", 64)
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeSnapshotFile(baselinePath, baseline); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-write-baseline"}, &stdout, &stderr)

	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	assertContainsString(t, stderr.String(), "baseline differs from current Amp binary")
	after, err := readSnapshotFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Source.SHA256 != baseline.Source.SHA256 {
		t.Fatalf("baseline sha changed to %q, want original %q", after.Source.SHA256, baseline.Source.SHA256)
	}
}

func TestRunAuditWriteBaselineAcceptsReviewedDiff(t *testing.T) {
	binaryPath := writeAuditFixtureBinary(t)
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	baseline := snapshot
	baseline.Source.SHA256 = strings.Repeat("0", 64)
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeSnapshotFile(baselinePath, baseline); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-write-baseline", "-accept-baseline-diff"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	after, err := readSnapshotFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Source.SHA256 != snapshot.Source.SHA256 {
		t.Fatalf("baseline sha = %q, want current %q", after.Source.SHA256, snapshot.Source.SHA256)
	}
}

func TestRunAuditWriteBaselineAcceptsReviewedStalePromptCounts(t *testing.T) {
	binaryPath := writeAuditFixtureBinary(t)
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	baseline := snapshot
	for i := 0; i < knownPromptKindCountValues["prompt"]+1; i++ {
		baseline.Signals.PromptFingerprints = append(baseline.Signals.PromptFingerprints, PromptFingerprint{
			SHA256: fmt.Sprintf("%064x", i+1),
			Kind:   "prompt",
			Tags:   []string{"tools"},
		})
	}
	baseline.Signals.PromptTagCounts = []PromptTagCount{{Kind: "prompt", Tag: "tools", Count: knownPromptTagCountValues["prompt/tools"] + 1}}
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeSnapshotFile(baselinePath, baseline); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-write-baseline", "-accept-baseline-diff"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	assertContainsString(t, stderr.String(), "existing baseline has stale audit values")
	assertContainsString(t, stderr.String(), "unexpected prompt/source kind counts")
	assertContainsString(t, stderr.String(), "unexpected prompt/source tag counts")
	after, err := readSnapshotFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Signals.PromptFingerprints) != len(snapshot.Signals.PromptFingerprints) {
		t.Fatalf("baseline prompt fingerprints = %d, want %d", len(after.Signals.PromptFingerprints), len(snapshot.Signals.PromptFingerprints))
	}
}

func TestReadSnapshotFileRejectsStaleSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "baseline.json")
	snapshot := Snapshot{Schema: minSupportedSnapshotSchema - 1}
	if err := writeJSONFile(path, snapshot); err != nil {
		t.Fatal(err)
	}

	_, err := readSnapshotFile(path)

	if !errors.Is(err, errUnsupportedSnapshotSchema) {
		t.Fatalf("readSnapshotFile error = %v, want unsupported schema", err)
	}
}

func TestRunAuditWriteBaselineRequiresAcceptForStaleSchema(t *testing.T) {
	binaryPath := writeAuditFixtureBinary(t)
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	baseline := snapshot
	baseline.Schema = minSupportedSnapshotSchema - 1
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeJSONFile(baselinePath, baseline); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-write-baseline"}, &stdout, &stderr)

	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	assertContainsString(t, stderr.String(), "existing baseline uses a stale schema")
	after, err := readJSONSnapshotFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Schema != baseline.Schema {
		t.Fatalf("baseline schema changed to %d, want original %d", after.Schema, baseline.Schema)
	}
}

func TestRunAuditWriteBaselineAcceptsStaleSchemaAfterReview(t *testing.T) {
	binaryPath := writeAuditFixtureBinary(t)
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	baseline := snapshot
	baseline.Schema = minSupportedSnapshotSchema - 1
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeJSONFile(baselinePath, baseline); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-write-baseline", "-accept-baseline-diff"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	assertContainsString(t, stderr.String(), "existing baseline uses a stale schema")
	after, err := readSnapshotFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Schema != snapshotSchema {
		t.Fatalf("baseline schema = %d, want %d", after.Schema, snapshotSchema)
	}
}

func TestRunAuditWriteBaselineAcceptsSupportedOlderSchemaAfterReview(t *testing.T) {
	if snapshotSchema-1 < minSupportedSnapshotSchema {
		t.Skip("no supported older schema available")
	}
	binaryPath := writeAuditFixtureBinary(t)
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	baseline := snapshot
	baseline.Schema = snapshotSchema - 1
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeJSONFile(baselinePath, baseline); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-write-baseline", "-accept-baseline-diff"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	assertContainsString(t, stderr.String(), "existing baseline uses a stale schema")
	after, err := readSnapshotFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Schema != snapshotSchema {
		t.Fatalf("baseline schema = %d, want %d", after.Schema, snapshotSchema)
	}
}

func TestRunAuditStrictRejectsMalformedBaseline(t *testing.T) {
	binaryPath := writeAuditFixtureBinary(t)
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Signals.ThreadDeltaEvents) == 0 {
		t.Fatal("fixture did not produce thread delta events")
	}
	baseline := snapshot
	baseline.Signals.ThreadDeltaEvents = append(baseline.Signals.ThreadDeltaEvents, baseline.Signals.ThreadDeltaEvents[0])
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeSnapshotFile(baselinePath, baseline); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-strict"}, &stdout, &stderr)

	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	assertContainsString(t, stdout.String(), "baseline: duplicate snapshot values:")
	assertContainsString(t, stdout.String(), "thread_delta_events:"+baseline.Signals.ThreadDeltaEvents[0])
}

func TestRunAuditWriteBaselineRejectsMalformedExistingBaseline(t *testing.T) {
	binaryPath := writeAuditFixtureBinary(t)
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Signals.ThreadDeltaEvents) == 0 {
		t.Fatal("fixture did not produce thread delta events")
	}
	baseline := snapshot
	baseline.Signals.ThreadDeltaEvents = append(baseline.Signals.ThreadDeltaEvents, baseline.Signals.ThreadDeltaEvents[0])
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeSnapshotFile(baselinePath, baseline); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-write-baseline", "-accept-baseline-diff"}, &stdout, &stderr)

	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	assertContainsString(t, stderr.String(), "existing baseline: duplicate snapshot values:")
	assertContainsString(t, stderr.String(), "thread_delta_events:"+baseline.Signals.ThreadDeltaEvents[0])
}

func TestRunAuditAllowUnknownBaselineDoesNotAcceptExistingDiff(t *testing.T) {
	binaryPath := writeAuditFixtureBinary(t, "/api/future-local-runtime")
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	baseline := snapshot
	baseline.Source.SHA256 = strings.Repeat("0", 64)
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeSnapshotFile(baselinePath, baseline); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-write-baseline", "-allow-unknown-baseline"}, &stdout, &stderr)

	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	assertContainsString(t, stderr.String(), "baseline differs from current Amp binary")
	if strings.Contains(stderr.String(), "unknown routes") {
		t.Fatalf("stderr = %q, should not report unknown coverage when -allow-unknown-baseline is set", stderr.String())
	}
}

func TestRunAuditChecklistWritesToProvidedStdout(t *testing.T) {
	binaryPath := writeAuditFixtureBinary(t)
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeSnapshotFile(baselinePath, snapshot); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-checklist"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	assertContainsString(t, stdout.String(), "Amp binary parity audit")
	assertContainsString(t, stdout.String(), "lifecycle parity checklist")
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunAuditPromptDiffExcerptsWriteToProvidedStdout(t *testing.T) {
	prompt := strings.Repeat("Please inspect guidance and compaction behavior before updating the baseline. ", 12)
	binaryPath := writeAuditFixtureBinary(t, prompt)
	snapshot, err := BuildSnapshot(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Signals.PromptFingerprints) == 0 {
		t.Fatal("fixture did not produce prompt fingerprints")
	}
	baseline := snapshot
	baseline.Signals.PromptFingerprints = nil
	baseline.Signals.PromptTagCounts = nil
	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeSnapshotFile(baselinePath, baseline); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer

	code := runAudit([]string{"-binary", binaryPath, "-baseline", baselinePath, "-prompt-diff-excerpts"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	assertContainsString(t, stdout.String(), "prompt/source excerpts:")
	assertContainsString(t, stdout.String(), "Please inspect guidance and compaction behavior")
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func assertContains[T comparable](t *testing.T, values []T, want T) {
	t.Helper()
	for _, value := range values {
		if value == want {
			return
		}
	}
	t.Fatalf("%#v does not contain %#v", values, want)
}

func assertNotContains[T comparable](t *testing.T, values []T, want T) {
	t.Helper()
	for _, value := range values {
		if value == want {
			t.Fatalf("%#v contains %#v", values, want)
		}
	}
}

func assertContainsString(t *testing.T, value, want string) {
	t.Helper()
	if !strings.Contains(value, want) {
		t.Fatalf("%q does not contain %q", value, want)
	}
}

func assertStringSetsEqual(t *testing.T, name string, got, want []string) {
	t.Helper()
	gotSet := sliceSet(got)
	wantSet := sliceSet(want)
	var missing []string
	for value := range wantSet {
		if _, ok := gotSet[value]; !ok {
			missing = append(missing, value)
		}
	}
	var extra []string
	for value := range gotSet {
		if _, ok := wantSet[value]; !ok {
			extra = append(extra, value)
		}
	}
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("%s mismatch: missing=%v extra=%v", name, sortedStrings(missing), sortedStrings(extra))
	}
}

func sortedStrings(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return sortedKeys(set)
}

func sortedStringMapKeys(values map[string]string) []string {
	set := make(map[string]struct{}, len(values))
	for key := range values {
		set[key] = struct{}{}
	}
	return sortedKeys(set)
}

func intMapStrings(values map[string]int) []string {
	items := make([]string, 0, len(values))
	for key, count := range values {
		items = append(items, fmt.Sprintf("%s=%d", key, count))
	}
	return sortedStrings(items)
}

func captureAuditStdout(t *testing.T, fn func()) string {
	t.Helper()
	var out bytes.Buffer
	previousStdout := auditStdout
	auditStdout = &out
	defer func() {
		auditStdout = previousStdout
	}()
	fn()
	return out.String()
}

func lifecycleCheckAreas(checks []lifecycleCheck) []string {
	areas := make([]string, 0, len(checks))
	for _, check := range checks {
		areas = append(areas, check.Area)
	}
	return areas
}

func lifecycleCheckByArea(t *testing.T, checks []lifecycleCheck, area string) lifecycleCheck {
	t.Helper()
	for _, check := range checks {
		if check.Area == area {
			return check
		}
	}
	t.Fatalf("missing checklist area %q in %#v", area, checks)
	return lifecycleCheck{}
}

func diffSnapshotCategoryNamesForTest() []string {
	diff := diffSnapshots(Snapshot{}, Snapshot{})
	names := make([]string, 0, len(diff.Categories))
	for _, category := range diff.Categories {
		names = append(names, category.Name)
	}
	return sortedStrings(names)
}

func signalJSONFieldNamesForTest() []string {
	signalType := reflect.TypeOf(Signals{})
	names := make([]string, 0, signalType.NumField())
	for i := 0; i < signalType.NumField(); i++ {
		field := signalType.Field(i)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			continue
		}
		names = append(names, name)
	}
	return sortedStrings(names)
}

func lifecycleChecklistDiffCaseNames(cases []lifecycleChecklistDiffCase) []string {
	names := make([]string, 0, len(cases))
	for _, item := range cases {
		names = append(names, item.name)
	}
	return sortedStrings(names)
}

func sortedStringSliceMapKeys(values map[string][]string) []string {
	set := make(map[string]struct{}, len(values))
	for key := range values {
		set[key] = struct{}{}
	}
	return sortedKeys(set)
}

func sortedBoolMapKeys(values map[string]bool) []string {
	set := make(map[string]struct{}, len(values))
	for key, enabled := range values {
		if enabled {
			set[key] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func parseLifecycleGoTestCommandForTest(command string) (string, []string, bool) {
	command = strings.TrimSpace(command)
	if !strings.HasPrefix(command, "go test ") {
		return "", nil, false
	}
	match := regexp.MustCompile(`\s-run\s+'([^']+)'\s+(.+)$`).FindStringSubmatch(command)
	if len(match) != 3 {
		return "", nil, false
	}
	var packages []string
	for _, field := range strings.Fields(match[2]) {
		if strings.HasPrefix(field, "./") {
			packages = append(packages, field)
		}
	}
	return match[1], packages, true
}

func parseLifecycleGoRunCommandForTest(command string) (string, bool) {
	fields := strings.Fields(strings.TrimSpace(command))
	if len(fields) < 3 || fields[0] != "go" || fields[1] != "run" {
		return "", false
	}
	return fields[2], true
}

func parseLifecycleGoBuildCommandForTest(command string) (string, bool) {
	fields := strings.Fields(strings.TrimSpace(command))
	if len(fields) != 5 || fields[0] != "go" || fields[1] != "build" || fields[2] != "-o" {
		return "", false
	}
	if !strings.HasPrefix(fields[3], "/tmp/") {
		return "", false
	}
	if !strings.HasPrefix(fields[4], "./") {
		return "", false
	}
	return fields[4], true
}

func parseLifecycleBunRunCommandForTest(command string) (string, string, bool) {
	fields := strings.Fields(strings.TrimSpace(command))
	if len(fields) != 5 || fields[0] != "bun" || fields[1] != "run" || fields[2] != "--cwd" {
		return "", "", false
	}
	return fields[3], fields[4], true
}

func parseLifecycleBunScriptCommandForTest(command string) (string, bool) {
	fields := strings.Fields(strings.TrimSpace(command))
	if len(fields) != 2 || fields[0] != "bun" {
		return "", false
	}
	if !strings.HasPrefix(fields[1], "dev/") || !strings.HasSuffix(fields[1], ".mjs") {
		return "", false
	}
	return fields[1], true
}

func isLifecycleGitCommandForTest(command string) bool {
	switch strings.TrimSpace(command) {
	case "git status --short --branch", "git remote get-url private":
		return true
	default:
		return false
	}
}

func splitLifecycleRunAlternativesForTest(pattern string) []string {
	var alternatives []string
	for _, alternative := range strings.Split(pattern, "|") {
		alternative = strings.TrimSpace(alternative)
		if alternative != "" {
			alternatives = append(alternatives, alternative)
		}
	}
	if len(alternatives) == 0 {
		return []string{pattern}
	}
	return alternatives
}

func lifecyclePackageScriptsForTest(t *testing.T, path string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read package scripts %s: %v", path, err)
	}
	var pkg struct {
		Scripts map[string]string `json:"scripts"`
	}
	if err := json.Unmarshal(raw, &pkg); err != nil {
		t.Fatalf("parse package scripts %s: %v", path, err)
	}
	return pkg.Scripts
}

func lifecycleTestNamesForPackagePattern(t *testing.T, packagePattern string) []string {
	t.Helper()
	if !strings.HasPrefix(packagePattern, "./") {
		t.Fatalf("unsupported checklist package pattern %q", packagePattern)
	}
	root := repoRootForAuditChecklistTest(t)
	rel := strings.TrimPrefix(packagePattern, "./")
	var names []string
	if strings.HasSuffix(rel, "/...") {
		base := filepath.Join(root, strings.TrimSuffix(rel, "/..."))
		if err := filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(info.Name(), "_test.go") {
				return nil
			}
			names = append(names, lifecycleTestNamesFromFile(t, path)...)
			return nil
		}); err != nil {
			t.Fatalf("walk checklist package pattern %q: %v", packagePattern, err)
		}
		return names
	}
	dir := filepath.Join(root, rel)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read checklist package %q: %v", packagePattern, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		names = append(names, lifecycleTestNamesFromFile(t, filepath.Join(dir, entry.Name()))...)
	}
	return names
}

func lifecycleTestNamesFromFile(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test file %s: %v", path, err)
	}
	matches := regexp.MustCompile(`func\s+(Test[A-Za-z0-9_]+)\s*\(`).FindAllStringSubmatch(string(raw), -1)
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		names = append(names, match[1])
	}
	return names
}

func repoRootForAuditChecklistTest(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find repo root from %s", dir)
		}
		dir = parent
	}
}

func writeAuditFixtureBinary(t *testing.T, extraStrings ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "amp-fixture")
	body := "0.0.1-gabcdef0\x00assistant:message\x00threadActor\x00"
	for _, value := range extraStrings {
		body += value + "\x00"
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeJSONFile(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func readJSONSnapshotFile(path string) (Snapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, err
	}
	var snapshot Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func sampleDiffValue(category string) string {
	switch category {
	case "binary-source":
		return "sha256=new"
	case "routes":
		return "/api/internal"
	case "route-methods":
		return "/api/internal=GET=remote-web"
	case "route-coverage":
		return "/api/internal=remote-web"
	case "thread-delta-events":
		return "assistant:message-update"
	case "thread-delta-coverage":
		return "assistant:message-update=assistant-message"
	case "thread-reader-markers":
		return "read_messages"
	case "thread-reader-coverage":
		return "read_messages=message-reader-route"
	case "tool-cancel-reasons":
		return "user:interrupted"
	case "tool-cancel-coverage":
		return "user:interrupted=user-interrupt"
	case "tool-run-statuses":
		return "blocked-on-user"
	case "tool-run-coverage":
		return "blocked-on-user=pending"
	case "tool-catalog-markers":
		return "builtin:edit_file"
	case "tool-catalog-coverage":
		return "builtin:edit_file=file-edit"
	case "stream-json-markers":
		return "error_during_execution"
	case "stream-json-coverage":
		return "error_during_execution=error-subtype"
	case "mode-setting-markers":
		return "lastSpeedByMode"
	case "mode-setting-coverage":
		return "lastSpeedByMode=session-default"
	case "provider-protocol-markers":
		return "x-amp-feature"
	case "provider-protocol-coverage":
		return "x-amp-feature=amp-provider-header"
	case "review-contract-markers":
		return "review-cli-appends-check-findings"
	case "agent-mode-profiles":
		return "smart|primary=CLAUDE_OPUS_4_8|reasoning=high"
	case "agent-mode-routes":
		return "smart|provider=anthropic|model=claude-opus-4-8"
	case "agent-mode-coverage":
		return "smart=local-runtime"
	case "settings":
		return "painter.model"
	case "setting-defaults":
		return "painter.model=gpt-image-2=local-runtime"
	case "setting-coverage":
		return "showCosts=remote-web"
	case "models":
		return "claude-opus-4-8"
	case "model-limits":
		return "claude-opus-4-8|enum=CLAUDE_OPUS_4_8|provider=anthropic"
	case "large-context-rules":
		return "CLAUDE_OPUS_4_6|alias=claude-opus-4-6-1m"
	case "adaptive-thinking-rules":
		return "CLAUDE_OPUS_4_8|models=claude-opus-4-8"
	case "provider-reasoning-rules":
		return "anthropic|sources=setting:reasoning.effort"
	case "provider-header-rules":
		return "anthropic|feature=x-amp-feature:amp.chat"
	case "provider-feature-rules":
		return "amp.review|provider=google|callsite=code-review"
	case "compaction-rules":
		return "anthropic-tool-runner|provider=anthropic|threshold=100000"
	case "model-coverage":
		return "claude-opus-4-8=anthropic/claude-opus"
	case "actor-runtime-markers":
		return "threadStatusUpdated"
	case "actor-runtime-coverage":
		return "threadStatusUpdated=local-runtime"
	case "prompt-fingerprint-metadata":
		return "same|len=260|kind=prompt|tags=guidance"
	case "prompt-kind-counts":
		return "prompt=121"
	case "prompt-tag-counts":
		return "prompt/guidance=22"
	default:
		return "changed"
	}
}

func diffCategory(t *testing.T, diff auditDiff, name string) categoryDiff {
	t.Helper()
	for _, category := range diff.Categories {
		if category.Name == name {
			return category
		}
	}
	t.Fatalf("missing diff category %s in %#v", name, diff.Categories)
	return categoryDiff{}
}
