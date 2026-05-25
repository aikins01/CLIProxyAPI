<script lang="ts">
  import {
    ArrowLeft,
    ArrowUp,
    CheckCircle2,
    ChevronRight,
    Copy,
    FileCode2,
    Gauge,
    GitBranch,
    KeyRound,
    Laptop,
    List,
    Loader2,
    LogOut,
    Moon,
    PanelRight,
    Search,
    Send,
    Sparkles,
    Sun,
    Wrench,
    Wifi,
    WifiOff
  } from 'lucide-svelte';
  import { pushState, replaceState } from '$app/navigation';
  import { onMount } from 'svelte';
  import PatchDiff from '$lib/PatchDiff.svelte';

  type Theme = 'system' | 'dark' | 'light';
  type AgentState = 'idle' | 'working' | 'streaming' | 'awaiting_approval' | 'error' | string;

  type ContentBlock = {
    type: string;
    text?: string;
    thinking?: string;
    name?: string;
    id?: string;
    input?: unknown;
    content?: unknown;
    patch?: unknown;
    diff?: unknown;
    blockState?: string;
    startTime?: number | string;
    finalTime?: number | string;
    timestamp?: number | string;
  };

  type NeoMessage = {
    threadId?: string;
    messageId: string;
    role: 'user' | 'assistant' | 'system' | string;
    content: ContentBlock[];
    state?: { type?: string; stopReason?: string };
    agentMode?: string;
    reasoningEffort?: string;
    usage?: Record<string, unknown>;
  };

  type MessageSegment =
    | { kind: 'text'; block: ContentBlock; key: string }
    | { kind: 'work'; blocks: ContentBlock[]; key: string; live: boolean };

  type TranscriptItem =
    | { kind: 'user'; message: NeoMessage; key: string }
    | { kind: 'assistant'; messages: NeoMessage[]; key: string };

  type ThreadSummary = {
    id: string;
    title: string;
    repo: string;
    branch: string;
    preview: string;
    messageCount: number;
    agentMode: string;
    updatedLabel: string;
    diffLabel?: string;
    archived?: boolean;
  };

  type ThreadDetail = {
    id: string;
    title: string;
    repo: string;
    branch: string;
    agentMode: string;
    messages: NeoMessage[];
    contextLabel: string;
    contextUsage?: { used: number; total: number };
    cost?: { amount: number; free?: number; paid?: number; included?: boolean; url?: string };
    costBreakdownURL?: string;
    handoffFrom?: { threadId: string; instructions?: string };
  };

  type Incoming = Record<string, unknown>;
  type QueuedMessage = {
    id: string;
    messageId: string;
    preview: string;
    steer: boolean;
  };
  type ToolApproval = {
    toolCallId: string;
    name: string;
    message: string;
    input: Record<string, unknown>;
  };
  type RuntimeArtifact = {
    key: string;
    dataType: string;
    updatedAt: string;
    preview: string;
  };
  type Relationship = {
    threadID: string;
    type: string;
    role: string;
    comment: string;
  };
  type ExecutorStatus = {
    id: string;
    status: string;
    message: string;
    details: Record<string, unknown>;
  };
  type ToolLease = {
    toolCallId: string;
    toolName: string;
    messageId: string;
    args: Record<string, unknown>;
  };
  type MobilePane = 'threads' | 'thread';
  type OpenThreadOptions = {
    replaceURL?: boolean;
    skipURL?: boolean;
  };

  let theme = $state<Theme>('system');
  let apiKey = $state('');
  let keyDraft = $state('');
  let isAuthenticated = $state(false);
  let authLoading = $state(false);
  let mobilePane = $state<MobilePane>('threads');
  let query = $state('');
  let threads = $state<ThreadSummary[]>([]);
  let selectedThreadId = $state('');
  let detail = $state<ThreadDetail | null>(null);
  let loadingThreads = $state(false);
  let loadingThread = $state(false);
  let connection = $state<'offline' | 'connecting' | 'connected'>('offline');
  let agentState = $state<AgentState>('idle');
  let composer = $state('');
  let socket: WebSocket | null = null;
  let lastError = $state('');
  let queuedCount = $state(0);
  let activeError = $state<Record<string, unknown> | null>(null);
  let threadStatus = $state('');
  let runtimeSettings = $state<Record<string, unknown>>({});
  let environment = $state<Record<string, unknown>>({});
  let queuedMessages = $state<QueuedMessage[]>([]);
  let toolApprovals = $state<ToolApproval[]>([]);
  let compactionActive = $state(false);
  let compactionRecords = $state<Record<string, unknown>[]>([]);
  let artifacts = $state<RuntimeArtifact[]>([]);
  let relationships = $state<Relationship[]>([]);
  let executorConnected = $state(false);
  let executorInfo = $state<Record<string, unknown>>({});
  let executorStatuses = $state<ExecutorStatus[]>([]);
  let inferenceTools = $state<{ messageId: string; agentMode: string; tools: string[] } | null>(null);
  let toolLeases = $state<ToolLease[]>([]);
  let draftPreview = $state('');
  let pendingNavigation = $state('');
  let mainThreadId = $state('');
  let maxTokensLabel = $state('');
  let retryNotice = $state('');

  // Mobile inspector drawer
  let mobileInspectorOpen = $state(false);

  const filteredThreads = $derived(
    threads.filter((thread) => {
      const haystack = `${thread.title} ${thread.repo} ${thread.branch} ${thread.preview}`.toLowerCase();
      return haystack.includes(query.trim().toLowerCase());
    })
  );
  const inspectorContextUsage = $derived(contextUsageForDetail(detail));
  const inspectorCost = $derived(detail?.cost);

  function handleEscape(event: KeyboardEvent) {
    if (event.key !== 'Escape') return;
    if (mobileInspectorOpen) { mobileInspectorOpen = false; return; }
  }

  const activeMessages = $derived.by(() => dedupeReplayMessages(detail?.messages ?? []));
  const transcriptItems = $derived.by(() => groupTranscriptItems(activeMessages));
  // Per-message jump nav: only user turns are anchors (matches ampcode pattern of one mark per turn)
  const userTurnAnchors = $derived(
    activeMessages
      .filter((m) => m.role === 'user' && userTextFromBlocks(m.content).trim().length > 0)
      .map((m) => ({
        messageId: m.messageId,
        preview: userTextFromBlocks(m.content).replace(/\s+/g, ' ').trim().slice(0, 120),
      }))
  );
  let msgNavHover = $state(false);
  let msgNavHoverTimer: ReturnType<typeof setTimeout> | null = null;
  function msgNavEnter() {
    if (msgNavHoverTimer) { clearTimeout(msgNavHoverTimer); msgNavHoverTimer = null; }
    msgNavHover = true;
  }
  function msgNavLeave() {
    if (msgNavHoverTimer) clearTimeout(msgNavHoverTimer);
    msgNavHoverTimer = setTimeout(() => { msgNavHover = false; msgNavHoverTimer = null; }, 250);
  }
  function scrollToMsg(messageId: string) {
    const el = document.querySelector(`[data-message-id='${messageId}']`);
    if (el) (el as HTMLElement).scrollIntoView({ behavior: 'smooth', block: 'center' });
  }
  // Handoff marker derived from current messages (messages stream in after initial detail load).
  // Use the thread title as the displayed "Instructions:" sentence, matching ampcode.
  const handoffFrom = $derived.by(() => {
    const base = handoffFromMessages(activeMessages);
    if (!base) return undefined;
    return { threadId: base.threadId, instructions: detail?.title ?? base.instructions };
  });
  const selectedSummary = $derived(threads.find((thread) => thread.id === selectedThreadId));
  const threadCommand = $derived(`amp threads continue ${detail?.id ?? (selectedThreadId || 'T-...')}`);

  onMount(() => {
    const savedTheme = localStorage.getItem('neo-remote-theme') as Theme | null;
    if (savedTheme === 'dark' || savedTheme === 'light' || savedTheme === 'system') {
      theme = savedTheme;
    }
    apiKey = localStorage.getItem('neo-remote-api-key') ?? '';
    keyDraft = apiKey;
    const urlThread = threadIdFromURL();
    if (urlThread) {
      selectedThreadId = urlThread;
      mobilePane = 'thread';
    }
    if (apiKey.trim()) {
      isAuthenticated = true;
      void refreshThreads(urlThread);
    }
    const handlePopState = () => {
      const nextThread = threadIdFromURL();
      if (nextThread) {
        mobilePane = 'thread';
        void openThread(nextThread, { skipURL: true });
        return;
      }
      selectedThreadId = '';
      detail = null;
      mobilePane = 'threads';
      disconnect();
    };
    addEventListener('popstate', handlePopState);
    addEventListener('keydown', handleEscape);
    return () => {
      removeEventListener('popstate', handlePopState);
      removeEventListener('keydown', handleEscape);
      disconnect();
    };
  });

  $effect(() => {
    if (typeof document !== 'undefined') {
      document.documentElement.dataset.theme = theme;
      localStorage.setItem('neo-remote-theme', theme);
    }
  });

  // Lock body scroll while mobile sheet is open
  $effect(() => {
    if (typeof document === 'undefined') return;
    if (mobileInspectorOpen) {
      const prev = document.body.style.overflow;
      document.body.style.overflow = 'hidden';
      return () => { document.body.style.overflow = prev; };
    }
  });

  function persistAPIKey() {
    localStorage.setItem('neo-remote-api-key', apiKey);
  }

  function resetRuntimeState() {
    queuedCount = 0;
    activeError = null;
    threadStatus = '';
    runtimeSettings = {};
    environment = {};
    queuedMessages = [];
    toolApprovals = [];
    compactionActive = false;
    compactionRecords = [];
    artifacts = [];
    relationships = [];
    executorConnected = false;
    executorInfo = {};
    executorStatuses = [];
    inferenceTools = null;
    toolLeases = [];
    draftPreview = '';
    pendingNavigation = '';
    mainThreadId = '';
    maxTokensLabel = '';
    retryNotice = '';
  }

  async function submitKey() {
    const key = keyDraft.trim();
    if (!key) {
      lastError = 'Enter your access key to continue.';
      return;
    }
    authLoading = true;
    lastError = '';
    apiKey = key;
    persistAPIKey();
    try {
      const result = await rpc('listThreads', { includeArchived: false, limit: 80 });
      const rawThreads = Array.isArray(result?.threads) ? result.threads : [];
      threads = rawThreads.map(threadSummaryFromAPI).filter(Boolean) as ThreadSummary[];
      isAuthenticated = true;
      lastError = '';
      const targetThreadId = threadIdFromURL() || selectedThreadId || threads[0]?.id || '';
      if (targetThreadId) {
        void openThread(targetThreadId, { replaceURL: true });
      }
    } catch (error) {
      const status = error instanceof RpcError ? error.status : 0;
      if (status === 401 || status === 403) {
        apiKey = '';
        localStorage.removeItem('neo-remote-api-key');
        lastError = 'That key was rejected. Double-check and try again.';
      } else {
        isAuthenticated = true;
        lastError = error instanceof Error ? error.message : String(error);
      }
    } finally {
      authLoading = false;
    }
  }

  function forgetKey() {
    apiKey = '';
    keyDraft = '';
    isAuthenticated = false;
    lastError = '';
    threads = [];
    selectedThreadId = '';
    detail = null;
    resetRuntimeState();
    localStorage.removeItem('neo-remote-api-key');
    syncThreadURL('', true);
    disconnect();
  }

  function headers() {
    const next: Record<string, string> = { 'Content-Type': 'application/json' };
    if (apiKey.trim()) {
      next.Authorization = `Bearer ${apiKey.trim()}`;
    }
    return next;
  }

  class RpcError extends Error {
    status: number;
    method: string;
    constructor(method: string, status: number, message?: string) {
      super(message || `${method} failed${status ? `: HTTP ${status}` : ''}`);
      this.name = 'RpcError';
      this.method = method;
      this.status = status;
    }
  }

  async function rpc(method: string, params: Record<string, unknown>) {
    const response = await fetch(`/api/internal?${encodeURIComponent(method)}`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({ method, params })
    });
    if (!response.ok) {
      throw new RpcError(method, response.status);
    }
    const data = await response.json();
    if (data?.ok === false) {
      throw new RpcError(method, 0, `${method} failed`);
    }
    return data?.result ?? data;
  }

  async function refreshThreadUsageInfo(threadId: string) {
    if (!threadId) return;
    try {
      const displayCost = await rpc('threadDisplayCostInfo', { threadID: threadId });
      applyThreadUsageInfo(threadId, displayCost);
    } catch {
      // Cost display info is advisory; message usage still drives the context label.
    }
    try {
      const response = await fetch(`/api/threads/${encodeURIComponent(threadId)}/usage`, {
        headers: headers()
      });
      if (!response.ok) return;
      applyThreadUsageInfo(threadId, await response.json());
    } catch {
      // Usage snapshots are refreshed opportunistically; live deltas keep usage current.
    }
  }

  function applyThreadUsageInfo(threadId: string, raw: unknown) {
    if (!detail || detail.id !== threadId) return;
    const item = asRecord(raw);
    const contextUsage = contextUsageFrom(item);
    const cost = costFrom(item);
    const costBreakdownURL = costBreakdownURLFrom(item);
    detail = {
      ...detail,
      contextUsage: contextUsage ?? detail.contextUsage,
      cost: mergeCost(detail.cost, cost, costBreakdownURL),
      costBreakdownURL: costBreakdownURL || detail.costBreakdownURL
    };
  }

  async function refreshThreads(preferredThreadId = selectedThreadId) {
    if (!apiKey.trim()) {
      isAuthenticated = false;
      return false;
    }
    loadingThreads = true;
    lastError = '';
    try {
      const result = await rpc('listThreads', { includeArchived: true, limit: 120 });
      const rawThreads = Array.isArray(result?.threads) ? result.threads : [];
      threads = rawThreads.map(threadSummaryFromAPI).filter(Boolean) as ThreadSummary[];
      const targetThreadId = preferredThreadId || selectedThreadId || threads[0]?.id || '';
      if (targetThreadId) {
        await openThread(targetThreadId, { replaceURL: true });
      }
      return true;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      return false;
    } finally {
      loadingThreads = false;
    }
  }

  async function selectThread(threadId: string) {
    mobilePane = 'thread';
    await openThread(threadId);
  }

  async function openThread(threadId: string, options: OpenThreadOptions = {}) {
    selectedThreadId = threadId;
    if (!options.skipURL) {
      syncThreadURL(threadId, Boolean(options.replaceURL));
    }
    loadingThread = true;
    lastError = '';
    resetRuntimeState();
    disconnect();
    try {
      const result = await rpc('getThread', { thread: threadId });
      const thread = normalizeThreadPayload(result);
      detail = threadDetailFromAPI(thread);
      void refreshThreadUsageInfo(threadId);
      connect(threadId, Number(thread?.v ?? detail.messages.length));
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      detail = threadDetailFromSummary(threads.find((thread) => thread.id === threadId));
      connect(threadId, 0);
    } finally {
      loadingThread = false;
    }
  }

  function connect(threadId: string, version = 0) {
    if (!threadId || typeof WebSocket === 'undefined') return;
    disconnect();
    connection = 'connecting';
    const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
    const runtimeKey = encodeURIComponent(apiKey.trim());
    const authQuery = runtimeKey ? `&auth_token=${runtimeKey}` : '';
    const url = `${scheme}://${location.host}/gateway/threadActor/?rvt-method=getOrCreate&rvt-key=${encodeURIComponent(threadId)}&rvt-skip-ready-wait=true${authQuery}`;
    socket = new WebSocket(url, ['rivet', 'rivet_encoding.4', 'rivet_skip_ready_wait']);
    socket.addEventListener('open', () => {
      connection = 'connected';
      sendFrame({ type: 'client_resume', version });
    });
    socket.addEventListener('close', () => {
      connection = 'offline';
    });
    socket.addEventListener('error', () => {
      connection = 'offline';
      lastError = 'WebSocket connection failed';
    });
    socket.addEventListener('message', (event) => {
      if (event.data === 'pong') return;
      try {
        const decoded = JSON.parse(String(event.data));
        const messages = Array.isArray(decoded) ? decoded : [decoded];
        for (const message of messages) {
          if (message && typeof message === 'object') {
            applyIncoming(message as Incoming);
          }
        }
      } catch {
        // Rivet keepalives and binary frames are ignored by the local JSON bridge.
      }
    });
  }

  function disconnect() {
    if (socket) {
      socket.close();
      socket = null;
    }
    connection = 'offline';
  }

  function sendFrame(payload: Incoming) {
    if (!socket || socket.readyState !== WebSocket.OPEN) return false;
    socket.send(JSON.stringify(payload));
    return true;
  }

  function sendMessage() {
    const text = composer.trim();
    if (!text || !detail) return;
    const messageId = `M-local-${crypto.randomUUID()}`;
    const message: NeoMessage = {
      threadId: detail.id,
      messageId,
      role: 'user',
      content: [{ type: 'text', text }]
    };
    detail = { ...detail, messages: [...detail.messages, message] };
    composer = '';
    sendFrame({
      type: 'client_append_user_msg',
      messageId,
      agentMode: detail.agentMode || 'smart',
      content: message.content
    });
  }

  function applyIncoming(message: Incoming) {
    const type = String(message.type ?? '');
    if (type === 'message_added') {
      const next = normalizeMessage(message.message);
      if (next) upsertMessage(next, false);
      return;
    }
    if (type === 'message_updated') {
      const next = normalizeMessage(message.message);
      if (next) upsertMessage(next, true);
      return;
    }
    if (type === 'delta') {
      applyDelta(message);
      return;
    }
    if (type === 'agent_state') {
      agentState = String(message.state ?? 'idle');
      return;
    }
    if (type === 'thread_settings') {
      runtimeSettings = asRecord(message.settings);
      if (detail) {
        const nextMode = stringFrom(runtimeSettings.agentMode) || detail.agentMode;
        detail = { ...detail, agentMode: nextMode };
      }
      return;
    }
    if (type === 'queued_messages') {
      queuedMessages = (Array.isArray(message.messages) ? message.messages : []).map(queuedMessageFromAny);
      queuedCount = queuedMessages.length;
      return;
    }
    if (type === 'queued_message_added') {
      const item = queuedMessageFromAny(message.message);
      queuedMessages = [...queuedMessages.filter((queued) => queued.id !== item.id), item];
      queuedCount = queuedMessages.length;
      return;
    }
    if (type === 'queued_message_removed' || type === 'queued_message_dequeued') {
      const id = stringFrom(message.queuedMessageId ?? message.messageId);
      queuedMessages = queuedMessages.filter((queued) => queued.id !== id && queued.messageId !== id);
      queuedCount = queuedMessages.length;
      return;
    }
    if (type === 'tool_approval_queue') {
      toolApprovals = (Array.isArray(message.approvals) ? message.approvals : []).map(approvalFromAny).filter((approval) => approval.toolCallId);
      return;
    }
    if (type === 'tool_lease') {
      const lease = toolLeaseFromAny(message);
      if (lease.toolCallId) {
        toolLeases = [...toolLeases.filter((item) => item.toolCallId !== lease.toolCallId), lease];
      }
      return;
    }
    if (type === 'executor_tool_lease_revoked' || type === 'executor_tool_result_ack') {
      const id = stringFrom(message.toolCallId ?? message.toolUseId ?? message.id);
      toolLeases = toolLeases.filter((lease) => lease.toolCallId !== id);
      return;
    }
    if (type === 'tool:processed' || type === 'tool:data') {
      const id = stringFrom(message.toolCallId ?? message.toolUseId ?? message.id);
      if (id && type === 'tool:processed') {
        toolLeases = toolLeases.filter((lease) => lease.toolCallId !== id);
      }
      return;
    }
    if (type === 'executor_connected') {
      executorConnected = true;
      executorInfo = {
        executorId: message.executorId,
        registeredToolCount: message.registeredToolCount,
        guidanceInventory: message.guidanceInventory
      };
      return;
    }
    if (type === 'executor_disconnected') {
      executorConnected = false;
      return;
    }
    if (type === 'executor_status') {
      const status = executorStatusFromAny(message);
      executorStatuses = [status, ...executorStatuses.filter((item) => item.id !== status.id)].slice(0, 5);
      return;
    }
    if (type === 'executor_error') {
      activeError = { message: message.message, code: message.code };
      return;
    }
    if (type === 'error_set') {
      activeError = asRecord(message.error);
      return;
    }
    if (type === 'error_cleared') {
      activeError = null;
      return;
    }
    if (type === 'thread_status') {
      threadStatus = stringFrom(message.status);
      return;
    }
    if (type === 'compaction_started') {
      compactionActive = true;
      return;
    }
    if (type === 'compaction_complete') {
      compactionActive = false;
      return;
    }
    if (type === 'compaction_records') {
      compactionRecords = (Array.isArray(message.records) ? message.records : []).map(asRecord);
      return;
    }
    if (type === 'artifacts_snapshot') {
      artifacts = artifactsFromAny(message.artifacts);
      return;
    }
    if (type === 'artifact_upserted') {
      const artifact = artifactFromAny(message.artifact);
      if (artifact.key) {
        artifacts = [...artifacts.filter((item) => item.key !== artifact.key), artifact];
      }
      return;
    }
    if (type === 'artifact_deleted') {
      const key = stringFrom(message.key);
      artifacts = artifacts.filter((artifact) => artifact.key !== key);
      return;
    }
    if (type === 'thread_relationships') {
      relationships = (Array.isArray(message.relationships) ? message.relationships : []).map(relationshipFromAny).filter((item) => item.threadID);
      return;
    }
    if (type === 'environment_update') {
      environment = asRecord(message.environment);
      return;
    }
    if (type === 'draft') {
      draftPreview = contentPreview(message.content);
      return;
    }
    if (type === 'setPendingNavigation') {
      pendingNavigation = stringFrom(message.threadID ?? message.threadId ?? message.value);
      return;
    }
    if (type === 'clearPendingNavigation') {
      pendingNavigation = '';
      return;
    }
    if (type === 'max-tokens') {
      maxTokensLabel = valueLabel(message.value);
      return;
    }
    if (type === 'main-thread') {
      mainThreadId = stringFrom(message.value ?? message.threadID ?? message.threadId);
      return;
    }
    if (type === 'inference_tools') {
      inferenceTools = {
        messageId: stringFrom(message.messageId),
        agentMode: stringFrom(message.agentMode),
        tools: stringArray(message.tools)
      };
      return;
    }
    if (type === 'cancelled') {
      toolApprovals = [];
      toolLeases = [];
      retryNotice = '';
      return;
    }
    if (type === 'retry_scheduled') {
      retryNotice = `Retry ${valueLabel(message.attempt)}/${valueLabel(message.maxAttempts)}: ${stringFrom(message.reason) || 'scheduled'}`;
      return;
    }
    if (type === 'retry_started' || type === 'retry_cancelled') {
      retryNotice = type === 'retry_started' ? 'Retry started' : '';
      return;
    }
    if (type === 'thread_title' && detail) {
      detail = { ...detail, title: String(message.title ?? detail.title) };
    }
  }

  function upsertMessage(message: NeoMessage, replace: boolean) {
    if (!detail) return;
    const index = detail.messages.findIndex((item) => item.messageId === message.messageId);
    if (index === -1) {
      detail = { ...detail, messages: [...detail.messages, message] };
      return;
    }
    const next = [...detail.messages];
    const usage = mergeUsage(next[index].usage, message.usage);
    next[index] = replace ? { ...message, usage } : { ...next[index], ...message, usage };
    detail = { ...detail, messages: next };
  }

  function applyDelta(delta: Incoming) {
    if (!detail) return;
    const messageId = String(delta.messageId ?? '');
    if (!messageId) return;
    const role = String(delta.role ?? 'assistant');
    const state = String(delta.state ?? '');
    let nextMessages = [...detail.messages];
    let index = nextMessages.findIndex((item) => item.messageId === messageId);
    if (index === -1) {
      nextMessages.push({
        threadId: detail.id,
        messageId,
        role,
        content: [],
        state: { type: state || 'streaming' }
      });
      index = nextMessages.length - 1;
    }
    const current = nextMessages[index];
    const blocks = Array.isArray(delta.blocks) ? (delta.blocks as ContentBlock[]) : [];
    const blockIndex = typeof delta.blockIndex === 'number' ? delta.blockIndex : current.content.length;
    const content = [...current.content];
    for (let offset = 0; offset < blocks.length; offset += 1) {
      const incoming = blocks[offset];
      const targetIndex = blockIndex + offset;
      const previous = content[targetIndex];
      content[targetIndex] = mergeBlock(previous, incoming);
    }
    nextMessages[index] = {
      ...current,
      role,
      content,
      state: state ? { type: state } : current.state,
      usage: mergeUsage(current.usage, asRecord(delta.usage))
    };
    detail = { ...detail, messages: nextMessages };
  }

  function mergeBlock(previous: ContentBlock | undefined, incoming: ContentBlock): ContentBlock {
    if (!previous) return { ...incoming };
    if (incoming.type === 'text') {
      return { ...previous, ...incoming, text: `${previous.text ?? ''}${incoming.text ?? ''}` };
    }
    if (incoming.type === 'thinking') {
      return { ...previous, ...incoming, thinking: `${previous.thinking ?? ''}${incoming.thinking ?? ''}` };
    }
    return { ...previous, ...incoming };
  }

  function queuedMessageFromAny(raw: unknown): QueuedMessage {
    const item = asRecord(raw);
    const queued = asRecord(item.queuedMessage);
    const source = Object.keys(queued).length ? queued : item;
    const messageId = stringFrom(source.messageId ?? item.messageId);
    const id = stringFrom(item.id ?? item.queuedMessageId ?? messageId);
    return {
      id,
      messageId,
      preview: contentPreview(source.content),
      steer: Boolean(item.steer ?? source.steer)
    };
  }

  function approvalFromAny(raw: unknown): ToolApproval {
    const item = asRecord(raw);
    const input = asRecord(item.input ?? item.args);
    const name = stringFrom(item.toolName ?? item.name ?? input.command ?? input.path);
    return {
      toolCallId: stringFrom(item.toolCallId ?? item.toolUseId ?? item.id),
      name: name || 'tool approval',
      message: stringFrom(item.message ?? item.reason ?? item.prompt),
      input
    };
  }

  function toolLeaseFromAny(raw: unknown): ToolLease {
    const item = asRecord(raw);
    return {
      toolCallId: stringFrom(item.toolCallId ?? item.toolUseId ?? item.id),
      toolName: stringFrom(item.toolName ?? item.name) || 'tool',
      messageId: stringFrom(item.messageId),
      args: asRecord(item.args ?? item.input)
    };
  }

  function executorStatusFromAny(raw: unknown): ExecutorStatus {
    const item = asRecord(raw);
    const id = stringFrom(item.spawnId ?? item.executorId ?? item.requestId) || `status-${Date.now()}`;
    return {
      id,
      status: stringFrom(item.status) || 'unknown',
      message: stringFrom(item.message),
      details: asRecord(item.details)
    };
  }

  function artifactsFromAny(raw: unknown): RuntimeArtifact[] {
    if (Array.isArray(raw)) return raw.map(artifactFromAny).filter((artifact) => artifact.key);
    return Object.entries(asRecord(raw)).map(([key, value]) => artifactFromAny({ key, ...asRecord(value) })).filter((artifact) => artifact.key);
  }

  function artifactFromAny(raw: unknown): RuntimeArtifact {
    const item = asRecord(raw);
    return {
      key: stringFrom(item.key ?? item.id ?? item.path),
      dataType: stringFrom(item.dataType ?? item.data_type) || 'artifact',
      updatedAt: stringFrom(item.updatedAt),
      preview: artifactPreview(item)
    };
  }

  function relationshipFromAny(raw: unknown): Relationship {
    const item = asRecord(raw);
    return {
      threadID: stringFrom(item.threadID ?? item.threadId ?? item.id),
      type: stringFrom(item.type) || 'relationship',
      role: stringFrom(item.role),
      comment: stringFrom(item.comment)
    };
  }

  function approveTool(approval: ToolApproval) {
    sendFrame({ type: 'client_tool_approval_response', toolCallId: approval.toolCallId, accepted: true });
    toolApprovals = toolApprovals.filter((item) => item.toolCallId !== approval.toolCallId);
  }

  function denyTool(approval: ToolApproval) {
    sendFrame({
      type: 'client_tool_approval_response',
      toolCallId: approval.toolCallId,
      accepted: false,
      denyFeedback: 'Denied from Neo Remote'
    });
    toolApprovals = toolApprovals.filter((item) => item.toolCallId !== approval.toolCallId);
  }

  function contentPreview(raw: unknown) {
    const blocks = Array.isArray(raw) ? raw.map((part) => asRecord(part) as ContentBlock) : [];
    if (blocks.length === 0) {
      return typeof raw === 'string' ? raw : '';
    }
    return textFromBlocks(blocks).replace(/\s+/g, ' ').trim();
  }

  function artifactPreview(item: Record<string, unknown>) {
    if (typeof item.content === 'string') return item.content.slice(0, 180);
    if (typeof item.text === 'string') return item.text.slice(0, 180);
    const encoded = stringFrom(item.contentBase64);
    if (!encoded || typeof atob === 'undefined') return '';
    try {
      return atob(encoded).slice(0, 180);
    } catch {
      return '';
    }
  }

  function valueLabel(value: unknown) {
    if (value == null || value === '') return '';
    if (typeof value === 'number') return new Intl.NumberFormat().format(value);
    return String(value);
  }

  function stringArray(value: unknown) {
    return (Array.isArray(value) ? value : []).map(stringFrom).filter(Boolean);
  }

  function objectSummary(value: Record<string, unknown>) {
    return Object.entries(value)
      .filter(([, item]) => item !== undefined && item !== null && item !== '')
      .slice(0, 4)
      .map(([key, item]) => `${key}: ${valueLabel(item)}`)
      .join(', ');
  }

  function environmentLabel() {
    const repo = repoFromEnv(environment);
    const branch = branchFromEnv(environment);
    return [repo, branch].filter(Boolean).join(':') || objectSummary(environment) || 'local context';
  }

  function runtimeWorkspacePath() {
    const initial = asRecord(environment.initial);
    const trees = Array.isArray(initial.trees) ? initial.trees : Array.isArray(environment.trees) ? environment.trees : [];
    const first = asRecord(trees[0]);
    return (
      stringFrom(environment.workingDirectory) ||
      stringFrom(environment.working_directory) ||
      stringFrom(environment.workspaceRoot) ||
      stringFrom(environment.cwd) ||
      filePathFromURI(stringFrom(first.uri)) ||
      stringFrom(first.path) ||
      stringFrom(first.root)
    );
  }

  function filePathFromURI(value: string) {
    if (!value) return '';
    if (!value.startsWith('file://')) return value;
    try {
      return decodeURIComponent(value.replace(/^file:\/\//, ''));
    } catch {
      return value.replace(/^file:\/\//, '');
    }
  }

  function composerStatusMain() {
    if (connection === 'connected') return 'Connected Neo runtime';
    if (connection === 'connecting') return 'Connecting Neo runtime';
    return 'Neo runtime offline';
  }

  function composerStatusParts() {
    return [runtimeWorkspacePath() || repoFromEnv(environment) || detail?.repo, branchFromEnv(environment) || detail?.branch].filter(Boolean);
  }

  function threadRuntimeLabel(threadId: string) {
    if (threadId !== selectedThreadId) return '';
    if (connection === 'connected') return 'Neo active';
    if (connection === 'connecting') return 'Neo connecting';
    return '';
  }

  function normalizeThreadPayload(result: unknown): Record<string, unknown> {
    const root = asRecord(result);
    const thread = asRecord(root.thread);
    const data = asRecord(thread.data);
    return Object.keys(data).length > 0 ? { ...thread, ...data } : thread;
  }

  function threadSummaryFromAPI(raw: unknown): ThreadSummary | null {
    const item = asRecord(raw);
    const data = Object.keys(asRecord(item.data)).length ? asRecord(item.data) : item;
    const id = stringFrom(data.id ?? item.id);
    if (!id) return null;
    const messages = Array.isArray(data.messages) ? data.messages : [];
    const lastMessage = messages[messages.length - 1] as unknown;
    const preview = previewFromMessage(lastMessage) || stringFrom(data.preview) || stringFrom(data.title);
    const env = asRecord(data.env);
    const repo = stringFrom(data.repository) || repoFromEnv(env) || 'local/runtime';
    return {
      id,
      title: stringFrom(data.title) || 'Untitled',
      repo,
      branch: stringFrom(data.branch) || branchFromEnv(env) || 'main',
      preview,
      messageCount: Number(data.messageCount ?? messages.length ?? 0),
      agentMode: stringFrom(data.agentMode) || 'smart',
      updatedLabel: relativeLabel(Number(data.updatedAt ?? data.createdAt ?? Date.now())),
      diffLabel: diffLabelFrom(data),
      archived: Boolean(data.archived)
    };
  }

  function threadDetailFromAPI(thread: Record<string, unknown>): ThreadDetail {
    const messages = Array.isArray(thread.messages)
      ? thread.messages.map(normalizeMessage).filter(Boolean) as NeoMessage[]
      : [];
    const env = asRecord(thread.env);
    return {
      id: stringFrom(thread.id) || selectedThreadId,
      title: stringFrom(thread.title) || 'Untitled',
      repo: repoFromEnv(env) || 'local/runtime',
      branch: branchFromEnv(env) || 'main',
      agentMode: stringFrom(thread.agentMode) || 'smart',
      messages,
      contextLabel: 'local context',
      contextUsage: contextUsageFrom(thread) ?? contextUsageFromMessages(messages),
      cost: costFrom(thread),
      costBreakdownURL: costBreakdownURLFrom(thread),
      handoffFrom: handoffFromMessages(messages),
    };
  }

  function contextUsageFrom(raw: Record<string, unknown>): { used: number; total: number } | undefined {
    const usage = firstRecord(raw.usage, raw.contextUsage, raw.tokens);
    const used = usageInputTokens(usage) ?? finiteNumberFrom(usage.used, usage.input, usage.promptTokens, raw.tokensUsed);
    const total = usageContextWindow(usage) ?? finiteNumberFrom(raw.maxInputTokens, raw.max_input_tokens, raw.contextWindow, raw.context_window);
    if (used !== undefined && total !== undefined && total > 0) return { used, total };
    return undefined;
  }

  function contextUsageForDetail(thread: ThreadDetail | null): { used: number; total: number } | undefined {
    if (!thread) return undefined;
    return contextUsageFromMessages(thread.messages) ?? thread.contextUsage;
  }

  function contextUsageFromMessages(messages: NeoMessage[]): { used: number; total: number } | undefined {
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      const message = messages[index];
      if (message?.role !== 'assistant') continue;
      const usage = asRecord(message.usage);
      const messageUsed = usageInputTokens(usage);
      const messageTotal = usageContextWindow(usage);
      if (messageUsed !== undefined && messageUsed > 0 && messageTotal !== undefined && messageTotal > 0) return { used: messageUsed, total: messageTotal };
    }
    return undefined;
  }

  function usageInputTokens(usage: Record<string, unknown>): number | undefined {
    const total = finiteNumberFrom(usage.totalInputTokens, usage.total_input_tokens);
    if (total !== undefined) return total;
    const input = finiteNumberFrom(usage.inputTokens, usage.input_tokens, usage.prompt_tokens, usage.promptTokenCount, usage.input);
    const cacheCreation = finiteNumberFrom(usage.cacheCreationInputTokens, usage.cache_creation_input_tokens);
    const details = asRecord(usage.prompt_tokens_details);
    const cacheRead = finiteNumberFrom(usage.cacheReadInputTokens, usage.cache_read_input_tokens, usage.cachedContentTokenCount, details.cached_tokens);
    if (input === undefined && cacheCreation === undefined && cacheRead === undefined) return undefined;
    return (input ?? 0) + (cacheCreation ?? 0) + (cacheRead ?? 0);
  }

  function usageContextWindow(usage: Record<string, unknown>): number | undefined {
    return finiteNumberFrom(usage.maxInputTokens, usage.max_input_tokens, usage.contextWindow, usage.context_window, usage.window, usage.total);
  }

  function costFrom(raw: Record<string, unknown>): ThreadDetail['cost'] {
    const usage = asRecord(raw.usage);
    const cost = asRecord(raw.cost);
    const breakdown = firstRecord(raw.costBreakdown, cost.costBreakdown, usage.costBreakdown);
    if (raw.totalCostUSD === null || cost.totalCostUSD === null || usage.totalCostUSD === null) return undefined;
    const amount = finiteNumberFrom(
      raw.totalCostUSD,
      raw.total_cost_usd,
      cost.totalCostUSD,
      cost.total_cost_usd,
      cost.amount,
      usage.totalCostUSD,
      usage.cost,
      usage.amount,
      raw.cost
    );
    const freeUSD = finiteNumberFrom(breakdown.freeUSD, breakdown.free_usd);
    const paidUSD = finiteNumberFrom(breakdown.paidUSD, breakdown.paid_usd);
    if (freeUSD !== undefined || paidUSD !== undefined) {
      const free = freeUSD ?? 0;
      const paid = paidUSD ?? 0;
      if (free === 0 && paid === 0) return undefined;
      const url = costBreakdownURLFrom(raw);
      return {
        amount: amount ?? free + paid,
        free,
        paid,
        included: free > 0 && paid === 0,
        url: url || undefined
      };
    }
    if (amount === undefined || amount === 0) return undefined;
    const included = Boolean(raw.included ?? raw.free ?? cost.included ?? cost.free ?? usage.included ?? usage.free);
    const url = costBreakdownURLFrom(raw);
    return { amount, included, url: url || undefined };
  }

  function mergeCost(
    current: ThreadDetail['cost'],
    incoming: ThreadDetail['cost'],
    url = ''
  ): ThreadDetail['cost'] {
    if (!current && !incoming) return undefined;
    const merged = { ...(current ?? {}), ...(incoming ?? {}) };
    if (url) merged.url = url;
    return Number.isFinite(merged.amount) ? (merged as ThreadDetail['cost']) : undefined;
  }

  function costBreakdownURLFrom(raw: Record<string, unknown>) {
    const usage = asRecord(raw.usage);
    const cost = asRecord(raw.cost);
    return stringFrom(raw.costBreakdownURL ?? raw.cost_breakdown_url ?? usage.costBreakdownURL ?? usage.cost_breakdown_url ?? cost.costBreakdownURL ?? cost.cost_breakdown_url);
  }

  function firstRecord(...values: unknown[]): Record<string, unknown> {
    for (const value of values) {
      const record = asRecord(value);
      if (Object.keys(record).length > 0) return record;
    }
    return {};
  }

  function finiteNumberFrom(...values: unknown[]): number | undefined {
    for (const value of values) {
      if (value === undefined || value === null || value === '') continue;
      const number = Number(value);
      if (Number.isFinite(number)) return number;
    }
    return undefined;
  }

  function mergeUsage(current?: Record<string, unknown>, incoming?: Record<string, unknown>) {
    const next = { ...(current ?? {}), ...(incoming ?? {}) };
    return Object.keys(next).length > 0 ? next : undefined;
  }

  // Strip the "Continuing work from thread T-..." sentence so the user message bubble
  // doesn't repeat the same info that's already shown in the handoff card above.
  function stripHandoffPrefix(text: string): string {
    return text.replace(/^Continuing work from thread\s+T-[a-f0-9-]+\.?\s*/i, '').trim();
  }

  // Detect "Continuing work from thread T-..." marker in the first user message
  function handoffFromMessages(messages: NeoMessage[]): { threadId: string; instructions?: string } | undefined {
    const first = messages.find((m) => m.role === 'user');
    if (!first) return undefined;
    const text = userTextFromBlocks(first.content);
    const m = text.match(/Continuing work from thread\s+(T-[a-f0-9-]+)\.?\s*([\s\S]*)$/i);
    if (!m) return undefined;
    const threadId = m[1];
    const rest = m[2].trim();
    // Pull "Instructions:" sentence if present; otherwise take first ~120 chars
    const instr = rest.match(/Instructions?:?\s*["']?(.+?)["']?(?:\n|$)/i);
    const instructions = instr ? instr[1] : rest.split('\n')[0]?.slice(0, 140);
    return { threadId, instructions };
  }

  function threadDetailFromSummary(summary: ThreadSummary | undefined): ThreadDetail | null {
    if (!summary) return null;
    return {
      id: summary.id,
      title: summary.title,
      repo: summary.repo,
      branch: summary.branch,
      agentMode: summary.agentMode,
      messages: [],
      contextLabel: 'local context'
    };
  }

  function normalizeMessage(raw: unknown): NeoMessage | null {
    const item = asRecord(raw);
    const messageId = stringFrom(item.messageId ?? item.id);
    if (!messageId) return null;
    const usage = asRecord(item.usage);
    return {
      threadId: stringFrom(item.threadId),
      messageId,
      role: stringFrom(item.role) || 'assistant',
      content: Array.isArray(item.content) ? item.content.map((part) => asRecord(part) as ContentBlock) : [],
      state: asRecord(item.state),
      agentMode: stringFrom(item.agentMode),
      reasoningEffort: stringFrom(item.reasoningEffort),
      ...(Object.keys(usage).length > 0 ? { usage } : {})
    };
  }

  function textFromBlocks(blocks: ContentBlock[]) {
    return blocks
      .map((block) => {
        if (block.type === 'text') return block.text ?? '';
        if (block.type === 'thinking') return block.thinking ? `Thinking: ${block.thinking}` : '';
        if (block.type === 'tool_use') return `Using ${block.name ?? 'tool'}`;
        return '';
      })
      .filter(Boolean)
      .join('');
  }

  function userTextFromBlocks(blocks: ContentBlock[]) {
    return blocks
      .map((block) => block.type === 'text' ? block.text ?? '' : '')
      .filter(Boolean)
      .join('');
  }

  function escapeHtml(s: string): string {
    return s
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  // Tiny markdown renderer focused on what assistant text actually uses:
  // headers, fenced code, inline code (backticks), bullet lists, bold, italic, paragraphs.
  function renderMarkdown(raw: string): string {
    if (!raw) return '';
    const fences: string[] = [];
    // 1. Pull out fenced code blocks first so we don't touch their contents
    let src = raw.replace(/```([\w-]*)\n([\s\S]*?)```/g, (_m, lang, code) => {
      const safe = escapeHtml(code.replace(/\n$/, ''));
      fences.push(`<pre class="md-pre"><code class="md-code-block${lang ? ' lang-' + escapeHtml(lang) : ''}">${safe}</code></pre>`);
      return `NEO_FENCE_${fences.length - 1}_END`;
    });

    // 2. Escape the rest
    src = escapeHtml(src);

    // 3. Inline replacements: `code`, **bold**, *italic*, file:// links, bare URLs
    src = src
      .replace(/`([^`\n]+?)`/g, '<code class="md-code">$1</code>')
      .replace(/\*\*([^*\n]+?)\*\*/g, '<strong>$1</strong>')
      .replace(/(^|[\s(])\*([^*\n]+?)\*(?=[\s.,;:!?)]|$)/g, '$1<em>$2</em>')
      // file:// URIs -> clickable link
      .replace(/(file:\/\/[^\s<]+)/g, '<a class="md-file-link" href="$1">$1</a>')
      // bare http(s) URLs -> clickable link
      .replace(/(?<!["'>=\/])\b(https?:\/\/[^\s<)]+)/g, '<a class="md-link" href="$1" target="_blank" rel="noopener">$1</a>');

    // 4. Block-level: process line by line
    const lines = src.split('\n');
    const out: string[] = [];
    let inList = false;
    let paragraph: string[] = [];

    const flushParagraph = () => {
      if (paragraph.length) {
        out.push(`<p>${paragraph.join('<br>')}</p>`);
        paragraph = [];
      }
    };
    const closeList = () => {
      if (inList) {
        out.push('</ul>');
        inList = false;
      }
    };

    for (const line of lines) {
      const m = line.match(/^\s*[-*]\s+(.+)$/);
      const h = line.match(/^(#{1,3})\s+(.+)$/);
      if (m) {
        flushParagraph();
        if (!inList) { out.push('<ul class="md-list">'); inList = true; }
        out.push(`<li>${m[1]}</li>`);
      } else if (h) {
        flushParagraph();
        closeList();
        const level = h[1].length;
        out.push(`<h${level} class="md-h${level}">${h[2]}</h${level}>`);
      } else if (!line.trim()) {
        flushParagraph();
        closeList();
      } else {
        closeList();
        paragraph.push(line);
      }
    }
    flushParagraph();
    closeList();

    let html = out.join('');
    // 5. Restore fenced code blocks
    html = html.replace(/NEO_FENCE_(\d+)_END/g, (_m, i) => fences[Number(i)] || '');
    return html;
  }

  function formatTokenCount(n: number): string {
    if (n >= 1_000_000) return `${Math.round(n / 100_000) / 10}M`;
    if (n >= 1_000) return `${Math.round(n / 1_000)}k`;
    return String(n);
  }

  function formatCost(amount: number): string {
    if (amount === 0) return '$0';
    if (amount < 0.01) return `$${amount.toFixed(4)}`;
    return `$${amount.toFixed(2)}`;
  }

  function contextUsageLabel(usage: { used: number; total: number }) {
    const percent = Math.max(0, Math.min(Math.round((usage.used / usage.total) * 100), 100));
    return `${percent}% of ${formatTokenCount(usage.total)} context`;
  }

  function costLabel(cost: { amount: number; free?: number; paid?: number; included?: boolean }) {
    if ((cost.free ?? 0) > 0 && (cost.paid ?? 0) > 0) {
      return `${formatCost(cost.free ?? 0)} (free) + ${formatCost(cost.paid ?? 0)}`;
    }
    if ((cost.free ?? 0) > 0 && (cost.paid ?? 0) === 0) {
      return `${formatCost(cost.free ?? 0)} (free)`;
    }
    return `${formatCost(cost.amount)}${cost.included ? ' (free)' : ''}`;
  }

  function costURL(cost: ThreadDetail['cost'], thread: ThreadDetail | null) {
    return cost?.url || thread?.costBreakdownURL || (thread?.id ? `/threads/${encodeURIComponent(thread.id)}/usage` : '');
  }

  // Render a diff label like "+42 -7 ~7" with colored numbers
  function renderDiffStats(label: string): string {
    return escapeHtml(label).replace(/([+\-~])(\d+)/g, (_m, sign, num) => {
      const cls = sign === '+' ? 'diff-add' : sign === '-' ? 'diff-del' : 'diff-mod';
      return `<span class="${cls}">${sign}${num}</span>`;
    });
  }

  function previewFromMessage(raw: unknown) {
    const message = normalizeMessage(raw);
    if (!message) return '';
    return textFromBlocks(message.content).replace(/\s+/g, ' ').slice(0, 180);
  }

  function repoFromEnv(env: Record<string, unknown>) {
    const initial = asRecord(env.initial);
    const trees = Array.isArray(initial.trees) ? initial.trees : [];
    const first = asRecord(trees[0]);
    const repository = asRecord(first.repository);
    return (
      stringFrom(first.displayName) ||
      repoNameFromURL(stringFrom(repository.url)) ||
      stringFrom(first.repository) ||
      stringFrom(first.repo) ||
      stringFrom(first.name) ||
      repoNameFromURL(stringFrom(first.uri))
    );
  }

  function branchFromEnv(env: Record<string, unknown>) {
    const initial = asRecord(env.initial);
    const trees = Array.isArray(initial.trees) ? initial.trees : [];
    const first = asRecord(trees[0]);
    const repository = asRecord(first.repository);
    return stringFrom(first.branch) || stringFrom(first.gitBranch) || branchNameFromRef(stringFrom(repository.ref));
  }

  function repoNameFromURL(value: string) {
    if (!value) return '';
    const withoutGit = value.replace(/\.git$/, '');
    const parts = withoutGit.split(/[/:]/).filter(Boolean);
    return parts[parts.length - 1] ?? '';
  }

  function branchNameFromRef(value: string) {
    return value.startsWith('refs/heads/') ? value.slice('refs/heads/'.length) : value;
  }

  function diffLabelFrom(thread: Record<string, unknown>) {
    const stats = asRecord(thread.summaryStats);
    const diffStats = asRecord(stats.diffStats);
    const additions = Number(stats.additions ?? diffStats.added ?? 0);
    const deletions = Number(stats.deletions ?? diffStats.deleted ?? 0);
    if (!additions && !deletions) return undefined;
    return `+${additions} -${deletions}`;
  }

  function relativeLabel(value: number) {
    const millis = value > 10_000_000_000 ? value : value * 1000;
    const delta = Math.max(0, Date.now() - millis);
    const minutes = Math.floor(delta / 60_000);
    if (minutes < 1) return 'now';
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    return `${Math.floor(hours / 24)}d ago`;
  }

  function asRecord(value: unknown): Record<string, unknown> {
    return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {};
  }

  function stringFrom(value: unknown) {
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
    return typeof value === 'string' ? value : '';
  }

  function cycleTheme() {
    theme = theme === 'system' ? 'dark' : theme === 'dark' ? 'light' : 'system';
  }

  let copyOk = $state(false);
  let copyTimer: ReturnType<typeof setTimeout> | null = null;
  async function copyThreadCommand() {
    const text = threadCommand;
    let success = false;
    try {
      if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
        await navigator.clipboard.writeText(text);
        success = true;
      }
    } catch (e) {
      // fall through to legacy path
    }
    if (!success) {
      try {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.cssText = 'position:fixed;opacity:0;pointer-events:none;left:-9999px;top:-9999px';
        document.body.appendChild(ta);
        ta.focus();
        ta.select();
        success = document.execCommand('copy');
        ta.remove();
      } catch (e) {
        success = false;
      }
    }
    if (success) {
      copyOk = true;
      if (copyTimer) clearTimeout(copyTimer);
      copyTimer = setTimeout(() => { copyOk = false; copyTimer = null; }, 1500);
    }
  }

  function threadIdFromURL() {
    if (typeof location === 'undefined') return '';
    return new URL(location.href).searchParams.get('thread') ?? '';
  }

  function syncThreadURL(threadId: string, replace = false) {
    if (typeof location === 'undefined' || typeof history === 'undefined') return;
    const url = new URL(location.href);
    if (threadId) {
      url.searchParams.set('thread', threadId);
    } else {
      url.searchParams.delete('thread');
    }
    const next = `${url.pathname}${url.search}${url.hash}`;
    const current = `${location.pathname}${location.search}${location.hash}`;
    if (next === current) return;
    if (replace) {
      replaceState(next, {});
      return;
    }
    pushState(next, {});
  }

  function connectionLabel() {
    if (connection === 'connected') return agentState === 'idle' ? 'connected' : agentState;
    return connection;
  }

  function groupTranscriptItems(messages: NeoMessage[]): TranscriptItem[] {
    const items: TranscriptItem[] = [];
    let assistantMessages: NeoMessage[] = [];
    let assistantStart = '';

    const flushAssistant = () => {
      if (assistantMessages.length === 0) return;
      items.push({
        kind: 'assistant',
        messages: assistantMessages,
        key: `assistant-${assistantStart || items.length}`
      });
      assistantMessages = [];
      assistantStart = '';
    };

    for (const message of messages) {
      if (isHumanUserMessage(message)) {
        flushAssistant();
        items.push({ kind: 'user', message, key: `user-${message.messageId}` });
        continue;
      }

      if (!assistantStart) assistantStart = message.messageId;
      assistantMessages.push(message);
    }

    flushAssistant();
    return items;
  }

  function dedupeReplayMessages(messages: NeoMessage[]) {
    const next: NeoMessage[] = [];
    const bySignature = new Map<string, number>();

    for (const message of messages) {
      const signature = replayMessageSignature(message);
      const existingIndex = signature ? bySignature.get(signature) : undefined;
      if (existingIndex !== undefined) {
        const existing = next[existingIndex];
        if (isLikelyReplayDuplicate(existing, message)) {
          next[existingIndex] = preferReplayMessage(existing, message);
          continue;
        }
      }

      if (signature) bySignature.set(signature, next.length);
      next.push(message);
    }

    return next;
  }

  function replayMessageSignature(message: NeoMessage) {
    if (message.content.length === 0) return '';
    return JSON.stringify({
      role: message.role,
      content: message.content.map((block) => ({
        type: block.type,
        text: block.text ?? '',
        thinking: block.thinking ?? '',
        name: block.name ?? '',
        id: block.id ?? '',
        startTime: block.startTime ?? '',
        finalTime: block.finalTime ?? '',
        blockState: block.blockState ?? '',
        preview: blockContentPreview(block).slice(0, 240)
      }))
    });
  }

  function isLikelyReplayDuplicate(existing: NeoMessage, incoming: NeoMessage) {
    if (existing.messageId === incoming.messageId) return false;
    return isImportedMessageId(existing.messageId) !== isImportedMessageId(incoming.messageId);
  }

  function preferReplayMessage(existing: NeoMessage, incoming: NeoMessage) {
    return isImportedMessageId(existing.messageId) && !isImportedMessageId(incoming.messageId) ? incoming : existing;
  }

  function isImportedMessageId(messageId: string) {
    return /^\d+$/.test(messageId);
  }

  function isHumanUserMessage(message: NeoMessage) {
    return message.role === 'user' && userTextFromBlocks(message.content).trim().length > 0;
  }

  function assistantTurnSegments(messages: NeoMessage[]): MessageSegment[] {
    let workBlocks: ContentBlock[] = [];
    let workStart = '';
    const textSegments: MessageSegment[] = [];

    messages.forEach((message, messageIndex) => {
      message.content.forEach((block, blockIndex) => {
        const key = `${message.messageId}-${messageIndex}-${blockIndex}`;
        if (isRenderableWorkBlock(block) || isProgressTextBlock(message, block, blockIndex)) {
          if (workBlocks.length === 0) workStart = key;
          workBlocks.push(block);
          return;
        }

        if (block.type === 'text' && block.text) {
          textSegments.push({ kind: 'text', block, key });
        }
      });
    });

    return workBlocks.length > 0
      ? [{ kind: 'work', blocks: workBlocks, key: `${workStart}-work`, live: assistantTurnStreaming(messages) }, ...textSegments]
      : textSegments;
  }

  function assistantTurnStreaming(messages: NeoMessage[]) {
    return messages.some((message) => ['generating', 'streaming', 'tool_use', 'start'].includes(message.state?.type ?? ''));
  }

  function messageSegments(message: NeoMessage): MessageSegment[] {
    const segments: MessageSegment[] = [];
    let workBlocks: ContentBlock[] = [];
    let workStart = 0;

    const flushWork = () => {
      if (workBlocks.length === 0) return;
      segments.push({
        kind: 'work',
        blocks: workBlocks,
        key: `${message.messageId}-work-${workStart}`,
        live: assistantTurnStreaming([message])
      });
      workBlocks = [];
    };

    message.content.forEach((block, index) => {
      if (isRenderableWorkBlock(block)) {
        if (workBlocks.length === 0) workStart = index;
        workBlocks.push(block);
        return;
      }

      flushWork();
      if (block.type === 'text' && block.text) {
        segments.push({ kind: 'text', block, key: `${message.messageId}-text-${index}` });
      }
    });

    flushWork();
    return segments;
  }

  function isRenderableWorkBlock(block: ContentBlock) {
    if (block.type === 'thinking') return Boolean(block.thinking) || plausibleBlockTimeMillis(block.startTime) > 0;
    if (block.type === 'tool_use') return true;
    if (block.type === 'tool_result') return Boolean(blockContentPreview(block));
    return false;
  }

  function isProgressTextBlock(message: NeoMessage, block: ContentBlock, blockIndex: number) {
    if (block.type !== 'text' || !block.text) return false;
    if (plausibleBlockTimeMillis(block.startTime) <= 0 && plausibleBlockTimeMillis(block.finalTime) <= 0) return false;
    if (message.state?.stopReason === 'tool_use') return true;
    return message.content.slice(blockIndex + 1).some((next) => next.type === 'tool_use');
  }

  function workDurationLabel(blocks: ContentBlock[], live = false) {
    const starts = blocks.map((block) => plausibleBlockTimeMillis(block.startTime)).filter((value) => value > 0);
    if (starts.length === 0) return '';
    const ends = blocks
      .map((block) => plausibleBlockTimeMillis(block.finalTime))
      .filter((value) => value > 0);
    const start = Math.min(...starts);
    const streaming = live && blocks.some((block) => block.blockState === 'streaming');
    const end = ends.length > 0 ? Math.max(...ends) : streaming ? Date.now() : 0;
    if (end <= start) return '';
    return `Worked for ${formatWorkDuration(end - start)}`;
  }

  function blockTimeMillis(value: unknown) {
    const n = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
    if (!Number.isFinite(n) && typeof value === 'string') {
      const parsed = Date.parse(value);
      return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
    }
    if (!Number.isFinite(n) || n <= 0) return 0;
    return n > 10_000_000_000 ? n : n * 1000;
  }

  function plausibleBlockTimeMillis(value: unknown) {
    const ms = blockTimeMillis(value);
    const min = Date.UTC(2000, 0, 1);
    const max = Date.now() + 7 * 24 * 60 * 60 * 1000;
    return ms >= min && ms <= max ? ms : 0;
  }

  function formatWorkDuration(milliseconds: number) {
    const seconds = Math.max(1, Math.floor(milliseconds / 1000));
    if (seconds < 60) return 'less than a minute';
    const minutes = Math.max(1, Math.round(seconds / 60));
    if (minutes < 60) return `${minutes} ${minutes === 1 ? 'minute' : 'minutes'}`;
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = minutes % 60;
    const hourSuffix = hours === 1 ? 'hour' : 'hours';
    if (remainingMinutes === 0) return `${hours} ${hourSuffix}`;
    const minuteSuffix = remainingMinutes === 1 ? 'minute' : 'minutes';
    return `${hours} ${hourSuffix} ${remainingMinutes} ${minuteSuffix}`;
  }

  function blockStatus(block: ContentBlock) {
    return block.blockState === 'complete' ? 'done' : block.blockState || 'running';
  }

  function toolTitle(block: ContentBlock) {
    if (block.type === 'tool_result') return 'tool result';
    return prettyToolLabel(block.name || 'tool');
  }

  // Classify a tool_use block into an ampcode-style category for grouping.
  // Normalize by stripping spaces/underscores/dashes so 'Shell command', 'shell_command', 'shellCommand' all match.
  type ToolCategory = 'explore' | 'edit' | 'command' | 'thread' | 'skill' | 'task' | 'web' | 'other';
  function toolCategory(name: string): ToolCategory {
    const n = (name || '').toLowerCase().replace(/[\s_-]+/g, '');
    // Order matters — check most specific first
    if (n.includes('shell') || n.includes('command') || n.includes('bash') || n.includes('terminal') || n === 'exec' || n === 'run') return 'command';
    if (n.includes('editfile') || n.includes('writefile') || n.includes('createfile') || n.includes('patch') || n.includes('edit') || n === 'write' || n === 'modify' || n === 'update') return 'edit';
    if (n.includes('thread')) return 'thread';
    if (n.includes('subagent') || n === 'task' || n === 'agent' || n.includes('spawn')) return 'task';
    if (n.includes('web')) return 'web';
    if (n.includes('skill')) return 'explore';
    if (n.includes('search') || n.includes('grep') || n.includes('ripgrep')) return 'explore';
    if (n.includes('read') || n.includes('view') || n === 'cat') return 'explore';
    if (n.includes('list') || n.includes('glob') || n.includes('find') || n === 'tree' || n === 'ls' || n.includes('explore') || n.includes('directory')) return 'explore';
    return 'other';
  }
  function exploreNoun(name: string): string {
    const n = (name || '').toLowerCase().replace(/[\s_-]+/g, '');
    if (n.includes('search') || n.includes('grep') || n.includes('ripgrep')) return 'search';
    if (n.includes('list') || n.includes('glob') || n.includes('find') || n === 'tree' || n === 'ls' || n.includes('directory')) return 'list';
    if (n.includes('skill')) return 'skill';
    return 'file';
  }
  function pluralize(noun: string, n: number): string {
    if (n === 1) return `1 ${noun}`;
    if (noun === 'search') return `${n} searches`;
    return `${n} ${noun}s`;
  }

  type DisplayRow =
    | { kind: 'thinking'; block: ContentBlock }
    | { kind: 'progress'; block: ContentBlock }
    | { kind: 'explore'; tools: ContentBlock[] }
    | { kind: 'edit'; block: ContentBlock }
    | { kind: 'command'; block: ContentBlock }
    | { kind: 'tool'; block: ContentBlock }
    | { kind: 'result'; block: ContentBlock };

  function groupWorkBlocks(blocks: ContentBlock[]): DisplayRow[] {
    const rows: DisplayRow[] = [];
    let buf: ContentBlock[] = [];
    const flush = () => { if (buf.length) { rows.push({ kind: 'explore', tools: buf }); buf = []; } };
    for (const b of blocks) {
      if (b.type === 'thinking') { flush(); rows.push({ kind: 'thinking', block: b }); }
      else if (b.type === 'text') { flush(); rows.push({ kind: 'progress', block: b }); }
      else if (b.type === 'tool_use') {
        const cat = toolCategory(b.name || '');
        if (cat === 'explore') buf.push(b);
        else if (cat === 'edit') { flush(); rows.push({ kind: 'edit', block: b }); }
        else if (cat === 'command') { flush(); rows.push({ kind: 'command', block: b }); }
        else { flush(); rows.push({ kind: 'tool', block: b }); }
      } else if (b.type === 'tool_result') { flush(); rows.push({ kind: 'result', block: b }); }
      else { flush(); rows.push({ kind: 'tool', block: b }); }
    }
    flush();
    return rows;
  }

  function exploreSummary(tools: ContentBlock[]): string {
    const counts: Record<string, number> = {};
    for (const t of tools) {
      const noun = exploreNoun(t.name || '');
      counts[noun] = (counts[noun] || 0) + 1;
    }
    const order = ['file', 'search', 'list', 'skill'];
    const parts: string[] = [];
    for (const k of order) if (counts[k]) parts.push(pluralize(k, counts[k]));
    return parts.join(', ');
  }

  function editTarget(block: ContentBlock): string {
    const input = asRecord(block.input);
    const path = stringFrom(input.path ?? input.file ?? input.filename ?? input.file_path);
    if (path) {
      const parts = path.split('/');
      return parts[parts.length - 1] || path;
    }
    const patch = displayPatchFromBlock(block);
    const m = patch.match(/^\+\+\+\s+b?\/?(.+?)$/m) || patch.match(/^diff --git\s+a\/(\S+)/m);
    if (m && m[1]) {
      const parts = m[1].split('/');
      return parts[parts.length - 1] || m[1];
    }
    return block.name || 'file';
  }

  function commandText(block: ContentBlock): string {
    const input = asRecord(block.input);
    return stringFrom(input.command ?? input.cmd ?? input.script) || '';
  }

  // Map runtime tool names to ampcode-style labels.
  function prettyToolLabel(name: string): string {
    const cat = toolCategory(name);
    if (cat === 'command') return 'Ran';
    if (cat === 'edit') return 'Edited';
    if (cat === 'explore') {
      const noun = exploreNoun(name);
      if (noun === 'search') return 'Searched';
      if (noun === 'list') return 'Explored';
      return 'Read';
    }
    if (cat === 'web') return 'Searched the web';
    if (cat === 'thread') return 'Read thread';
    if (cat === 'skill') return 'Used skill';
    if (cat === 'task') return 'Delegated';
    // Fallback: capitalize first letter
    return (name || 'tool').charAt(0).toUpperCase() + (name || '').slice(1).replace(/_/g, ' ');
  }

  function toolSubtitle(block: ContentBlock) {
    const input = asRecord(block.input);
    const command = stringFrom(input.command);
    if (command) return command;
    const goal = stringFrom(input.goal);
    if (goal) return goal;
    const threadID = stringFrom(input.threadID ?? input.threadId);
    if (threadID) return threadID;
    const patch = rawPatchFromBlock(block);
    if (patch) {
      const stats = patchStats(displayPatchFromBlock(block));
      return `${stats.files} ${stats.files === 1 ? 'file' : 'files'} changed`;
    }
    return block.id ?? '';
  }

  function rawPatchFromBlock(block: ContentBlock) {
    const input = asRecord(block.input);
    return firstString(block.patch, block.diff, input.patchText, input.patch, input.diff);
  }

  function displayPatchFromBlock(block: ContentBlock) {
    const patch = rawPatchFromBlock(block);
    if (!patch.trim()) return '';
    if (patch.trimStart().startsWith('*** Begin Patch')) {
      return applyPatchToUnifiedPatch(patch);
    }
    return patch;
  }

  function hasPatch(block: ContentBlock) {
    return displayPatchFromBlock(block).trim().length > 0;
  }

  function patchStats(patch: string) {
    const files = new Set<string>();
    let additions = 0;
    let deletions = 0;
    for (const line of patch.split('\n')) {
      if (line.startsWith('diff --git ')) {
        files.add(line);
      } else if (line.startsWith('+++ ') || line.startsWith('--- ')) {
        files.add(line.slice(4));
      } else if (line.startsWith('+')) {
        additions += 1;
      } else if (line.startsWith('-')) {
        deletions += 1;
      }
    }
    return { files: Math.max(1, files.size), additions, deletions };
  }

  function toolInputPreview(block: ContentBlock) {
    const input = asRecord(block.input);
    if (Object.keys(input).length === 0) return '';
    return JSON.stringify(input, null, 2);
  }

  function blockContentPreview(block: ContentBlock) {
    if (typeof block.content === 'string') return block.content;
    if (block.content == null) return '';
    return JSON.stringify(block.content, null, 2);
  }

  function firstString(...values: unknown[]) {
    for (const value of values) {
      if (typeof value === 'string' && value.trim()) return value;
    }
    return '';
  }

  function applyPatchToUnifiedPatch(patch: string) {
    const lines = patch.replace(/\r\n/g, '\n').split('\n');
    const output: string[] = [];
    let index = 0;

    while (index < lines.length) {
      const line = lines[index] ?? '';
      if (line.startsWith('*** Add File: ')) {
        const file = line.slice('*** Add File: '.length).trim();
        const body: string[] = [];
        index += 1;
        while (index < lines.length && !lines[index].startsWith('*** ')) {
          if (lines[index].startsWith('+')) body.push(lines[index]);
          index += 1;
        }
        appendUnifiedFile(output, file, body, true);
        continue;
      }
      if (line.startsWith('*** Update File: ')) {
        const file = line.slice('*** Update File: '.length).trim();
        const hunks: string[][] = [];
        let current: string[] = [];
        index += 1;
        while (index < lines.length && !isPatchFileBoundary(lines[index])) {
          const next = lines[index] ?? '';
          if (next.startsWith('@@')) {
            if (current.length > 0) hunks.push(current);
            current = [];
          } else if (next !== '*** End of File' && isPatchBodyLine(next)) {
            current.push(next);
          }
          index += 1;
        }
        if (current.length > 0) hunks.push(current);
        appendUnifiedFile(output, file, hunks.flat(), false);
        continue;
      }
      index += 1;
    }

    return output.join('\n');
  }

  function appendUnifiedFile(output: string[], file: string, body: string[], isNewFile: boolean) {
    if (body.length === 0) return;
    const oldCount = body.filter((line) => !line.startsWith('+')).length;
    const newCount = body.filter((line) => !line.startsWith('-')).length;
    const safeFile = file || 'patch';
    output.push(`diff --git a/${safeFile} b/${safeFile}`);
    output.push(isNewFile ? '--- /dev/null' : `--- a/${safeFile}`);
    output.push(`+++ b/${safeFile}`);
    output.push(`@@ -${isNewFile ? 0 : 1},${oldCount} +1,${newCount} @@`);
    output.push(...body);
  }

  function isPatchFileBoundary(line: string) {
    return line.startsWith('*** Update File: ') || line.startsWith('*** Add File: ') || line.startsWith('*** Delete File: ') || line.startsWith('*** End Patch');
  }

  function isPatchBodyLine(line: string) {
    return line.startsWith('+') || line.startsWith('-') || line.startsWith(' ');
  }
</script>

<svelte:head>
  <title>Neo Remote</title>
</svelte:head>

{#snippet traceBlock(block: ContentBlock)}
  {#if block.type === 'thinking' && block.thinking}
    <details class="trace-block trace-block--thinking">
      <summary>
        <ChevronRight size={14} class="trace-block__chevron" />
        <Sparkles size={14} />
        <span>thinking</span>
        <small>{blockStatus(block)}</small>
      </summary>
      <p>{block.thinking}</p>
    </details>
  {:else if block.type === 'tool_use'}
    <details class="trace-block trace-block--tool">
      <summary>
        <ChevronRight size={14} class="trace-block__chevron" />
        {#if hasPatch(block)}
          <FileCode2 size={14} />
        {:else}
          <Wrench size={14} />
        {/if}
        <span>{toolTitle(block)}</span>
        <small>{blockStatus(block)}</small>
        {#if hasPatch(block)}
          {@const stats = patchStats(displayPatchFromBlock(block))}
          <b>+{stats.additions} -{stats.deletions}</b>
        {/if}
      </summary>
      {#if toolSubtitle(block)}
        <div class="trace-block__subline">{toolSubtitle(block)}</div>
      {/if}
      {#if hasPatch(block)}
        <PatchDiff patch={displayPatchFromBlock(block)} mode={theme} />
      {:else if toolInputPreview(block)}
        <pre class="code-panel">{toolInputPreview(block)}</pre>
      {/if}
    </details>
  {:else if block.type === 'tool_result'}
    <details class="trace-block trace-block--result">
      <summary>
        <ChevronRight size={14} class="trace-block__chevron" />
        <CheckCircle2 size={14} />
        <span>tool result</span>
        <small>received</small>
      </summary>
      <pre class="code-panel">{blockContentPreview(block)}</pre>
    </details>
  {/if}
{/snippet}

{#snippet workGroup(blocks: ContentBlock[], live = false)}
  {@const duration = workDurationLabel(blocks, live)}
  <details class="work-group">
    <summary>
      <span class="work-group__line"></span>
      <span class="work-group__button">
        {#if duration}
          <span>{duration}</span>
        {:else}
          <span>Show Work</span>
        {/if}
        <ChevronRight size={14} class="work-group__chevron" />
      </span>
      <span class="work-group__line"></span>
    </summary>
    <div class="work-group__body">
      {#each groupWorkBlocks(blocks) as row, index (index)}
        {#if row.kind === 'thinking'}
          {#if row.block.thinking}
            <div class="trace-thinking md">{@html renderMarkdown(row.block.thinking)}</div>
          {/if}
        {:else if row.kind === 'progress'}
          {#if row.block.text}
            <div class="trace-thinking trace-thinking--progress md">{@html renderMarkdown(row.block.text)}</div>
          {/if}
        {:else if row.kind === 'explore'}
          <details class="trace-row trace-row--explore">
            <summary>
              <span class="trace-row__label">Explored</span>
              <span class="trace-row__sub">{exploreSummary(row.tools)}</span>
              <ChevronRight size={12} class="trace-row__chevron" />
            </summary>
            <ul class="trace-row__list">
              {#each row.tools as tool, i (tool.id ?? i)}
                <li>
                  <span class="trace-row__list-label">{prettyToolLabel(tool.name || '')}</span>
                  <span class="trace-row__list-target">{toolSubtitle(tool)}</span>
                </li>
              {/each}
            </ul>
          </details>
        {:else if row.kind === 'edit'}
          {@const stats = patchStats(displayPatchFromBlock(row.block))}
          <details class="trace-row trace-row--edit">
            <summary>
              <span class="trace-row__label">Edited</span>
              <span class="trace-row__file">{editTarget(row.block)}</span>
              {#if stats.additions || stats.deletions}
                <b class="trace-row__diff">
                  {#if stats.additions}<span class="diff-add">+{stats.additions}</span>{/if}
                  {#if stats.deletions}<span class="diff-del">-{stats.deletions}</span>{/if}
                </b>
              {/if}
            </summary>
            {#if hasPatch(row.block)}
              <PatchDiff patch={displayPatchFromBlock(row.block)} mode={theme} />
            {/if}
          </details>
        {:else if row.kind === 'command'}
          <details class="trace-row trace-row--cmd">
            <summary>
              <code class="trace-row__cmd">$ {commandText(row.block) || prettyToolLabel(row.block.name || 'command')}</code>
            </summary>
            {#if toolInputPreview(row.block)}
              <pre class="code-panel">{toolInputPreview(row.block)}</pre>
            {/if}
          </details>
        {:else}
          {@render traceBlock(row.block)}
        {/if}
      {/each}
    </div>
  </details>
{/snippet}

{#snippet threadInspector()}
  <div class="inspector-card">
    <div class="inspector-fields">
      <div class="inspector-field"><Sparkles size={13} /> <span>{detail?.agentMode ?? selectedSummary?.agentMode ?? 'smart'}</span></div>
      {#if inspectorContextUsage}
        <div class="inspector-field inspector-field--usage">
          <Gauge size={13} />
          <span>
            {contextUsageLabel(inspectorContextUsage)}
            {#if inspectorCost}
              <a class="usage-cost-link" href={costURL(inspectorCost, detail)}>[{costLabel(inspectorCost)}]</a>
            {/if}
          </span>
        </div>
      {:else if detail?.repo}
        <div class="inspector-field"><Gauge size={13} /> <span>{detail.repo}{detail.branch ? ':' + detail.branch : ''}</span></div>
      {/if}
      <div class:status-ok={executorConnected} class="inspector-field"><Wifi size={13} /> <span>{executorConnected ? 'connected' : 'not connected'}</span></div>
      {#if threadStatus}
        <div class="inspector-field"><span>{threadStatus}</span></div>
      {/if}
      {#if queuedCount > 0}
        <div class="inspector-field"><ArrowUp size={13} /> <span>{queuedCount} pending</span></div>
      {/if}
      {#if compactionActive || compactionRecords.length > 0}
        <div class="inspector-field"><span>{compactionActive ? 'compaction running' : `compaction: ${compactionRecords.length} records`}</span></div>
      {/if}
      {#if selectedSummary?.diffLabel}
        <div class="inspector-field diff-stats">{@html renderDiffStats(selectedSummary.diffLabel)}</div>
      {/if}
      {#if maxTokensLabel}
        <div class="inspector-field"><span>max {maxTokensLabel}</span></div>
      {/if}
      {#if mainThreadId}
        <div class="inspector-field inspector-field--mono"><span>{mainThreadId}</span></div>
      {/if}
    </div>

    <div class="inspector-section">
      <div class="inspector-section__head">Open in CLI</div>
      <div class="cli-row">
        <div class="cli-command">{threadCommand}</div>
        <button
          class:cli-row__copy--ok={copyOk}
          class="icon-button cli-row__copy"
          type="button"
          title={copyOk ? 'Copied!' : 'Copy command'}
          aria-label="Copy CLI command"
          onclick={copyThreadCommand}
        >
          {#if copyOk}
            <CheckCircle2 size={13} />
          {:else}
            <Copy size={13} />
          {/if}
        </button>
      </div>
    </div>
  </div>

  {#if activeError}
    <section class="runtime-section runtime-section--error">
      <h2>Active error</h2>
      <p>{stringFrom(activeError.message) || stringFrom(activeError.code) || 'Runtime error'}</p>
    </section>
  {/if}

  {#if toolApprovals.length > 0}
    <section class="runtime-section">
      <h2>Approvals</h2>
      <div class="runtime-list">
        {#each toolApprovals as approval (approval.toolCallId)}
          <article class="approval-card">
            <strong>{approval.name}</strong>
            {#if approval.message}
              <p>{approval.message}</p>
            {/if}
            {#if Object.keys(approval.input).length > 0}
              <pre>{JSON.stringify(approval.input, null, 2)}</pre>
            {/if}
            <div class="approval-card__actions">
              <button type="button" onclick={() => approveTool(approval)}>Approve</button>
              <button type="button" onclick={() => denyTool(approval)}>Deny</button>
            </div>
          </article>
        {/each}
      </div>
    </section>
  {/if}

  {#if queuedMessages.length > 0}
    <section class="runtime-section">
      <h2>Queued messages</h2>
      <div class="runtime-list">
        {#each queuedMessages as queued (queued.id)}
          <p class="runtime-pill">
            <span>{queued.steer ? 'steer' : 'queued'}</span>
            {queued.preview || queued.messageId}
          </p>
        {/each}
      </div>
    </section>
  {/if}

  {#if inferenceTools || toolLeases.length > 0 || executorStatuses.length > 0 || retryNotice}
    <section class="runtime-section">
      <h2>Runtime activity</h2>
      <div class="runtime-list">
        {#if inferenceTools}
          <p class="runtime-pill">
            <span>{inferenceTools.agentMode || 'inference'}</span>
            {inferenceTools.tools.join(', ') || inferenceTools.messageId}
          </p>
        {/if}
        {#each toolLeases as lease (lease.toolCallId)}
          <p class="runtime-pill">
            <span>lease</span>
            {lease.toolName}
          </p>
        {/each}
        {#if retryNotice}
          <p class="runtime-pill"><span>retry</span>{retryNotice}</p>
        {/if}
        {#each executorStatuses as status (status.id)}
          <p class="runtime-pill">
            <span>{status.status}</span>
            {status.message || objectSummary(status.details) || status.id}
          </p>
        {/each}
      </div>
    </section>
  {/if}

  {#if artifacts.length > 0}
    <section class="runtime-section">
      <h2>Artifacts</h2>
      <div class="runtime-list">
        {#each artifacts as artifact (artifact.key)}
          <p class="runtime-pill">
            <span>{artifact.dataType}</span>
            {artifact.key}
          </p>
        {/each}
      </div>
    </section>
  {/if}

  {#if relationships.length > 0}
    <section class="runtime-section">
      <h2>Relationships</h2>
      <div class="runtime-list">
        {#each relationships as relationship (`${relationship.threadID}-${relationship.type}-${relationship.role}`)}
          <button
            class="runtime-pill runtime-pill--link"
            type="button"
            onclick={() => { void selectThread(relationship.threadID); }}
            title="Open {relationship.threadID}"
          >
            <span>{relationship.type}</span>
            {relationship.threadID}{relationship.comment ? `: ${relationship.comment}` : ''}
          </button>
        {/each}
      </div>
    </section>
  {/if}

  {#if draftPreview || pendingNavigation}
    <section class="runtime-section">
      <h2>Draft</h2>
      {#if draftPreview}
        <p>{draftPreview}</p>
      {/if}
      {#if pendingNavigation}
        <p class="runtime-pill"><span>navigate</span>{pendingNavigation}</p>
      {/if}
    </section>
  {/if}

  {#if lastError}
    <div class="error-card">{lastError}</div>
  {/if}
{/snippet}

{#if !isAuthenticated}
  <main class="auth-shell">
    <section class="auth-panel" aria-label="Sign in">
      <div class="auth-panel__brand">
        <svg class="auth-panel__logo" viewBox="0 0 281 144" fill="currentColor" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
          <path d="M236.014 20C260.431 20.0001 280.602 37.4115 280.603 64.7432C280.602 93.5337 260.065 114.166 233.52 114.166C224.158 114.166 215.639 112.422 208.63 108.49C202.886 105.27 198.203 100.605 194.919 94.3379L188.115 141.822L187.946 143.016H174.214L174.448 141.423L191.772 22.4941H205.372L203.937 31.3369C212.143 23.8608 223.2 20.0002 236.014 20ZM47.082 20.1543C56.4435 20.1543 65.0012 21.8991 72.0488 25.8486C77.8222 29.0831 82.5323 33.7713 85.8271 40.085L88.1201 23.6924L88.2861 22.4932H101.863L89.1611 110.633L88.9873 111.826H75.4092L76.7227 102.855C68.5854 110.456 57.3981 114.323 44.5889 114.323C20.1709 114.323 0.000167223 96.9087 0 69.5771C0.000149745 40.7854 20.54 20.1549 47.082 20.1543ZM116.234 110.636L116.061 111.827H102.485L115.351 23.6855L115.521 22.4941H129.083L116.234 110.636ZM140.673 110.636L140.499 111.827H126.924L139.789 23.6855L139.96 22.4941H153.521L140.673 110.636ZM177.958 22.4941L165.108 110.636L164.935 111.827H151.36L164.225 23.6855L164.396 22.4941H177.958ZM48.4854 31.9844C27.8638 31.985 14.0133 48.3799 14.0127 68.9521C14.0127 77.7907 16.8094 86.1771 22.3145 92.334C27.7973 98.4657 36.0631 102.493 47.2402 102.493C67.8534 102.493 81.7122 85.9487 81.7129 65.3682C81.7129 55.4076 78.2493 47.0792 72.4131 41.2441C66.5794 35.4088 58.2871 31.9844 48.4854 31.9844ZM233.362 31.8291C212.749 31.8297 198.89 48.3716 198.89 68.9521C198.89 78.9123 202.356 87.2403 208.189 93.0742C214.023 98.9107 222.315 102.336 232.116 102.336C252.738 102.335 266.589 85.9407 266.59 65.3682C266.59 56.5296 263.795 48.1424 258.29 41.9863C252.807 35.8551 244.542 31.8291 233.362 31.8291Z"/>
        </svg>
        <span class="auth-panel__sub">neo remote</span>
      </div>

      <div class="auth-panel__intro">
        <h1>Connect</h1>
      </div>

      <form class="auth-form" onsubmit={(event) => { event.preventDefault(); void submitKey(); }}>
        <label class="auth-form__label" for="auth-key">Access key</label>
        <div class="auth-form__field">
          <KeyRound size={14} />
          <input
            id="auth-key"
            bind:value={keyDraft}
            type="password"
            autocomplete="off"
            placeholder="Enter your key"
          />
        </div>
        <button class="auth-form__submit" type="submit" disabled={authLoading || !keyDraft.trim()}>
          {#if authLoading}
            <Loader2 size={14} class="spin" />
            Connecting...
          {:else}
            Connect
          {/if}
        </button>
      </form>

      {#if lastError}
        <p class="auth-panel__error">{lastError}</p>
      {/if}
    </section>
  </main>
{:else}
<main class="neo-shell">
  <header class="neo-topbar">
    <div class="neo-brand" aria-label="Neo Remote">
      <svg class="neo-brand__logo" viewBox="0 0 281 144" fill="currentColor" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
        <path d="M236.014 20C260.431 20.0001 280.602 37.4115 280.603 64.7432C280.602 93.5337 260.065 114.166 233.52 114.166C224.158 114.166 215.639 112.422 208.63 108.49C202.886 105.27 198.203 100.605 194.919 94.3379L188.115 141.822L187.946 143.016H174.214L174.448 141.423L191.772 22.4941H205.372L203.937 31.3369C212.143 23.8608 223.2 20.0002 236.014 20ZM47.082 20.1543C56.4435 20.1543 65.0012 21.8991 72.0488 25.8486C77.8222 29.0831 82.5323 33.7713 85.8271 40.085L88.1201 23.6924L88.2861 22.4932H101.863L89.1611 110.633L88.9873 111.826H75.4092L76.7227 102.855C68.5854 110.456 57.3981 114.323 44.5889 114.323C20.1709 114.323 0.000167223 96.9087 0 69.5771C0.000149745 40.7854 20.54 20.1549 47.082 20.1543ZM116.234 110.636L116.061 111.827H102.485L115.351 23.6855L115.521 22.4941H129.083L116.234 110.636ZM140.673 110.636L140.499 111.827H126.924L139.789 23.6855L139.96 22.4941H153.521L140.673 110.636ZM177.958 22.4941L165.108 110.636L164.935 111.827H151.36L164.225 23.6855L164.396 22.4941H177.958ZM48.4854 31.9844C27.8638 31.985 14.0133 48.3799 14.0127 68.9521C14.0127 77.7907 16.8094 86.1771 22.3145 92.334C27.7973 98.4657 36.0631 102.493 47.2402 102.493C67.8534 102.493 81.7122 85.9487 81.7129 65.3682C81.7129 55.4076 78.2493 47.0792 72.4131 41.2441C66.5794 35.4088 58.2871 31.9844 48.4854 31.9844ZM233.362 31.8291C212.749 31.8297 198.89 48.3716 198.89 68.9521C198.89 78.9123 202.356 87.2403 208.189 93.0742C214.023 98.9107 222.315 102.336 232.116 102.336C252.738 102.335 266.589 85.9407 266.59 65.3682C266.59 56.5296 263.795 48.1424 258.29 41.9863C252.807 35.8551 244.542 31.8291 233.362 31.8291Z"/>
      </svg>
      <span class="neo-brand__sub">neo remote</span>
    </div>
    <button
      class:threads-tab--active={!detail || mobilePane === 'threads'}
      class="threads-tab"
      type="button"
      onclick={() => { mobilePane = 'threads'; }}
    >
      <List size={14} />
      Threads
    </button>

    <div class="neo-topbar__actions">
      <span class="neo-key" title="API key active"><KeyRound size={13} /> key</span>
      {#if detail && mobilePane === 'thread'}
        <button
          class="icon-button icon-button--mobile-only"
          type="button"
          title="Thread info"
          aria-label="Open thread info"
          onclick={() => { mobileInspectorOpen = true; }}
        >
          <PanelRight size={15} />
        </button>
      {/if}
      <button class="icon-button" type="button" title="Refresh threads" onclick={() => { void refreshThreads(); }}>
        {#if loadingThreads}
          <Loader2 size={15} class="spin" />
        {:else}
          <Wifi size={15} />
        {/if}
      </button>
      <button class="icon-button" type="button" title="Cycle theme" onclick={cycleTheme}>
        {#if theme === 'light'}
          <Sun size={15} />
        {:else if theme === 'dark'}
          <Moon size={15} />
        {:else}
          <Sparkles size={15} />
        {/if}
      </button>
      <button class="icon-button" type="button" title="Change key" onclick={forgetKey}>
        <LogOut size={15} />
      </button>
    </div>
  </header>

  {#if !detail || mobilePane === 'threads'}
    <section class="neo-container">
      <div class="thread-search">
        <div class="search-box">
          <Search size={15} />
          <input bind:value={query} placeholder="Search threads..." />
        </div>
      </div>

      <div class="thread-list" aria-label="Threads">
        {#each filteredThreads as thread (thread.id)}
          <div class="thread-row">
            <button
              class:thread-card--active={thread.id === selectedThreadId}
              class="thread-card"
              type="button"
              onclick={() => { void selectThread(thread.id); }}
            >
              <span class="thread-card__main">
                <span class="thread-card__title-row">
                  <strong>{thread.title}</strong>
                  {#if threadRuntimeLabel(thread.id)}
                    <span
                      class:thread-card__runtime--live={connection === 'connected'}
                      class="thread-card__runtime"
                    >
                      <span class="runtime-dot"></span>
                      {threadRuntimeLabel(thread.id)}
                    </span>
                  {/if}
                </span>
                <span class="thread-card__meta">
                  <span>{thread.updatedLabel}</span>
                  <span>—</span>
                  {#if thread.diffLabel}
                    <span class="thread-card__diff">{@html renderDiffStats(thread.diffLabel)}</span>
                    <span>,</span>
                  {/if}
                  <span>{thread.messageCount} {thread.messageCount === 1 ? 'message' : 'messages'}</span>
                  <span class="thread-card__repo">{thread.repo}:{thread.branch}</span>
                </span>
                {#if thread.preview}
                  <span class="thread-card__preview">{thread.preview}</span>
                {/if}
              </span>
            </button>
          </div>
        {/each}
      </div>
    </section>
  {:else}
  <section class:neo-grid--thread-open={Boolean(detail) && mobilePane === 'thread'} class="neo-grid">
    <div class="neo-grid__spacer" aria-hidden="true"></div>
    <section class="thread-stage">
      {#if detail}
        <div class="thread-head">
          <button class="thread-back" type="button" onclick={() => { mobilePane = 'threads'; }}>
            <ArrowLeft size={15} />
            Threads
          </button>
          <h1>{detail.title}</h1>
          <div class="thread-head__meta">
            <span class="thread-head__time">{selectedSummary?.updatedLabel ?? ''}</span>
          </div>
          <div class="thread-head__repo">
            <span><Laptop size={14} /> {detail.repo}</span>
            <span><GitBranch size={14} /> {detail.branch}</span>
          </div>
          <div
            class:connection-chip--live={connection === 'connected'}
            class="connection-chip"
            title={connectionLabel()}
            aria-label={connectionLabel()}
          >
            {#if connection === 'connected'}
              <Wifi size={12} />
            {:else}
              <WifiOff size={12} />
            {/if}
          </div>
        </div>

        {#if userTurnAnchors.length > 1}
          <nav
            class:msg-nav--open={msgNavHover}
            class="msg-nav"
            aria-label="Jump to message"
            onmouseenter={msgNavEnter}
            onmouseleave={msgNavLeave}
          >
            <div class="msg-nav__inner">
              {#each userTurnAnchors as anchor, i (anchor.messageId)}
                <button
                  class:msg-nav__tick--primary={i === 0}
                  class="msg-nav__tick"
                  type="button"
                  title="Jump to message {i + 1}"
                  aria-label="Jump to message {i + 1}"
                  onclick={() => scrollToMsg(anchor.messageId)}
                >
                  <span class="msg-nav__dot"></span>
                  <span class="msg-nav__line"></span>
                </button>
              {/each}
            </div>
            <div
              class="msg-nav__popover"
              role="listbox"
              aria-label="All turns"
              tabindex="-1"
              onmouseenter={msgNavEnter}
              onmouseleave={msgNavLeave}
            >
              {#each userTurnAnchors as anchor, i (anchor.messageId)}
                <button
                  class="msg-nav__popover-item"
                  type="button"
                  onclick={() => scrollToMsg(anchor.messageId)}
                >
                  <span class="msg-nav__popover-index">{i + 1}.</span>
                  <span class="msg-nav__popover-text">{anchor.preview}</span>
                </button>
              {/each}
            </div>
          </nav>
        {/if}

        <div class="transcript" aria-live="polite">
          {#if loadingThread}
            <div class="loading-row"><Loader2 size={18} class="spin" /> Loading thread</div>
          {/if}
          {#if handoffFrom}
            <button
              class="handoff-card"
              type="button"
              onclick={() => { void selectThread(handoffFrom.threadId); }}
              title="Open source thread"
            >
              <ArrowLeft size={14} />
              <div class="handoff-card__body">
                <div class="handoff-card__head">Handed off from <span class="handoff-card__id">{handoffFrom.threadId}</span></div>
                {#if handoffFrom.instructions}
                  <div class="handoff-card__instr">Instructions: "{handoffFrom.instructions}"</div>
                {/if}
              </div>
            </button>
          {/if}
          {#each transcriptItems as item (item.key)}
            {#if item.kind === 'user'}
              <article class="message message--user" data-message-id={item.message.messageId}>
                <div class="message__bubble">{userTextFromBlocks(item.message.content)}</div>
              </article>
            {:else if assistantTurnSegments(item.messages).length > 0}
              <article class="message" data-message-id={item.key}>
                <div class="message__agent">
                  <div>
                    {#each assistantTurnSegments(item.messages) as segment (segment.key)}
                      {#if segment.kind === 'work'}
                        {@render workGroup(segment.blocks, segment.live)}
                      {:else}
                        <div class:streaming-text={assistantTurnStreaming(item.messages)} class="md">{@html renderMarkdown(segment.block.text ?? '')}</div>
                      {/if}
                    {/each}
                  </div>
                </div>
              </article>
            {/if}
          {/each}
        </div>

        <div class="composer-dock" aria-hidden="true"></div>
        <footer class="composer-footer">
          <form class="composer" onsubmit={(event) => { event.preventDefault(); sendMessage(); }}>
          <div
            class:composer-status--live={connection === 'connected'}
            class:composer-status--connecting={connection === 'connecting'}
            class="composer-status"
          >
            <span class="runtime-dot"></span>
            <span class="composer-status__main">{composerStatusMain()}</span>
            {#each composerStatusParts() as part}
              <span class="composer-status__sep">·</span>
              <span class="composer-status__part">{part}</span>
            {/each}
          </div>
          <div class="composer-body">
            <textarea
              bind:value={composer}
              rows="2"
              placeholder={connection === 'connected' ? 'Send a message to this thread...' : 'Connect to send a message...'}
              onkeydown={(event) => {
                if (event.key === 'Enter' && !event.shiftKey) {
                  event.preventDefault();
                  sendMessage();
                }
              }}
            ></textarea>
            <div class="composer-actions">
              <div class="composer-actions__left"></div>
              <button class="send-button" type="submit" disabled={!composer.trim() || connection !== 'connected'} aria-label="Send message">
                <ArrowUp size={14} />
              </button>
            </div>
          </div>
          </form>
        </footer>
      {:else}
        <div class="empty-state">
          <PanelRight size={28} />
          <h1>Select a thread</h1>
          <p>Load a local Neo thread to start streaming runtime events into the browser.</p>
        </div>
      {/if}
    </section>

    <aside class="inspector">
      {@render threadInspector()}
    </aside>
  </section>
  {/if}

  {#if detail && mobileInspectorOpen}
    <div class="sheet" role="dialog" aria-modal="true" aria-label="Thread info">
      <button class="sheet__backdrop" type="button" aria-label="Close" onclick={() => { mobileInspectorOpen = false; }}></button>
      <div class="sheet__panel">
        <div class="sheet__grabber"></div>
        <div class="sheet__head">
          <h2>Thread info</h2>
          <button class="icon-button" type="button" aria-label="Close" onclick={() => { mobileInspectorOpen = false; }}>
            <ArrowLeft size={15} />
          </button>
        </div>
        <div class="sheet__body">
          {@render threadInspector()}
        </div>
      </div>
    </div>
  {/if}
</main>
{/if}


<style>
  :global(*) { box-sizing: border-box; }

  :global(html) {
    color-scheme: dark light;
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    background: var(--neo-bg);
    color: var(--neo-ink);
    font-size: 13px;
    line-height: 1.54;
  }

  :global(body) {
    margin: 0;
    min-width: 0;
    background: var(--neo-bg);
  }

  :global(html[data-theme='dark']),
  :global(html[data-theme='system']) {
    --neo-bg: #0b0d0b;
    --neo-ink: #f6fff5;
    --neo-muted: #9ca49c;
    --neo-soft: rgba(156, 164, 156, 0.7);
    --neo-panel: #0b0d0b;
    --neo-panel-lift: rgba(246, 255, 245, 0.04);
    --neo-card: rgba(246, 255, 245, 0.03);
    --neo-card-hover: rgba(246, 255, 245, 0.07);
    --neo-field: #0b0d0b;
    --neo-bubble: rgba(246, 255, 245, 0.07);
    --neo-border: rgba(135, 139, 134, 0.12);
    --neo-border-strong: rgba(246, 255, 245, 0.15);
    --neo-accent: #f6fff5;
    --neo-accent-soft: rgba(246, 255, 245, 0.035);
    --neo-pill-bg: rgba(135, 139, 134, 0.1);
    --neo-success: #6ecb73;
    --neo-success-soft: rgba(110, 203, 115, 0.12);
    --neo-danger: #bd2b2b;
    --neo-shadow: 0 6px 24px rgba(0, 0, 0, 0.25);
  }

  @media (prefers-color-scheme: light) {
    :global(html[data-theme='system']) {
      --neo-bg: #fafaf8;
      --neo-ink: #0b0d0b;
      --neo-muted: #5a5f59;
      --neo-soft: #878b86;
      --neo-panel: #fafaf8;
      --neo-panel-lift: rgba(11, 13, 11, 0.04);
      --neo-card: rgba(11, 13, 11, 0.03);
      --neo-card-hover: rgba(11, 13, 11, 0.06);
      --neo-field: #fafaf8;
      --neo-bubble: rgba(11, 13, 11, 0.06);
      --neo-border: rgba(11, 13, 11, 0.12);
      --neo-border-strong: rgba(11, 13, 11, 0.18);
      --neo-accent: #0b0d0b;
      --neo-accent-soft: rgba(11, 13, 11, 0.05);
      --neo-pill-bg: rgba(11, 13, 11, 0.06);
      --neo-success: #2c8a3a;
      --neo-success-soft: rgba(44, 138, 58, 0.10);
      --neo-danger: #bd2b2b;
      --neo-shadow: 0 6px 24px rgba(0, 0, 0, 0.06);
    }
  }

  :global(html[data-theme='light']) {
    --neo-bg: #fafaf8;
    --neo-ink: #0b0d0b;
    --neo-muted: #5a5f59;
    --neo-soft: #878b86;
    --neo-panel: #fafaf8;
    --neo-panel-lift: rgba(11, 13, 11, 0.04);
    --neo-card: rgba(11, 13, 11, 0.03);
    --neo-card-hover: rgba(11, 13, 11, 0.06);
    --neo-field: #fafaf8;
    --neo-bubble: rgba(11, 13, 11, 0.06);
    --neo-border: rgba(11, 13, 11, 0.12);
    --neo-border-strong: rgba(11, 13, 11, 0.18);
    --neo-accent: #0b0d0b;
    --neo-accent-soft: rgba(11, 13, 11, 0.05);
    --neo-pill-bg: rgba(11, 13, 11, 0.06);
    --neo-success: #2c8a3a;
    --neo-success-soft: rgba(44, 138, 58, 0.10);
    --neo-danger: #bd2b2b;
    --neo-shadow: 0 6px 24px rgba(0, 0, 0, 0.06);
  }

  /* === AUTH === */
  .auth-shell {
    display: grid;
    min-height: 100dvh;
    place-items: center;
    padding: 24px;
    background: var(--neo-bg);
  }

  .auth-panel {
    display: grid;
    gap: 20px;
    width: min(100%, 400px);
    border: 1px solid var(--neo-border-strong);
    background: color-mix(in srgb, var(--neo-ink) 3%, var(--neo-bg));
    outline: 1px solid color-mix(in srgb, var(--neo-ink) 10%, transparent);
    outline-offset: -1px;
    border-radius: 16px;
    padding: 24px;
    box-shadow: 0 10px 32px rgba(0, 0, 0, 0.08);
  }

  .auth-panel__brand {
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 0 0 4px;
  }
  .auth-panel__logo {
    height: 24px;
    width: auto;
    color: var(--neo-ink);
    flex-shrink: 0;
  }
  .auth-panel__sub {
    color: var(--neo-muted);
    font-size: 13px;
    font-weight: 500;
    white-space: nowrap;
    border-left: 1px solid var(--neo-border-strong);
    padding-left: 10px;
    line-height: 1;
  }

  .auth-panel__intro { display: flex; flex-direction: column; gap: 6px; }
  .auth-panel__intro h1 {
    margin: 0;
    font-size: 18px;
    font-weight: 600;
    color: var(--neo-ink);
    letter-spacing: -0.01em;
  }

  .auth-form { display: flex; flex-direction: column; gap: 8px; }
  .auth-form__label {
    color: var(--neo-muted);
    font-size: 12px;
    line-height: 16px;
    font-weight: 500;
  }
  .auth-form__field {
    display: flex;
    align-items: center;
    gap: 8px;
    min-height: 36px;
    border: 1px solid var(--neo-border-strong);
    background: var(--neo-bg);
    border-radius: 6px;
    padding: 0 12px;
    color: var(--neo-muted);
    transition: border-color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .auth-form__field:focus-within { border-color: var(--neo-ink); }
  .auth-form input {
    width: 100%;
    min-width: 0;
    border: 0;
    outline: 0;
    background: transparent;
    color: var(--neo-ink);
    font: inherit;
    font-size: 13px;
    line-height: 20px;
    padding: 0;
  }
  .auth-form input::placeholder { color: var(--neo-muted); }
  .auth-form__submit {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    min-height: 36px;
    margin-top: 6px;
    border: 0;
    border-radius: 8px;
    background: var(--neo-accent);
    color: var(--neo-bg);
    font-weight: 500;
    font-size: 13px;
    cursor: pointer;
    transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .auth-form__submit:hover:not(:disabled) { opacity: 0.9; }
  .auth-form__submit:disabled { cursor: not-allowed; opacity: 0.4; }
  .auth-panel__error {
    margin: 0;
    color: var(--neo-danger);
    font-size: 12px;
    line-height: 16px;
  }

  /* === SHELL & HEADER === */
  .neo-shell {
    display: flex;
    flex-direction: column;
    min-height: 100vh;
    background: var(--neo-bg);
  }

  .neo-topbar {
    display: flex;
    align-items: center;
    gap: 4px;
    flex-wrap: wrap;
    padding: 8px 12px;
    border-bottom: 1px solid var(--neo-border);
    background: var(--neo-bg);
    position: sticky;
    top: 0;
    z-index: 50;
  }

  .neo-brand { display: flex; align-items: center; gap: 10px; min-width: 0; padding: 0 8px 0 4px; }
  .neo-brand__logo {
    height: 20px;
    width: auto;
    color: var(--neo-ink);
    flex-shrink: 0;
  }
  .neo-brand__sub {
    color: var(--neo-muted);
    font-size: 13px;
    font-weight: 500;
    white-space: nowrap;
    border-left: 1px solid var(--neo-border-strong);
    padding-left: 10px;
  }

  .threads-tab {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 6px 12px;
    border-radius: 6px;
    background: transparent;
    color: var(--neo-ink);
    font-size: 13px;
    font-weight: 500;
    border: 0;
    cursor: pointer;
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .threads-tab:hover { background: var(--neo-card-hover); }
  .threads-tab--active { background: var(--neo-card-hover); }

  .neo-topbar__actions {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 6px;
  }

  .neo-key {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    height: 30px;
    padding: 0 10px;
    border-radius: 6px;
    color: var(--neo-muted);
    font-size: 12px;
  }

  .icon-button {
    display: inline-grid;
    width: 30px;
    height: 30px;
    place-items: center;
    border: 0;
    background: transparent;
    color: var(--neo-muted);
    border-radius: 6px;
    cursor: pointer;
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .icon-button:hover { background: var(--neo-card-hover); color: var(--neo-ink); }
  /* Mobile-only icon button: hidden at lg+ where the right sidebar is visible */
  .icon-button--mobile-only { display: none; }
  @media (max-width: 1023.98px) {
    .icon-button--mobile-only { display: inline-grid; }
  }

  /* === MOBILE SHEET (bottom drawer for inspector) === */
  .sheet {
    position: fixed;
    inset: 0;
    z-index: 100;
    display: flex;
    align-items: flex-end;
    justify-content: center;
  }
  .sheet__backdrop {
    position: absolute;
    inset: 0;
    background: rgba(0, 0, 0, 0.55);
    border: 0;
    cursor: pointer;
    animation: sheet-fade 180ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .sheet__panel {
    position: relative;
    width: 100%;
    max-width: 480px;
    /* Use dynamic viewport units + capped height. Reserve room for iOS safe area + 24px gap. */
    max-height: min(85dvh, calc(100dvh - 24px - env(safe-area-inset-bottom, 0px)));
    display: flex;
    flex-direction: column;
    overflow: hidden;
    background: var(--neo-bg);
    border: 1px solid var(--neo-border-strong);
    border-bottom: 0;
    border-radius: 14px 14px 0 0;
    box-shadow: 0 -20px 60px rgba(0, 0, 0, 0.5);
    animation: sheet-slide 220ms cubic-bezier(0.4, 0, 0.2, 1);
    padding-bottom: env(safe-area-inset-bottom, 0px);
  }
  .sheet__grabber {
    align-self: center;
    width: 36px;
    height: 4px;
    margin: 8px 0 4px;
    border-radius: 2px;
    background: var(--neo-border-strong);
  }
  .sheet__head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 16px 12px;
    border-bottom: 1px solid var(--neo-border);
  }
  .sheet__head h2 {
    margin: 0;
    font-size: 15px;
    font-weight: 600;
    color: var(--neo-ink);
  }
  .sheet__body {
    flex: 1 1 auto;
    min-height: 0;
    overflow-y: auto;
    overscroll-behavior: contain;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 16px;
    color: var(--neo-muted);
    font-size: 13px;
    -webkit-overflow-scrolling: touch;
  }
  .sheet__body dl { display: grid; gap: 14px; margin: 0; }
  .sheet__body dt { margin-bottom: 4px; color: var(--neo-ink); font-size: 13px; font-weight: 600; }
  .sheet__body dd {
    display: flex;
    align-items: center;
    gap: 7px;
    margin: 0;
    color: var(--neo-muted);
    font-size: 13px;
  }
  .sheet__body .cli-command {
    display: block;
    border: 1px solid var(--neo-border);
    background: var(--neo-card);
    border-radius: 4px;
    padding: 8px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 12px;
    line-height: 1.45;
    overflow-wrap: anywhere;
    white-space: normal;
  }
  /* Desktop: sheet is never used (sidebar is visible instead) */
  @media (min-width: 1024px) { .sheet { display: none; } }

  @keyframes sheet-fade {
    from { opacity: 0; }
    to { opacity: 1; }
  }
  @keyframes sheet-slide {
    from { transform: translateY(100%); }
    to { transform: translateY(0); }
  }

  /* === MAIN CONTAINER === */
  .neo-container {
    width: 100%;
    max-width: 1024px;
    margin: 0 auto;
    padding: 8px;
    display: flex;
    flex-direction: column;
    gap: 24px;
    flex: 1 1 auto;
    min-height: 0;
  }
  @media (min-width: 768px) {
    .neo-container { padding: 16px; }
  }

  /* === THREAD SEARCH === */
  .thread-search {
    display: flex;
    align-items: stretch;
    width: 100%;
  }

  .search-box {
    display: flex;
    align-items: center;
    width: 100%;
    height: 36px;
    border-radius: 6px;
    border: 1px solid var(--neo-border-strong);
    background: var(--neo-bg);
    color: var(--neo-muted);
    padding: 6px 12px 6px 32px;
    position: relative;
  }
  .search-box :global(svg) {
    position: absolute;
    left: 10px;
    top: 50%;
    transform: translateY(-50%);
    color: var(--neo-muted);
  }
  .search-box input {
    width: 100%;
    height: 100%;
    border: 0;
    outline: 0;
    background: transparent;
    color: var(--neo-ink);
    font-size: 13px;
    padding: 0;
  }
  .search-box input::placeholder { color: var(--neo-muted); }

  /* === THREAD LIST === */
  .thread-list {
    display: flex;
    flex-direction: column;
  }
  .thread-row {
    border-bottom: 1px solid var(--neo-border);
  }
  .thread-row:last-child { border-bottom: 0; }

  .thread-card {
    display: block;
    width: 100%;
    border: 0;
    background: transparent;
    border-radius: 6px;
    padding: 10px 24px;
    text-align: left;
    cursor: pointer;
    color: var(--neo-ink);
    font: inherit;
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .thread-card:hover { background: var(--neo-accent-soft); }
  .thread-card--active { background: var(--neo-accent-soft); }

  .thread-card {
    display: flex;
    gap: 8px;
  }

  .thread-card__main {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 2px;
  }

  .thread-card__title-row {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
  }
  .thread-card__title-row strong {
    font-size: 18px;
    line-height: 1.2;
    font-weight: 500;
    color: var(--neo-ink);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 100%;
  }
  .thread-card__runtime {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    min-width: 0;
    border: 1px solid var(--neo-border);
    border-radius: 999px;
    padding: 2px 7px;
    color: var(--neo-muted);
    background: var(--neo-panel-lift);
    font-size: 11px;
    line-height: 15px;
    white-space: nowrap;
  }
  .thread-card__runtime--live {
    color: var(--neo-success);
    background: var(--neo-success-soft);
    border-color: transparent;
  }
  .runtime-dot {
    width: 6px;
    height: 6px;
    border-radius: 999px;
    background: currentColor;
    flex: 0 0 auto;
  }

  .thread-card__meta {
    display: flex;
    align-items: center;
    gap: 4px;
    flex-wrap: wrap;
    color: var(--neo-muted);
    font-size: 13px;
    margin-top: 0;
  }
  .thread-card__meta > * { white-space: nowrap; }
  .thread-card__repo {
    color: var(--neo-muted);
    font-size: 13px;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 100%;
  }

  .thread-card__preview {
    display: block;
    margin-top: 6px;
    padding: 4px 8px;
    border-radius: 4px;
    border: 1px solid var(--neo-border-strong);
    background: var(--neo-card-hover);
    color: var(--neo-ink);
    font-size: 13px;
    line-height: 1.4;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .thread-card__diff {
    font-size: 13px;
    white-space: nowrap;
    display: inline-flex;
    gap: 3px;
  }

  /* === THREAD STAGE (detail view) === */
  /* Flex row at lg+: main content fills, inspector hugs right viewport edge.
     Below lg: inspector hidden (matches ampcode `hidden lg:flex`), main content centered. */
  .neo-grid {
    display: flex;
    flex-direction: column;
    flex: 1;
    width: 100%;
    min-height: 0;
  }
  @media (min-width: 1024px) {
    .neo-grid { flex-direction: row; align-items: stretch; }
  }

  .thread-stage {
    display: grid;
    flex: 1 1 0;
    min-height: calc(100vh - 110px);
    grid-template-columns: minmax(0, 1fr);
    grid-template-rows: auto 1fr auto;
    min-width: 0;
    padding: 16px;
  }

  .neo-grid__spacer { display: none; }
  @media (min-width: 1280px) {
    .neo-grid__spacer {
      display: block;
      flex: 1 1 0;
      max-width: 21em;
    }
  }
  @media (max-width: 640px) {
    .thread-stage { padding: 12px; }
  }

  .thread-head {
    position: relative;
    display: flex;
    flex-direction: column;
    align-items: center;
    text-align: center;
    gap: 8px;
    margin: 0 auto 36px;
    width: min(100%, 672px);
    padding-top: 8px;
  }

  .thread-back {
    align-self: flex-start;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    margin: 0 0 8px;
    border: 0;
    background: transparent;
    color: var(--neo-muted);
    padding: 0;
    font: inherit;
    font-size: 13px;
    cursor: pointer;
  }
  .thread-back:hover { color: var(--neo-ink); }

  .thread-head h1 {
    margin: 0;
    font-size: 26px;
    font-weight: 600;
    letter-spacing: -0.01em;
    color: var(--neo-ink);
  }

  .thread-head__meta {
    display: flex;
    align-items: center;
    justify-content: center;
    flex-wrap: wrap;
    gap: 6px;
    color: var(--neo-muted);
    font-size: 13px;
  }
  .thread-head__time { color: var(--neo-muted); }
  .thread-head__repo {
    display: flex;
    align-items: center;
    justify-content: center;
    flex-wrap: wrap;
    gap: 14px;
    color: var(--neo-muted);
    font-size: 13px;
  }
  .thread-head__repo > span {
    display: inline-flex;
    align-items: center;
    gap: 5px;
  }
  .thread-head .connection-chip {
    position: absolute;
    top: 0;
    right: 0;
  }
  @media (max-width: 640px) {
    .thread-head .connection-chip { position: static; align-self: flex-start; }
  }

  .connection-chip {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    border: 1px solid var(--neo-border-strong);
    border-radius: 6px;
    padding: 6px 9px;
    color: var(--neo-muted);
    font-size: 12px;
    white-space: nowrap;
  }
  .connection-chip--live {
    color: var(--neo-success);
    background: var(--neo-success-soft);
    border-color: transparent;
  }

  .transcript {
    display: flex;
    flex-direction: column;
    gap: 16px;
    width: 100%;
    max-width: 672px;
    margin: 0 auto;
    padding-bottom: 36px;
    padding-left: 16px;
    padding-right: 16px;
    box-sizing: border-box;
  }

  .message {
    min-width: 0;
    animation: message-enter 180ms ease-out;
    padding: 4px 0;
  }
  /* User row: full-width flex column with items-end (matches ampcode `flex w-full flex-col items-end py-1 mb-4`) */
  .message--user { display: flex; flex-direction: column; align-items: flex-end; width: 100%; min-width: 0; }

  .message__bubble {
    max-width: min(85%, 672px);
    border: 0;
    background: color-mix(in srgb, var(--neo-ink) 10%, transparent);
    border-radius: 12px;
    padding: 8px 12px;
    color: var(--neo-ink);
    font-size: 13px;
    line-height: 20px;
    font-weight: 400;
    overflow-wrap: anywhere;
    white-space: pre-wrap;
    word-break: break-word;
  }

  .message__agent {
    display: block;
    color: var(--neo-ink);
  }
  .message__agent p { margin: 0; font-size: 13px; line-height: 20px; }

  /* === Markdown rendering for assistant messages === */
  .md { font-size: 13px; line-height: 20px; color: var(--neo-ink); }
  .md :global(p) { margin: 0 0 12px; font-size: inherit; line-height: inherit; }
  .md :global(p:last-child) { margin-bottom: 0; }
  .md :global(ul.md-list) {
    margin: 8px 0 12px;
    padding-left: 22px;
    list-style: disc;
  }
  .md :global(ul.md-list li) { margin: 4px 0; padding-left: 4px; }
  .md :global(ul.md-list li::marker) { color: var(--neo-muted); }
  .md :global(h1.md-h1), .md :global(h2.md-h2), .md :global(h3.md-h3) {
    margin: 16px 0 8px;
    font-weight: 600;
    color: var(--neo-ink);
    line-height: 1.3;
  }
  .md :global(h1.md-h1) { font-size: 20px; }
  .md :global(h2.md-h2) { font-size: 17px; }
  .md :global(h3.md-h3) { font-size: 15px; }
  /* Inline code: match ampcode — tight padding, allow break anywhere on long identifiers/paths. */
  .md :global(code.md-code) {
    background: var(--neo-bubble);
    border-radius: 3px;
    padding: 1px 4px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 0.9em;
    color: var(--neo-ink);
    white-space: normal;
    overflow-wrap: anywhere;
    word-break: break-word;
  }
  /* Fenced code blocks: scroll horizontally instead of forcing wrap (preserve indentation/columns). */
  .md :global(pre.md-pre) {
    margin: 10px 0;
    padding: 12px 14px;
    background: var(--neo-card);
    border: 1px solid var(--neo-border);
    border-radius: 6px;
    overflow-x: auto;
    overflow-y: hidden;
    max-width: 100%;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 12.5px;
    line-height: 1.5;
    color: var(--neo-ink);
    -webkit-overflow-scrolling: touch;
  }
  .md :global(pre.md-pre code.md-code-block) {
    background: transparent;
    padding: 0;
    border-radius: 0;
    font-size: inherit;
    white-space: pre;
    display: block;
  }

  /* Auto-link file paths (we rewrite plain file: URIs to <a> via renderMarkdown). */
  .md :global(a.md-file-link) {
    color: var(--neo-ink);
    text-decoration: underline;
    text-underline-offset: 2px;
    text-decoration-color: color-mix(in srgb, currentColor 30%, transparent);
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 0.9em;
    word-break: break-all;
  }
  .md :global(a.md-file-link:hover) { text-decoration-color: currentColor; }
  .md :global(a.md-link) {
    color: var(--neo-ink);
    text-decoration: underline;
    text-underline-offset: 2px;
    text-decoration-color: color-mix(in srgb, currentColor 40%, transparent);
    word-break: break-all;
  }
  .md :global(a.md-link:hover) { text-decoration-color: currentColor; }
  .md :global(strong) { font-weight: 600; color: var(--neo-ink); }
  .md :global(em) { font-style: italic; }

  .streaming-text::after {
    content: '';
    display: inline-block;
    width: 7px;
    height: 1em;
    margin-left: 3px;
    vertical-align: -0.15em;
    background: var(--neo-accent);
    animation: caret 900ms steps(2) infinite;
  }

  .work-group { margin: 12px 0; }
  .work-group > summary {
    display: flex;
    align-items: center;
    gap: 10px;
    cursor: pointer;
    list-style: none;
    color: var(--neo-muted);
    font-size: 13px;
  }
  .work-group > summary::-webkit-details-marker { display: none; }
  .work-group__line { flex: 1; height: 1px; background: var(--neo-border); }
  .work-group__button {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    min-height: 28px;
    color: var(--neo-muted);
  }
  .work-group__button span {
    color: var(--neo-ink);
    font-weight: 500;
  }
  :global(.work-group__chevron) { transition: transform 140ms ease; }
  .work-group[open] :global(.work-group__chevron) { transform: rotate(90deg); }
  .work-group__body { display: grid; gap: 6px; margin-top: 8px; }

  /* Compact, flat trace blocks (ampcode-style): no card border, just inline label rows */
  .trace-block {
    margin: 4px 0;
    border: 0;
    background: transparent;
    color: var(--neo-muted);
    font-size: 14px;
  }
  .trace-block summary {
    display: flex;
    align-items: baseline;
    min-width: 0;
    min-height: 24px;
    gap: 8px;
    padding: 2px 0;
    cursor: pointer;
    list-style: none;
  }
  .trace-block summary::-webkit-details-marker { display: none; }
  :global(.trace-block__chevron) { color: var(--neo-soft); opacity: 0.6; transition: transform 140ms ease; }
  .trace-block[open] :global(.trace-block__chevron) { transform: rotate(90deg); }
  .trace-block summary span {
    overflow: hidden;
    color: var(--neo-ink);
    font-weight: 500;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .trace-block summary small,
  .trace-block summary b {
    color: var(--neo-muted);
    font-size: 12px;
    font-weight: 400;
    white-space: nowrap;
  }
  .trace-block summary small { margin-left: auto; }
  .trace-block summary b { color: var(--neo-success); }
  /* Tool-call command line: shown as $ ... in monospace */
  .trace-block__subline {
    overflow: hidden;
    padding: 4px 0 4px 16px;
    margin-top: 2px;
    color: var(--neo-muted);
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 12.5px;
    line-height: 1.5;
    overflow-wrap: anywhere;
  }
  /* Italic thinking / narrative paragraphs */
  .trace-block p {
    margin: 4px 0 4px 16px;
    padding: 0;
    color: var(--neo-muted);
    font-size: 13.5px;
    line-height: 1.6;
  }
  .trace-block--thinking p { font-style: italic; }
  .trace-block--tool :global(.patch-shell) {
    margin-top: 4px;
    padding: 0 0 0 16px;
  }

  /* === Compact trace rows (ampcode-style grouping) === */
  .trace-thinking {
    margin: 8px 0;
    font-size: 13px;
    line-height: 20px;
    color: var(--neo-ink);
  }
  /* Inherit base sizes for markdown inside thinking so it matches surrounding prose */
  .trace-thinking.md { font-size: 13px; line-height: 20px; }
  .trace-thinking--progress { color: var(--neo-muted); }
  .trace-row {
    margin: 2px 0;
    border: 0;
    background: transparent;
    font-size: 14px;
    color: var(--neo-ink);
    min-width: 0;
    max-width: 100%;
    overflow: hidden;
  }
  .trace-row > summary {
    display: flex;
    align-items: baseline;
    gap: 6px;
    padding: 2px 0;
    min-height: 22px;
    cursor: pointer;
    list-style: none;
    color: var(--neo-ink);
    transition: color 150ms cubic-bezier(0.4, 0, 0.2, 1);
    opacity: 0.85;
    min-width: 0;
    max-width: 100%;
    overflow: hidden;
  }
  .trace-row > summary::-webkit-details-marker { display: none; }
  .trace-row:hover > summary { opacity: 1; }
  .trace-row__label { color: var(--neo-ink); font-weight: 400; white-space: nowrap; }
  .trace-row__sub { color: var(--neo-muted); white-space: nowrap; }
  /* Chevron hidden by default, revealed on row hover (matches ampcode `opacity-0 group-hover/row:opacity-100`). */
  :global(.trace-row__chevron) {
    color: var(--neo-muted);
    opacity: 0;
    margin-left: 4px;
    transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1), transform 140ms ease;
  }
  .trace-row:hover > summary :global(.trace-row__chevron) { opacity: 1; }
  .trace-row[open] > summary :global(.trace-row__chevron) { opacity: 1; transform: rotate(90deg); }

  /* Expanded list under an "Explored" row */
  .trace-row__list {
    list-style: none;
    margin: 4px 0 8px 16px;
    padding: 0;
    color: var(--neo-muted);
    font-size: 13px;
    line-height: 1.6;
  }
  .trace-row__list li { display: flex; gap: 6px; padding: 1px 0; }
  .trace-row__list-label { color: var(--neo-muted); min-width: 64px; flex-shrink: 0; }
  .trace-row__list-target {
    color: var(--neo-ink);
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 12.5px;
    min-width: 0;
    flex: 1 1 auto;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* Edit row: filename as link + +N -N */
  .trace-row__file {
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 13.5px;
    color: var(--neo-accent, var(--neo-ink));
    text-decoration: underline;
    text-underline-offset: 2px;
    text-decoration-color: color-mix(in srgb, currentColor 30%, transparent);
    cursor: pointer;
  }
  .trace-row__file:hover { text-decoration-color: currentColor; }
  .trace-row__diff {
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 12.5px;
    font-weight: 400;
    display: inline-flex;
    gap: 4px;
  }
  .trace-row--edit :global(.patch-shell) {
    margin: 6px 0 10px 16px;
  }

  /* Command row: monospace `$ cmd`. Truncate to one line with ellipsis when collapsed; expand reveals full. */
  .trace-row__cmd {
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 12.5px;
    color: var(--neo-ink);
    background: transparent;
    padding: 0;
    line-height: 1.55;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    min-width: 0;
    flex: 1 1 auto;
    max-width: 100%;
    display: block;
  }
  /* When the command row is expanded, allow the cmd text to wrap (full readability) */
  .trace-row--cmd[open] > summary .trace-row__cmd {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    word-break: break-word;
  }
  .trace-row--cmd .code-panel { margin: 4px 0 8px 16px; }

  .code-panel {
    overflow: auto;
    max-height: 360px;
    margin: 0;
    border-top: 1px solid var(--neo-border);
    background: var(--neo-card);
    color: var(--neo-muted);
    padding: 10px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 12px;
    line-height: 1.5;
  }

  /* Footer wrapping the composer, fixed at the bottom of the viewport (matches ampcode `fixed bottom-0 z-30 px-2 pb-2`). */
  .composer-footer {
    position: fixed;
    left: 0;
    right: 0;
    bottom: 0;
    z-index: 30;
    padding: 8px;
    pointer-events: none; /* let the page scroll under the gutter; only the composer itself catches events */
    display: flex;
    justify-content: center;
  }
  .composer-footer > .composer { pointer-events: auto; }
  @media (min-width: 1024px) and (max-width: 1279.98px) {
    .composer-footer { padding-right: calc(8px + 21em); padding-left: 8px; }
  }
  /* Spacer that reserves vertical space at the end of the transcript so the sticky composer never covers the last message. */
  .composer-dock {
    height: 140px;
    flex-shrink: 0;
  }
  @media (max-width: 640px) {
    .composer-footer { padding: 6px; }
    .composer-dock { height: 132px; }
  }

  /* Composer: rounded-2xl card with status bar above + body below (matches ampcode `divide-y` pattern). */
  .composer {
    display: flex;
    flex-direction: column;
    width: min(100%, 672px);
    margin: 0 auto;
    padding: 0;
    border-radius: 16px;
    border: 1px solid var(--neo-border-strong);
    background: color-mix(in srgb, var(--neo-ink) 3%, var(--neo-bg));
    outline: 1px solid color-mix(in srgb, var(--neo-ink) 10%, transparent);
    outline-offset: -1px;
    overflow: clip;
  }
  /* Status bar: green dot + "Connected" + Amp CLI · /path · branch */
  .composer-status {
    display: flex;
    align-items: center;
    gap: 7px;
    min-width: 0;
    padding: 6px 12px;
    border-bottom: 1px solid var(--neo-border);
    color: var(--neo-muted);
    font-size: 12px;
    line-height: 16px;
    white-space: nowrap;
    overflow: hidden;
  }
  .runtime-dot {
    flex: 0 0 6px;
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--neo-soft);
  }
  .composer-status--live .runtime-dot { background: var(--neo-success); }
  .composer-status--connecting .runtime-dot { background: #d4a045; }
  .composer-status__main {
    flex: 0 0 auto;
    font-weight: 500;
    color: var(--neo-ink);
  }
  .composer-status__sep { color: var(--neo-soft); flex: 0 0 auto; }
  .composer-status__part {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    color: var(--neo-muted);
  }
  /* Body: textarea + actions row. Solid muted overlay so it stands out from the card bg (matches ampcode visually). */
  .composer-body {
    display: flex;
    flex-direction: column;
    background: color-mix(in srgb, var(--neo-ink) 7%, var(--neo-bg));
  }
  .composer textarea {
    min-height: 60px;
    max-height: 180px;
    resize: none;
    border: 0;
    outline: 0;
    background: transparent;
    color: var(--neo-ink);
    font: inherit;
    font-size: 13px;
    line-height: 20px;
    padding: 12px;
    width: 100%;
  }
  .composer textarea::placeholder { color: var(--neo-muted); }
  /* Bottom action row: attach (left) + send (right) */
  .composer-actions {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 6px 8px;
  }
  .composer-actions__left { display: flex; align-items: center; gap: 4px; }
  .send-button {
    display: inline-grid;
    width: 28px;
    height: 28px;
    place-items: center;
    border: 0;
    background: var(--neo-accent);
    color: var(--neo-bg);
    border-radius: 50%;
    cursor: pointer;
    transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .send-button:hover:not(:disabled) { opacity: 0.85; }
  .send-button:disabled { cursor: not-allowed; opacity: 0.3; }

  /* Hidden on mobile (parity with ampcode `hidden lg:flex`). Shown at lg+ flush to right edge. */
  .inspector { display: none; }
  @media (min-width: 1024px) {
    .inspector {
      display: flex;
      flex-direction: column;
      gap: 16px;
      width: 100%;
      max-width: 21em;
      flex-shrink: 0;
      align-self: flex-start;
      position: sticky;
      top: 24px;
      margin-top: 24px;
      padding: 16px 24px;
      border: 1px solid var(--neo-border);
      border-right: 0;
      border-top-left-radius: 8px;
      border-bottom-left-radius: 8px;
      border-top-right-radius: 0;
      border-bottom-right-radius: 0;
      background: color-mix(in srgb, var(--neo-ink) 3%, var(--neo-bg));
      color: var(--neo-muted);
      font-size: 12px;
      line-height: 16px;
    }
  }
  .inspector-card,
  .error-card {
    border: 0;
    background: transparent;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 16px;
  }
  .inspector-card__actions { display: flex; align-items: center; justify-content: flex-end; gap: 4px; margin: 0; }
  .inspector-card__actions button:not(.icon-button) {
    border: 1px solid var(--neo-border-strong);
    background: transparent;
    color: var(--neo-ink);
    border-radius: 6px;
    padding: 4px 10px;
    cursor: pointer;
    font-size: 12px;
  }

  /* Single-line field rows: icon + value, matching ampcode's 12px / 16px compact list. */
  .inspector-fields {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .inspector-field {
    display: flex;
    align-items: center;
    gap: 4px;
    min-width: 0;
    color: var(--neo-muted);
    font-size: 12px;
    line-height: 16px;
  }
  .inspector-field > :global(svg) { color: var(--neo-muted); flex-shrink: 0; }
  .inspector-field > span {
    color: var(--neo-muted);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
  }
  .inspector-field--usage > span {
    display: flex;
    align-items: center;
    gap: 4px;
  }
  .usage-cost-link {
    color: var(--neo-muted);
    text-decoration: none;
    flex: 0 0 auto;
  }
  .usage-cost-link:hover {
    color: var(--neo-ink);
    text-decoration: underline;
    text-underline-offset: 2px;
  }
  .inspector-field--mono > span {
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  }

  /* Sub-section (heading + content) used for "Open in CLI" etc. */
  .inspector-section { display: flex; flex-direction: column; gap: 6px; }
  .inspector-section__head { color: var(--neo-ink); font-size: 12px; font-weight: 600; }
  .cli-row {
    display: flex;
    align-items: center;
    gap: 4px;
    min-width: 0;
  }
  .cli-row .cli-command { flex: 1 1 auto; min-width: 0; }
  .cli-row__copy {
    flex-shrink: 0;
    width: 24px;
    height: 24px;
  }
  .cli-row__copy--ok { color: var(--neo-success) !important; }
  .status-ok { color: var(--neo-success) !important; }
  .mono-line {
    display: block !important;
    overflow-wrap: anywhere;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  }
  .runtime-section {
    border-top: 1px solid var(--neo-border);
    padding-top: 14px;
  }
  .runtime-section h2 {
    margin: 0 0 8px;
    color: var(--neo-ink);
    font-size: 13px;
    font-weight: 600;
  }
  .runtime-section p {
    margin: 0;
    color: var(--neo-muted);
    font-size: 12px;
    line-height: 1.45;
    overflow-wrap: anywhere;
  }
  .runtime-section--error p { color: var(--neo-danger); }
  .runtime-list {
    display: grid;
    gap: 8px;
  }
  .runtime-pill {
    display: grid;
    gap: 3px;
    border: 1px solid var(--neo-border);
    border-radius: 6px;
    background: var(--neo-panel-lift);
    padding: 8px;
  }
  .runtime-pill span {
    color: var(--neo-soft);
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0;
  }
  .runtime-pill--link {
    color: var(--neo-ink);
    font: inherit;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 12px;
    text-align: left;
    width: 100%;
    cursor: pointer;
    word-break: break-all;
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1), border-color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .runtime-pill--link:hover { background: var(--neo-card-hover); border-color: var(--neo-border-strong); }
  .approval-card {
    display: grid;
    gap: 8px;
    border: 1px solid var(--neo-border);
    border-radius: 6px;
    background: var(--neo-panel-lift);
    padding: 10px;
  }
  .approval-card strong {
    color: var(--neo-ink);
    font-size: 13px;
    font-weight: 600;
  }
  .approval-card pre {
    overflow: auto;
    max-height: 180px;
    margin: 0;
    border: 1px solid var(--neo-border);
    border-radius: 4px;
    background: var(--neo-bg);
    color: var(--neo-muted);
    padding: 8px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 11px;
    line-height: 1.45;
  }
  .approval-card__actions {
    display: flex;
    gap: 8px;
  }
  .approval-card__actions button {
    flex: 1;
    min-height: 30px;
    border: 1px solid var(--neo-border-strong);
    border-radius: 6px;
    background: transparent;
    color: var(--neo-ink);
    cursor: pointer;
    font-size: 12px;
  }
  .approval-card__actions button:first-child {
    background: var(--neo-accent);
    color: var(--neo-bg);
    border-color: transparent;
  }
  .diff-stats { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 13px; gap: 6px; }
  :global(.diff-add) { color: var(--neo-success); }
  :global(.diff-del) { color: var(--neo-danger); }
  :global(.diff-mod) { color: #d4a045; }

  /* === HANDOFF CARD (above transcript when thread continues from another) === */
  .handoff-card {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    padding: 8px 10px;
    background: color-mix(in srgb, var(--neo-bubble) 70%, transparent);
    border: 1px solid var(--neo-border);
    border-radius: 6px;
    color: var(--neo-muted);
    font-size: 13px;
    line-height: 1.4;
    cursor: pointer;
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1);
    width: 100%;
    font-family: inherit;
    text-align: left;
  }
  .handoff-card:hover { background: var(--neo-card-hover); }
  .handoff-card > :global(svg) { color: var(--neo-muted); flex-shrink: 0; margin-top: 2px; }
  .handoff-card__body { min-width: 0; flex: 1; }
  .handoff-card__head { color: var(--neo-ink); font-size: 12px; opacity: 0.7; }
  .handoff-card__id {
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-weight: 600;
    color: var(--neo-ink);
    opacity: 1;
    word-break: break-all;
  }
  .handoff-card__instr {
    margin-top: 2px;
    font-size: 12px;
    color: var(--neo-muted);
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 1;
    line-clamp: 1;
    -webkit-box-orient: vertical;
  }

  /* === LEFT-SIDE MESSAGE NAV (per-message jump ticks) === */
  /* Hidden on mobile (matches ampcode `hidden lg:block`). */
  .msg-nav {
    position: fixed;
    left: 16px;
    top: 50%;
    transform: translateY(-50%);
    z-index: 40;
    display: none;
  }
  @media (min-width: 1024px) {
    .msg-nav { display: block; }
  }
  .msg-nav__inner {
    display: flex;
    flex-direction: column;
    padding: 4px 0;
    cursor: default;
  }
  .msg-nav__tick {
    display: flex;
    align-items: center;
    gap: 2px;
    padding: 3px 0;
    width: 28px;
    border: 0;
    background: transparent;
    cursor: pointer;
    color: var(--neo-ink);
  }
  .msg-nav__dot {
    width: 2px;
    height: 2px;
    border-radius: 50%;
    background: currentColor;
    opacity: 0.5;
    transition: opacity 200ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .msg-nav__line {
    width: 24px;
    height: 1px;
    border-radius: 1px;
    background: currentColor;
    transform-origin: left center;
    transform: scaleX(0.8);
    opacity: 0.3;
    transition: opacity 200ms cubic-bezier(0.4, 0, 0.2, 1), transform 200ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  /* Primary tick (first turn): full width, slightly stronger */
  .msg-nav__tick--primary .msg-nav__line {
    transform: scaleX(1);
    opacity: 0.5;
  }
  /* Hover: every tick brightens */
  .msg-nav__tick:hover .msg-nav__dot,
  .msg-nav__tick:hover .msg-nav__line { opacity: 1; }

  /* Hovering the nav reveals every tick at full opacity (matches ampcode group-hover behavior) */
  .msg-nav:hover .msg-nav__dot { opacity: 1; }
  .msg-nav:hover .msg-nav__line { opacity: 1; }

  /* Popover that appears next to the nav while hovering — numbered list of turns */
  .msg-nav__popover {
    position: absolute;
    top: 50%;
    left: calc(100% + 8px);
    transform: translateY(-50%);
    width: 384px;
    max-height: 384px;
    overflow-y: auto;
    padding: 4px;
    background: var(--neo-bg);
    border: 1px solid var(--neo-border);
    border-radius: 6px;
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.35);
    opacity: 0;
    pointer-events: none;
    transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
    z-index: 50;
  }
  /* Invisible bridge so mouse can cross from the nav into the popover without leaving the hover area */
  .msg-nav__popover::before {
    content: '';
    position: absolute;
    top: 0;
    bottom: 0;
    left: -12px;
    width: 12px;
  }
  .msg-nav--open .msg-nav__popover {
    opacity: 1;
    pointer-events: auto;
  }
  .msg-nav__popover-item {
    display: flex;
    align-items: baseline;
    gap: 6px;
    width: 100%;
    padding: 8px 12px;
    border: 0;
    background: transparent;
    color: var(--neo-ink);
    text-align: left;
    font: inherit;
    font-size: 13px;
    border-radius: 4px;
    cursor: pointer;
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .msg-nav__popover-item:hover { background: var(--neo-card-hover); }
  .msg-nav__popover-index { color: var(--neo-muted); font-variant-numeric: tabular-nums; flex-shrink: 0; }
  .msg-nav__popover-text {
    flex: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .cli-command {
    display: block;
    max-width: 100%;
    border: 1px solid var(--neo-border);
    background: var(--neo-card);
    border-radius: 6px;
    padding: 6px 10px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 11.5px;
    line-height: 1.5;
    color: var(--neo-ink);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .error-card { margin-top: 12px; color: var(--neo-danger); font-size: 13px; }

  .empty-state,
  .loading-row {
    display: grid;
    place-items: center;
    gap: 10px;
    min-height: 420px;
    color: var(--neo-muted);
    text-align: center;
  }
  .empty-state h1 { margin: 0; color: var(--neo-ink); }
  .empty-state p { margin: 0; max-width: 360px; }

  :global(.spin) { animation: spin 900ms linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  @keyframes caret { 50% { opacity: 0; } }
  @keyframes message-enter {
    from { opacity: 0; transform: translateY(5px); }
    to { opacity: 1; transform: translateY(0); }
  }

  /* === RESPONSIVE === */
  @media (max-width: 640px) {
    .auth-shell { align-items: start; padding: 56px 14px 24px; }
    .auth-panel { padding: 18px; }

    .neo-topbar { padding: 6px 8px; gap: 2px; }
    .neo-brand { padding: 0 4px; gap: 8px; }
    .neo-brand__logo { height: 18px; }
    .neo-brand__sub { font-size: 12px; padding-left: 8px; }
    .neo-key { display: none; }

    .threads-tab { padding: 5px 10px; font-size: 12px; }

    .neo-container { padding: 8px; gap: 20px; }

    .thread-card { padding: 10px 12px; }
    .thread-card__title-row strong { font-size: 16px; }
    .thread-card__meta { font-size: 12px; gap: 4px; }
    .thread-card__preview { font-size: 12px; }

    .thread-head { gap: 8px; margin-bottom: 22px; width: 100%; }
    .thread-head h1 { font-size: 22px; line-height: 1.18; }

    .transcript { width: 100%; gap: 18px; padding-bottom: 18px; }
    .message__bubble { max-width: min(85%, 672px); font-size: 13px; }
    .message__agent { display: block; }
    .message__agent p { font-size: 13px; line-height: 20px; }
    .work-group > summary { gap: 8px; font-size: 12px; }
    .trace-block summary { padding: 8px; }
    .code-panel { max-height: 300px; font-size: 11px; }

    .composer { width: 100%; border-radius: 14px; }
    .composer textarea { min-height: 56px; padding: 10px 12px; }
    .composer-actions { padding: 6px 8px; }
    .send-button { width: 26px; height: 26px; }
    .composer textarea { min-height: 36px; font-size: 14px; }
    .send-button { width: 36px; height: 36px; }
  }
</style>
