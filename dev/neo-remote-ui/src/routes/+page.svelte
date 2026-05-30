<script lang="ts">
  import {
    ArrowLeft,
    ArrowDown,
    ArrowUp,
    CheckCircle2,
    ChevronRight,
    CircleStop,
    Copy,
    Download,
    FileCode2,
    Gauge,
    GitBranch,
    ImagePlus,
    KeyRound,
    Laptop,
    List,
    Loader2,
    LogOut,
    Moon,
    Info,
    PanelRight,
    Search,
    Send,
    Sparkles,
    SquarePen,
    Sun,
    Trash2,
    Wrench,
    Wifi,
    WifiOff,
    X
  } from 'lucide-svelte';
  import { pushState, replaceState } from '$app/navigation';
  import { onMount, tick } from 'svelte';
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
    inputIncomplete?: unknown;
    inputPartialJSON?: unknown;
    inputPartialJSONDelta?: unknown;
    complete?: boolean;
    content?: unknown;
    patch?: unknown;
    diff?: unknown;
    run?: unknown;
    args?: unknown;
    toolRun?: unknown;
    hidden?: unknown;
    userInput?: unknown;
    toolUseID?: string;
    blockState?: string;
    startTime?: number | string;
    finalTime?: number | string;
    timestamp?: number | string;
    source?: Record<string, unknown>;
    image_url?: unknown;
    summary?: unknown;
    url?: string;
    uri?: string;
    href?: string;
    path?: string;
    savedPath?: string;
    saved_path?: string;
    sourcePath?: string;
    source_path?: string;
    data?: string;
    base64?: string;
    mediaType?: string;
    media_type?: string;
    mimeType?: string;
    mime_type?: string;
    filename?: string;
    file_name?: string;
    title?: string;
    attachmentUrl?: string;
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
    | { kind: 'assistant'; messages: NeoMessage[]; key: string }
    | { kind: 'compaction'; cutMessageId: string; key: string };

  type ThreadSummary = {
    id: string;
    title: string;
    repo: string;
    branch: string;
    preview: string;
    messageCount: number;
    agentMode: string;
    reasoningEffort?: string;
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
    reasoningEffort?: string;
    messages: NeoMessage[];
    contextLabel: string;
    contextUsage?: { used: number; total: number };
    cost?: { amount: number; free?: number; paid?: number; included?: boolean; url?: string };
    costBreakdownURL?: string;
    referencedThread?: { threadId: string; instructions?: string };
    compactionRecords?: Record<string, unknown>[];
  };

  type Incoming = Record<string, unknown>;
  type QueuedMessage = {
    id: string;
    messageId: string;
    preview: string;
    content: ContentBlock[];
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
  type RuntimeEvent = {
    id: string;
    label: string;
    detail: string;
  };
  type RuntimeTrace = {
    id: string;
    name: string;
    status: string;
    detail: string;
    startedAt: string;
    endedAt: string;
    eventCount: number;
    latestEvent: string;
    attributes: Record<string, unknown>;
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
  type ConnectOptions = {
    bootstrapExecutor?: boolean;
    agentMode?: string;
    reasoningEffort?: string;
    environment?: Record<string, unknown>;
    workingDirectory?: string;
  };
  type ComposerAttachment = {
    id: string;
    file: File;
    previewUrl: string;
    name: string;
    mediaType: string;
    size: number;
  };

  let theme = $state<Theme>('system');
  let devMode = $state(false);
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
  let newThreadStarting = $state(false);
  let connection = $state<'offline' | 'connecting' | 'connected'>('offline');
  let agentState = $state<AgentState>('idle');
  let composer = $state('');
  let composerAttachments = $state<ComposerAttachment[]>([]);
  let composerUploadActive = $state(false);
  let composerTextarea = $state<HTMLTextAreaElement | undefined>();
  let mentionPickerOpen = $state(false);
  let mentionSearch = $state('');
  let mentionStart = $state<number | null>(null);
  let mentionEnd = $state<number | null>(null);
  let mentionActiveIndex = $state(0);
  let mentionSearchInput = $state<HTMLInputElement | undefined>();
  let attachmentInput = $state<HTMLInputElement | undefined>();
  let socket: WebSocket | null = null;
  let socketGeneration = 0;
  let reconnectTimer: number | null = null;
  let reconnectResetTimer: number | null = null;
  let reconnectAttempts = 0;
  let pingTimer: number | null = null;
  let lastServerFrameAt = 0;
  let lastPingTickAt = 0;
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
  const railRelationships = $derived(relationships.filter(isVisibleRailRelationship));
  const previousThreadHint = $derived(previousThreadForReference());
  const mentionThreadOptions = $derived(threadMentionOptions());
  let executorConnected = $state(false);
  let executorInfo = $state<Record<string, unknown>>({});
  let executorStatuses = $state<ExecutorStatus[]>([]);
  let runtimeEvents = $state<RuntimeEvent[]>([]);
  let runtimeTraces = $state<RuntimeTrace[]>([]);
  let inferenceTools = $state<{ messageId: string; agentMode: string; tools: string[] } | null>(null);
  let toolLeases = $state<ToolLease[]>([]);
  let draftPreview = $state('');
  let pendingNavigation = $state('');
  let mainThreadId = $state('');
  let maxTokensLabel = $state('');
  let retryNotice = $state('');
  let settingsMenuOpen = $state<'mode' | 'effort' | null>(null);
  let newActivityBelow = $state(false);
  let transcriptScrollPlan:
    | { kind: 'follow'; top: number; manualScrollVersion: number; force: boolean }
    | { kind: 'preserve'; top: number; manualScrollVersion: number }
    | null = null;
  let transcriptScrollScheduled = false;
  let programmaticScrollUntil = 0;
  let programmaticScrollKind: 'follow' | 'preserve' | '' = '';
  let manualTranscriptScrollVersion = 0;
  let transcriptFollowPinned = true;
  let transcriptTouchStartY = 0;
  const devSignalCount = $derived.by(() => {
    let count = artifacts.length + executorStatuses.length + runtimeEvents.length + runtimeTraces.length + toolLeases.length;
    if (inferenceTools) count += 1;
    if (retryNotice) count += 1;
    return count;
  });
  const maxComposerImages = 8;
  const maxComposerImageEncodedBytes = 5_138_022;
  const maxComposerImageBytes = Math.floor(maxComposerImageEncodedBytes / 4) * 3;
  const maxQueuedMessages = 5;
  const reconnectDelayMs = 1_000;
  const maxReconnectDelayMs = 30_000;
  const maxReconnectAttempts = 124;
  const reconnectAttemptsResetMs = 30_000;
  const pingIntervalMs = 30_000;
  const defaultAgentMode = 'smart';
  const composerImageAccept = 'image/png,image/jpeg,image/gif,image/webp,.png,.jpg,.jpeg,.gif,.webp';
  const composerImageMediaTypes = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp']);
  const agentModeOptions = ['smart', 'large', 'rush', 'deep', 'nostromo', 'agg-man'];
  const visibleAgentModeOptions = ['smart', 'large', 'rush', 'deep', 'nostromo'];
  const agentModeLabels: Record<string, string> = {
    smart: 'Smart',
    large: 'Large',
    rush: 'Rush',
    deep: 'Deep',
    nostromo: 'Nostromo',
    'agg-man': 'Agg'
  };
  const reasoningEffortLabels: Record<string, string> = {
    none: 'None',
    low: 'Low',
    medium: 'Medium',
    high: 'High',
    xhigh: 'XHigh',
    max: 'Max'
  };

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
    if (settingsMenuOpen) { settingsMenuOpen = null; return; }
    if (mobileInspectorOpen) { mobileInspectorOpen = false; return; }
  }

  const activeMessages = $derived.by(() => dedupeReplayMessages(detail?.messages ?? []));
  const activeCompactionRecords = $derived.by(() => compactionRecords.length > 0 ? compactionRecords : (detail?.compactionRecords ?? []));
  const transcriptItems = $derived.by(() => groupTranscriptItems(activeMessages, activeCompactionRecords));
  // Per-message jump nav: only user turns are anchors (matches ampcode pattern of one mark per turn)
  const userTurnAnchors = $derived(
    activeMessages
      .filter((m) => m.role === 'user' && userPreviewFromBlocks(m.content).trim().length > 0)
      .map((m) => ({
        messageId: m.messageId,
        preview: userPreviewFromBlocks(m.content).replace(/\s+/g, ' ').trim().slice(0, 120),
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

  function pageScroller() {
    if (typeof document === 'undefined') return null;
    return document.scrollingElement as HTMLElement | null;
  }

  function pageDistanceFromBottom() {
    if (typeof window === 'undefined') return 0;
    const scroller = pageScroller();
    if (!scroller) return 0;
    return scroller.scrollHeight - (scroller.scrollTop + window.innerHeight);
  }

  function isTranscriptPinnedToBottom() {
    return pageDistanceFromBottom() <= 24;
  }

  function scrollTranscriptToBottom(behavior: ScrollBehavior = 'auto') {
    const scroller = pageScroller();
    if (!scroller) return;
    programmaticScrollUntil = Date.now() + 250;
    programmaticScrollKind = 'follow';
    transcriptFollowPinned = true;
    scroller.scrollTo({ top: scroller.scrollHeight, behavior });
    newActivityBelow = false;
  }

  function restoreTranscriptScrollTop(top: number) {
    const scroller = pageScroller();
    if (!scroller) return;
    programmaticScrollUntil = Date.now() + 250;
    programmaticScrollKind = 'preserve';
    scroller.scrollTop = Math.max(0, top);
  }

  async function flushTranscriptScrollPlan() {
    if (transcriptScrollScheduled) return;
    transcriptScrollScheduled = true;
    await tick();
    await new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
    const plan = transcriptScrollPlan;
    transcriptScrollPlan = null;
    transcriptScrollScheduled = false;
    if (!plan) return;
    if (plan.kind === 'follow') {
      if (!plan.force && manualTranscriptScrollVersion !== plan.manualScrollVersion && !isTranscriptPinnedToBottom()) {
        newActivityBelow = true;
        return;
      }
      scrollTranscriptToBottom();
      return;
    }
    if (manualTranscriptScrollVersion !== plan.manualScrollVersion) return;
    restoreTranscriptScrollTop(plan.top);
  }

  function planTranscriptScroll(options: { forceFollow?: boolean; markNewActivity?: boolean } = {}) {
    if (typeof window === 'undefined') return;
    const scroller = pageScroller();
    const top = scroller?.scrollTop ?? window.scrollY;
    const manualScrollVersion = manualTranscriptScrollVersion;
    const pinnedNow = isTranscriptPinnedToBottom();
    transcriptFollowPinned = pinnedNow;
    const shouldFollow = Boolean(options.forceFollow) || pinnedNow;
    if (shouldFollow) {
      transcriptScrollPlan = { kind: 'follow', top, manualScrollVersion, force: Boolean(options.forceFollow) };
    } else {
      transcriptScrollPlan = { kind: 'preserve', top, manualScrollVersion };
      if (options.markNewActivity !== false) newActivityBelow = true;
    }
    void flushTranscriptScrollPlan();
  }

  function handlePageScroll() {
    if (transcriptScrollScheduled && transcriptScrollPlan?.kind === 'preserve') {
      return;
    }
    if (Date.now() <= programmaticScrollUntil) {
      if (programmaticScrollKind === 'preserve') return;
      if (programmaticScrollKind === 'follow' && isTranscriptPinnedToBottom()) return;
    }
    programmaticScrollKind = '';
    manualTranscriptScrollVersion += 1;
    transcriptFollowPinned = isTranscriptPinnedToBottom();
    if (transcriptFollowPinned) newActivityBelow = false;
  }

  function markTranscriptDetachedByUser() {
    programmaticScrollKind = '';
    programmaticScrollUntil = 0;
    manualTranscriptScrollVersion += 1;
    transcriptFollowPinned = false;
  }

  function handleTranscriptWheel(event: WheelEvent) {
    if (event.deltaY < -2) markTranscriptDetachedByUser();
  }

  function handleTranscriptTouchStart(event: TouchEvent) {
    transcriptTouchStartY = event.touches[0]?.clientY ?? 0;
  }

  function handleTranscriptTouchMove(event: TouchEvent) {
    const y = event.touches[0]?.clientY ?? 0;
    if (transcriptTouchStartY > 0 && y - transcriptTouchStartY > 6) markTranscriptDetachedByUser();
  }

  function isEditableTarget(target: EventTarget | null) {
    if (!(target instanceof HTMLElement)) return false;
    const tag = target.tagName.toLowerCase();
    return tag === 'input' || tag === 'textarea' || tag === 'select' || target.isContentEditable;
  }

  function handleTranscriptKeydown(event: KeyboardEvent) {
    if (isEditableTarget(event.target)) return;
    if (event.key === 'PageUp' || event.key === 'Home' || event.key === 'ArrowUp' || (event.key === ' ' && event.shiftKey)) {
      markTranscriptDetachedByUser();
    }
  }

  function jumpToLatest() {
    scrollTranscriptToBottom('smooth');
  }
  // Thread-reference marker derived from current messages (messages stream in after initial detail load).
  // Use the thread title as the displayed "Instructions:" sentence, matching ampcode.
  const referencedThread = $derived.by(() => {
    const base = referencedThreadFromMessages(activeMessages) ?? detail?.referencedThread;
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
    devMode = localStorage.getItem('neo-remote-dev-mode') === '1';
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
    addEventListener('keydown', handleTranscriptKeydown);
    addEventListener('wheel', handleTranscriptWheel, { passive: true });
    addEventListener('touchstart', handleTranscriptTouchStart, { passive: true });
    addEventListener('touchmove', handleTranscriptTouchMove, { passive: true });
    addEventListener('scroll', handlePageScroll, { passive: true });
    return () => {
      removeEventListener('popstate', handlePopState);
      removeEventListener('keydown', handleEscape);
      removeEventListener('keydown', handleTranscriptKeydown);
      removeEventListener('wheel', handleTranscriptWheel);
      removeEventListener('touchstart', handleTranscriptTouchStart);
      removeEventListener('touchmove', handleTranscriptTouchMove);
      removeEventListener('scroll', handlePageScroll);
      clearComposerAttachments();
      disconnect();
    };
  });

  $effect(() => {
    if (typeof document !== 'undefined') {
      document.documentElement.dataset.theme = theme;
      localStorage.setItem('neo-remote-theme', theme);
    }
  });

  $effect(() => {
    if (typeof localStorage !== 'undefined') {
      localStorage.setItem('neo-remote-dev-mode', devMode ? '1' : '0');
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
    runtimeEvents = [];
    runtimeTraces = [];
    inferenceTools = null;
    toolLeases = [];
    draftPreview = '';
    pendingNavigation = '';
    mainThreadId = '';
    maxTokensLabel = '';
    retryNotice = '';
    newActivityBelow = false;
    transcriptFollowPinned = true;
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
      const result = await rpc('listThreads', neoThreadListParams(80));
      const rawThreads = Array.isArray(result?.threads) ? result.threads : [];
      threads = rawThreads.map(threadSummaryFromAPI).filter(Boolean) as ThreadSummary[];
      isAuthenticated = true;
      lastError = '';
      const targetThreadId = threadIdFromURL() || selectedThreadId || '';
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
    composer = '';
    clearComposerAttachments();
    resetRuntimeState();
    localStorage.removeItem('neo-remote-api-key');
    syncThreadURL('', true);
    disconnect();
  }

  function headers(options: { ampClient?: boolean } = {}) {
    const next: Record<string, string> = { 'Content-Type': 'application/json' };
    if (apiKey.trim()) {
      next.Authorization = `Bearer ${apiKey.trim()}`;
    }
    if (options.ampClient) {
      next['X-Amp-Client-Application'] = 'CLI';
      next['X-Amp-Client-Type'] = 'cli';
      next['X-Amp-Client-Version'] = 'neo-remote-ui';
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
      headers: headers({ ampClient: true }),
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

  function neoThreadListParams(limit: number): Record<string, unknown> {
    return { limit };
  }

  function threadCanUseLocalRuntime(thread: Record<string, unknown>) {
    const messages = Array.isArray(thread.messages) ? thread.messages : [];
    return Boolean(threadMapAgentMode(thread, messages));
  }

  async function importThreadIntoRuntime(threadId: string, thread: Record<string, unknown>) {
    if (!threadId || !threadCanUseLocalRuntime(thread)) return false;
    const params = new URLSearchParams({
      'rvt-method': 'getOrCreate',
      'rvt-key': threadId,
      'rvt-skip-ready-wait': 'true',
      'cliproxy-client': 'neo-remote-ui'
    });
    const runtimeKey = apiKey.trim();
    if (runtimeKey) params.set('auth_token', runtimeKey);
    const response = await fetch(`/gateway/threadActor/request/import?${params.toString()}`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({ thread })
    });
    if (!response.ok && response.status !== 409) {
      throw new RpcError('importThread', response.status);
    }
    return true;
  }

  async function refreshThreadUsageInfo(threadId: string) {
    if (!threadId) return;
    try {
      const displayCost = await rpc('threadDisplayCostInfo', { threadID: threadId });
      applyThreadUsageInfo(threadId, displayCost);
    } catch {
      // Cost display info is advisory; message usage still drives the context label.
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
      const result = await rpc('listThreads', neoThreadListParams(120));
      const rawThreads = Array.isArray(result?.threads) ? result.threads : [];
      threads = rawThreads.map(threadSummaryFromAPI).filter(Boolean) as ThreadSummary[];
      const targetThreadId = preferredThreadId || selectedThreadId || '';
      if (targetThreadId) {
        void openThread(targetThreadId, { replaceURL: true });
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

  async function startNewThread() {
    if (!apiKey.trim() || newThreadStarting) return;

    newThreadStarting = true;
    lastError = '';
    const threadId = newThreadID();
    const sourceEnvironment = cloneRecord(environment);
    const workingDirectory = runtimeWorkspacePath();
    if (workingDirectory && !stringFrom(sourceEnvironment.workingDirectory) && !stringFrom(sourceEnvironment.cwd)) {
      sourceEnvironment.workingDirectory = workingDirectory;
    }
    const repo = repoFromEnv(sourceEnvironment) || detail?.repo || selectedSummary?.repo || 'local';
    const branch = branchFromEnv(sourceEnvironment) || detail?.branch || selectedSummary?.branch || 'main';
    const agentMode = currentComposerMode();
    const reasoningEffort = currentComposerReasoningEffort();
    const summary: ThreadSummary = {
      id: threadId,
      title: 'Untitled',
      repo,
      branch,
      preview: '',
      messageCount: 0,
      agentMode,
      reasoningEffort,
      updatedLabel: 'now'
    };

    try {
      closeThreadMentionPicker();
      clearComposerAttachments();
      composer = '';
      threads = [summary, ...threads.filter((thread) => thread.id !== threadId)];
      selectedThreadId = threadId;
      syncThreadURL(threadId, false);
      resetRuntimeState();
      environment = sourceEnvironment;
      detail = threadDetailFromSummary(summary);
      mobilePane = 'thread';
      loadingThread = false;
      disconnect();
      connect(threadId, 0, {
        bootstrapExecutor: true,
        agentMode,
        reasoningEffort,
        environment: sourceEnvironment,
        workingDirectory
      });
      queueMicrotask(() => composerTextarea?.focus());
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    } finally {
      newThreadStarting = false;
    }
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
      compactionRecords = detail.compactionRecords ?? [];
      if (!threadCanUseLocalRuntime(thread)) {
        lastError = 'Thread is not available in the local runtime.';
        connection = 'offline';
        executorConnected = false;
        void refreshThreadUsageInfo(threadId);
        return;
      }
      await importThreadIntoRuntime(threadId, thread);
      void refreshThreadUsageInfo(threadId);
      connect(threadId, Number(thread?.v ?? detail.messages.length));
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      detail = threadDetailFromSummary(threads.find((thread) => thread.id === threadId));
      connection = 'offline';
      executorConnected = false;
    } finally {
      loadingThread = false;
    }
  }

  function clearReconnectTimer() {
    if (reconnectTimer !== null) {
      window.clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  }

  function clearReconnectResetTimer() {
    if (reconnectResetTimer !== null) {
      window.clearTimeout(reconnectResetTimer);
      reconnectResetTimer = null;
    }
  }

  function scheduleReconnectAttemptsReset() {
    clearReconnectResetTimer();
    reconnectResetTimer = window.setTimeout(() => {
      reconnectResetTimer = null;
      reconnectAttempts = 0;
    }, reconnectAttemptsResetMs);
  }

  function clearPingTimer() {
    if (pingTimer !== null) {
      window.clearInterval(pingTimer);
      pingTimer = null;
    }
  }

  function clearConnectionError() {
    if (/websocket/i.test(lastError)) lastError = '';
  }

  function scheduleReconnect(threadId: string, version: number, options: ConnectOptions, generation: number) {
    if (reconnectTimer !== null || generation !== socketGeneration || selectedThreadId !== threadId) return;
    if (reconnectAttempts >= maxReconnectAttempts) {
      connection = 'offline';
      lastError = 'Connection failed: WebSocket connection failed';
      return;
    }

    const jitter = 0.8 + Math.random() * 0.4;
    const delay = Math.min(reconnectDelayMs * 2 ** reconnectAttempts, maxReconnectDelayMs) * jitter;
    reconnectAttempts += 1;
    connection = 'connecting';
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = null;
      if (generation !== socketGeneration || selectedThreadId !== threadId) return;
      connect(threadId, version, options, true);
    }, delay);
  }

  function startPingTimer(activeSocket: WebSocket, threadId: string, version: number, options: ConnectOptions, generation: number) {
    clearPingTimer();
    lastServerFrameAt = Date.now();
    lastPingTickAt = lastServerFrameAt;
    pingTimer = window.setInterval(() => {
      if (socket !== activeSocket || generation !== socketGeneration || selectedThreadId !== threadId) {
        clearPingTimer();
        return;
      }
      const now = Date.now();
      if (now - lastPingTickAt > pingIntervalMs * 3) {
        lastServerFrameAt = now;
        lastPingTickAt = now;
        return;
      }
      lastPingTickAt = now;
      if (activeSocket.readyState !== WebSocket.OPEN) return;
      if (now - lastServerFrameAt > pingIntervalMs * 2) {
        try {
          activeSocket.close(4000, 'Pong timeout');
        } catch {
          socket = null;
          connection = 'offline';
          scheduleReconnect(threadId, version, options, generation);
        }
        return;
      }
      activeSocket.send('ping');
    }, pingIntervalMs);
  }

  function connect(threadId: string, version = 0, options: ConnectOptions = {}, reconnecting = false) {
    if (!threadId || typeof WebSocket === 'undefined') return;
    clearReconnectTimer();
    clearPingTimer();
    const previousSocket = socket;
    socket = null;
    if (previousSocket) previousSocket.close();
    if (!reconnecting) {
      clearReconnectResetTimer();
      reconnectAttempts = 0;
    }
    const generation = socketGeneration + 1;
    socketGeneration = generation;
    connection = 'connecting';
    clearConnectionError();
    const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
    const runtimeKey = apiKey.trim();
    const bootstrapAgentMode = normalizeAgentMode(options.agentMode || defaultAgentMode);
    const params = new URLSearchParams({
      'rvt-method': 'getOrCreate',
      'rvt-key': threadId,
      'rvt-skip-ready-wait': 'true'
    });
    if (options.bootstrapExecutor) {
      params.set('rvt-input', encodeGatewayInput({
        threadId,
        agentMode: bootstrapAgentMode,
        executorType: 'local-client'
      }));
    }
    if (runtimeKey) params.set('auth_token', runtimeKey);
    const url = `${scheme}://${location.host}/gateway/threadActor/?${params.toString()}`;
    const nextSocket = new WebSocket(url, ['rivet', 'rivet_encoding.4', 'rivet_skip_ready_wait']);
    socket = nextSocket;
    const activeSocket = () => socket === nextSocket && generation === socketGeneration && selectedThreadId === threadId;
    nextSocket.addEventListener('open', () => {
      if (!activeSocket()) return;
      scheduleReconnectAttemptsReset();
      connection = 'connected';
      clearConnectionError();
      startPingTimer(nextSocket, threadId, version, options, generation);
      sendFrame({ type: 'client_resume', version });
      if (options.bootstrapExecutor) {
        const bootstrapReasoningEffort = normalizeReasoningEffortForMode(bootstrapAgentMode, options.reasoningEffort || '');
        sendThreadModeFrames(bootstrapAgentMode, bootstrapReasoningEffort);
        sendFrame({
          type: 'client_update_thread_settings',
          settings: threadSettingsPayload(bootstrapAgentMode, bootstrapReasoningEffort)
        });
        sendFrame({ type: 'environment', env: options.environment ?? {} });
        sendFrame({
          type: 'client_spawn_executor',
          requestId: `spawn-${crypto.randomUUID()}`
        });
      }
    });
    nextSocket.addEventListener('close', () => {
      if (!activeSocket()) return;
      clearReconnectResetTimer();
      clearPingTimer();
      socket = null;
      connection = 'offline';
      scheduleReconnect(threadId, version, options, generation);
    });
    nextSocket.addEventListener('error', () => {
      if (!activeSocket()) return;
      connection = 'offline';
      scheduleReconnect(threadId, version, options, generation);
    });
    nextSocket.addEventListener('message', (event) => {
      if (!activeSocket()) return;
      lastServerFrameAt = Date.now();
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
    clearReconnectTimer();
    clearReconnectResetTimer();
    clearPingTimer();
    socketGeneration += 1;
    const closingSocket = socket;
    socket = null;
    if (closingSocket) closingSocket.close();
    connection = 'offline';
  }

  function sendFrame(payload: Incoming) {
    if (!socket || socket.readyState !== WebSocket.OPEN) return false;
    socket.send(JSON.stringify(payload));
    return true;
  }

  function composerHasDraft() {
    return composer.trim().length > 0 || composerAttachments.length > 0;
  }

  function canSubmitComposer() {
    return composerHasDraft() && canSendMessage() && !composerUploadActive && !(shouldQueueOutgoingMessage() && queueIsFull());
  }

  function shouldQueueOutgoingMessage() {
    if (!canSendMessage()) return false;
    if (agentState && agentState !== 'idle') return true;
    if (toolLeases.length > 0 || toolApprovals.length > 0 || compactionActive || retryNotice) return true;
    return Boolean(liveTranscriptVerb());
  }

  function queueIsFull() {
    return queuedMessages.length >= maxQueuedMessages;
  }

  function canInterruptActor() {
    if (!canSendMessage()) return false;
    const normalizedState = agentState.trim().toLowerCase();
    if (normalizedState && !['idle', 'error'].includes(normalizedState)) return true;
    return toolLeases.length > 0 || toolApprovals.length > 0 || compactionActive || Boolean(retryNotice) || Boolean(liveTranscriptVerb());
  }

  function canInterruptQueuedInference() {
    return queuedMessages.some((item) => !item.steer) && canInterruptActor();
  }

  function interruptActor() {
    if (!canInterruptActor()) return;
    retryNotice = '';
    sendFrame({ type: 'client_cancel' });
  }

  function previousThreadForReference(): ThreadSummary | null {
    const thread = detail;
    if (!thread || !canSendMessage() || thread.messages.length > 0 || composer.trim().length > 0) return null;

    const currentIndex = threads.findIndex((item) => item.id === thread.id);
    const usable = (item: ThreadSummary) => item.id !== thread.id && !item.archived;
    if (currentIndex >= 0) {
      const nextOlder = threads.slice(currentIndex + 1).find(usable);
      if (nextOlder) return nextOlder;
    }
    return threads.find(usable) ?? null;
  }

  function acceptPreviousThreadHint() {
    const previous = previousThreadHint;
    if (!previous) return;
    composer = `following: @${previous.id} `;
    queueMicrotask(() => composerTextarea?.focus());
  }

  function threadMentionOptions() {
    const activeThreadId = detail?.id || selectedThreadId;
    const search = mentionSearch.trim().toLowerCase();
    const options = threads.filter((thread) => {
      if (thread.id === activeThreadId || thread.archived) return false;
      if (!search) return true;
      const haystack = `${thread.title} ${thread.id} ${thread.repo} ${thread.branch} ${thread.preview}`.toLowerCase();
      return haystack.includes(search);
    });
    return options.slice(0, 12);
  }

  function openThreadMentionPicker(start: number, end: number) {
    mentionStart = start;
    mentionEnd = end;
    mentionSearch = '';
    mentionActiveIndex = 0;
    mentionPickerOpen = true;
    if (threads.length === 0 && !loadingThreads) void refreshThreads(selectedThreadId);
    queueMicrotask(() => mentionSearchInput?.focus());
  }

  function closeThreadMentionPicker() {
    mentionPickerOpen = false;
    mentionSearch = '';
    mentionStart = null;
    mentionEnd = null;
    mentionActiveIndex = 0;
  }

  function mentionTriggerStillExists() {
    if (mentionStart === null) return false;
    return composer.slice(mentionStart, mentionStart + 2) === '@@';
  }

  function detectThreadMentionTrigger() {
    if (mentionPickerOpen) {
      if (!mentionTriggerStillExists()) closeThreadMentionPicker();
      return;
    }
    const cursor = composerTextarea?.selectionStart ?? composer.length;
    if (cursor < 2) return;
    if (composer.slice(cursor - 2, cursor) === '@@') openThreadMentionPicker(cursor - 2, cursor);
  }

  function handleComposerKeydown(event: KeyboardEvent) {
    if (mentionPickerOpen) {
      if (event.key === 'Escape') {
        event.preventDefault();
        closeThreadMentionPicker();
        queueMicrotask(() => composerTextarea?.focus());
      }
      return;
    }

    if (event.key === '@') {
      const cursor = composerTextarea?.selectionStart ?? composer.length;
      const selectionEnd = composerTextarea?.selectionEnd ?? cursor;
      if (cursor === selectionEnd && cursor > 0 && composer[cursor - 1] === '@') {
        event.preventDefault();
        const start = cursor - 1;
        composer = `${composer.slice(0, start)}@@${composer.slice(selectionEnd)}`;
        openThreadMentionPicker(start, start + 2);
      }
      return;
    }

    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      if (previousThreadHint && composer.trim().length === 0) {
        acceptPreviousThreadHint();
        return;
      }
      if (composer.trim().length === 0 && composerAttachments.length === 0 && canInterruptQueuedInference()) {
        steerNextQueuedMessage();
        return;
      }
      void sendMessage();
    }
  }

  function handleMentionPickerKeydown(event: KeyboardEvent) {
    const count = mentionThreadOptions.length;
    if ((event.key === 'Backspace' || event.key === 'Delete') && mentionSearch.length === 0 && mentionTriggerStillExists()) {
      event.preventDefault();
      const start = mentionStart ?? 0;
      const end = mentionEnd ?? start + 2;
      composer = `${composer.slice(0, start)}${composer.slice(end)}`;
      closeThreadMentionPicker();
      queueMicrotask(() => {
        composerTextarea?.focus();
        composerTextarea?.setSelectionRange(start, start);
      });
      return;
    }
    if (event.key === 'Escape') {
      event.preventDefault();
      closeThreadMentionPicker();
      queueMicrotask(() => composerTextarea?.focus());
      return;
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      if (count > 0) mentionActiveIndex = (mentionActiveIndex + 1) % count;
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      if (count > 0) mentionActiveIndex = (mentionActiveIndex - 1 + count) % count;
      return;
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      const thread = mentionThreadOptions[Math.min(mentionActiveIndex, Math.max(count - 1, 0))];
      if (thread) insertThreadMention(thread);
    }
  }

  function insertThreadMention(thread: ThreadSummary) {
    const cursor = composerTextarea?.selectionStart ?? composer.length;
    const start = mentionStart ?? composer.slice(0, cursor).lastIndexOf('@@');
    if (start < 0) {
      composer = `${composer}@${thread.id} `;
      closeThreadMentionPicker();
      queueMicrotask(() => composerTextarea?.focus());
      return;
    }
    const end = mentionEnd ?? cursor;
    const suffix = composer.slice(end);
    const mention = `@${thread.id}${suffix.length === 0 || !/^\s/.test(suffix) ? ' ' : ''}`;
    composer = `${composer.slice(0, start)}${mention}${suffix}`;
    const nextCursor = start + mention.length;
    closeThreadMentionPicker();
    queueMicrotask(() => {
      composerTextarea?.focus();
      composerTextarea?.setSelectionRange(nextCursor, nextCursor);
    });
  }

  function mediaTypeForImage(file: File) {
    const type = file.type.trim().toLowerCase();
    if (composerImageMediaTypes.has(type)) return type;
    const ext = file.name.toLowerCase().split('.').pop() ?? '';
    const types: Record<string, string> = {
      gif: 'image/gif',
      jpeg: 'image/jpeg',
      jpg: 'image/jpeg',
      png: 'image/png',
      webp: 'image/webp'
    };
    return types[ext] ?? '';
  }

  function addComposerFiles(files: FileList | File[] | null | undefined) {
    const incoming = Array.from(files ?? []);
    if (incoming.length === 0) return;
    const next: ComposerAttachment[] = [];
    let rejected = 0;
    for (const file of incoming) {
      const mediaType = mediaTypeForImage(file);
      if (!mediaType) {
        rejected++;
        continue;
      }
      if (file.size > maxComposerImageBytes) {
        rejected++;
        continue;
      }
      if (composerAttachments.length + next.length >= maxComposerImages) {
        rejected++;
        continue;
      }
      next.push({
        id: crypto.randomUUID(),
        file,
        previewUrl: URL.createObjectURL(file),
        name: file.name || 'image',
        mediaType,
        size: file.size
      });
    }
    if (next.length > 0) {
      composerAttachments = [...composerAttachments, ...next];
      lastError = '';
    }
    if (rejected > 0) {
      lastError = `Skipped ${rejected} file${rejected === 1 ? '' : 's'}; attach png, jpg, gif, or webp images under ${formatBytes(maxComposerImageEncodedBytes)} encoded.`;
    }
  }

  function removeComposerAttachment(id: string) {
    const target = composerAttachments.find((attachment) => attachment.id === id);
    if (target) URL.revokeObjectURL(target.previewUrl);
    composerAttachments = composerAttachments.filter((attachment) => attachment.id !== id);
  }

  function clearComposerAttachments() {
    for (const attachment of composerAttachments) {
      URL.revokeObjectURL(attachment.previewUrl);
    }
    composerAttachments = [];
  }

  function handleAttachmentInput(event: Event) {
    const input = event.currentTarget as HTMLInputElement;
    addComposerFiles(input.files);
    input.value = '';
  }

  function handleComposerPaste(event: ClipboardEvent) {
    const files = Array.from(event.clipboardData?.files ?? []).filter((file) => Boolean(mediaTypeForImage(file)));
    if (files.length === 0) return;
    event.preventDefault();
    addComposerFiles(files);
  }

  function handleComposerDrop(event: DragEvent) {
    event.preventDefault();
    const files = Array.from(event.dataTransfer?.files ?? []).filter((file) => Boolean(mediaTypeForImage(file)));
    if (files.length > 0) addComposerFiles(files);
  }

  function fileToBase64(file: File) {
    return new Promise<string>((resolve, reject) => {
      const reader = new FileReader();
      reader.addEventListener('load', () => {
        const result = typeof reader.result === 'string' ? reader.result : '';
        const [, encoded = result] = result.split(',', 2);
        resolve(encoded);
      });
      reader.addEventListener('error', () => reject(reader.error ?? new Error('Failed to read image')));
      reader.readAsDataURL(file);
    });
  }

  async function uploadComposerAttachment(attachment: ComposerAttachment): Promise<ContentBlock> {
    const data = await fileToBase64(attachment.file);
    if (data.length > maxComposerImageEncodedBytes) {
      throw new Error(`Image upload failed for ${attachment.name}: image exceeds ${formatBytes(maxComposerImageEncodedBytes)} encoded.`);
    }
    const response = await fetch('/api/attachments', {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({ data, mediaType: attachment.mediaType, name: attachment.name })
    });
    if (!response.ok) {
      throw new Error(`Image upload failed for ${attachment.name}`);
    }
    const uploaded = asRecord(await response.json().catch(() => ({})));
    const attachmentUrl = stringFrom(uploaded.url);
    return {
      type: 'image',
      name: attachment.name,
      filename: attachment.name,
      mediaType: attachment.mediaType,
      media_type: attachment.mediaType,
      sourcePath: attachment.name || attachmentUrl || 'image',
      source: {
        type: 'base64',
        mediaType: attachment.mediaType,
        media_type: attachment.mediaType,
        data
      },
      attachmentUrl
    };
  }

  function formatBytes(size: number) {
    if (size >= 1024 * 1024) return `${Math.round((size / (1024 * 1024)) * 10) / 10} MB`;
    if (size >= 1024) return `${Math.round(size / 1024)} KB`;
    return `${size} B`;
  }

  function randomBase62(length: number) {
    const alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
    const bytes = new Uint8Array(length);
    crypto.getRandomValues(bytes);
    return Array.from(bytes, (byte) => alphabet[byte % alphabet.length]).join('');
  }

  function newMessageId() {
    return `M-${randomBase62(22)}`;
  }

  async function sendMessage() {
    const text = composer.trim();
    const attachments = composerAttachments;
    const thread = detail;
    if ((!text && attachments.length === 0) || !thread || !canSendMessage() || composerUploadActive) return;
    if (shouldQueueOutgoingMessage() && queueIsFull()) {
      lastError = `Queue is full. Amp keeps up to ${maxQueuedMessages} queued messages.`;
      return;
    }
    composerUploadActive = true;
    lastError = '';
    let imageBlocks: ContentBlock[] = [];
    try {
      imageBlocks = await Promise.all(attachments.map(uploadComposerAttachment));
      if (!detail || detail.id !== thread.id) {
        throw new Error('Thread changed before the image upload finished.');
      }
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      composerUploadActive = false;
      return;
    }
    const messageId = newMessageId();
    const content: ContentBlock[] = [
      ...(text ? [{ type: 'text', text }] : []),
      ...imageBlocks
    ];
    const agentMode = currentComposerMode();
    const reasoningEffort = currentComposerReasoningEffort();
    const shouldQueue = shouldQueueOutgoingMessage();
    if (shouldQueue && queueIsFull()) {
      lastError = `Queue is full. Amp keeps up to ${maxQueuedMessages} queued messages.`;
      composerUploadActive = false;
      return;
    }
    const message: NeoMessage = {
      threadId: thread.id,
      messageId,
      role: 'user',
      content,
      agentMode,
      reasoningEffort
    };
    if (!shouldQueue) {
      planTranscriptScroll({ forceFollow: true, markNewActivity: false });
      detail = { ...thread, messages: [...thread.messages, message] };
    }
    composer = '';
    clearComposerAttachments();
    composerUploadActive = false;
    if (shouldQueue) {
      sendFrame({
        type: 'user:message-queue:enqueue',
        message: {
          content,
          agentMode,
          reasoningEffort
        }
      });
    } else {
      sendFrame({
        type: 'client_append_user_msg',
        messageId,
        agentMode,
        reasoningEffort,
        content
      });
    }
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
    if (type === 'thread_truncated') {
      truncateMessagesFromEvent(message);
      return;
    }
    if (type === 'delta') {
      applyDelta(message);
      return;
    }
    if (type === 'agent_state') {
      agentState = String(message.state ?? 'idle');
      if (detail && (message.agentMode != null || message.reasoningEffort != null)) {
        const nextMode = normalizeAgentMode(stringFrom(message.agentMode) || detail.agentMode);
        const nextEffort = normalizeReasoningEffortForMode(
          nextMode,
          stringFrom(message.reasoningEffort) || detail.reasoningEffort || ''
        );
        if (nextMode !== detail.agentMode || nextEffort !== detail.reasoningEffort) {
          applyLocalThreadSettings(nextMode, nextEffort);
        }
      }
      return;
    }
    if (type === 'thread_settings') {
      runtimeSettings = asRecord(message.settings);
      if (detail) {
        const nextMode = stringFrom(runtimeSettings.agentMode) || detail.agentMode;
        const nextEffort = normalizeReasoningEffortForMode(
          nextMode,
          stringFrom(runtimeSettings['reasoning.effort'] ?? runtimeSettings.reasoningEffort) || detail.reasoningEffort || ''
        );
        applyLocalThreadSettings(nextMode, nextEffort);
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
      queuedMessages = [...queuedMessages.filter((queued) => !sameQueuedMessage(queued, item)), item];
      queuedCount = queuedMessages.length;
      return;
    }
    if (type === 'queued_message_removed' || type === 'queued_message_dequeued') {
      const id = stringFrom(message.queuedMessageId ?? message.messageId);
      queuedMessages = queuedMessages.filter((queued) => !queuedMessageMatches(queued, id));
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
    if (type === 'tool_progress') {
      applyToolProgress(message);
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
    if (type === 'observers') {
      if (typeof message.hasExecutor === 'boolean') {
        executorConnected = message.hasExecutor;
      }
      return;
    }
    if (type === 'executor_status') {
      const status = executorStatusFromAny(message);
      executorStatuses = [status, ...executorStatuses.filter((item) => item.id !== status.id)].slice(0, 5);
      return;
    }
    if (
      type === 'executor_filesystem_read_directory' ||
      type === 'executor_filesystem_read_file' ||
      type === 'client_filesystem_read_directory' ||
      type === 'client_filesystem_read_file'
    ) {
      const side = type.startsWith('client_') ? 'client' : 'executor';
      pushRuntimeEvent(
        `${side} ${type.includes('read_directory') ? 'fs dir' : 'fs file'}`,
        filesystemRequestDetail(message)
      );
      return;
    }
    if (
      type === 'client_filesystem_read_directory_result' ||
      type === 'client_filesystem_read_file_result' ||
      type === 'executor_filesystem_read_directory_result' ||
      type === 'executor_filesystem_read_file_result'
    ) {
      const side = type.startsWith('client_') ? 'client' : 'executor';
      pushRuntimeEvent(
        `${side} ${type.includes('read_directory') ? 'fs dir result' : 'fs file result'}`,
        filesystemResultDetail(message)
      );
      return;
    }
    if (type === 'executor_git_command' || type === 'client_git_command') {
      pushRuntimeEvent(`${type.startsWith('client_') ? 'client' : 'executor'} git`, gitRequestDetail(message));
      return;
    }
    if (type === 'client_git_command_result' || type === 'executor_git_command_result') {
      pushRuntimeEvent(`${type.startsWith('client_') ? 'client' : 'executor'} git result`, gitResultDetail(message));
      return;
    }
    if (type === 'executor_error') {
      activeError = { message: message.message, code: message.code };
      return;
    }
    if (type === 'executor_workspace_maybe_changed') {
      pushRuntimeEvent('workspace', runtimeEventDetail(message, ['toolName', 'toolCallId', 'reason', 'message']));
      return;
    }
    if (type === 'executor_tool_approval_response') {
      const id = stringFrom(message.toolCallId ?? message.toolUseId ?? message.id);
      if (id) toolApprovals = toolApprovals.filter((approval) => approval.toolCallId !== id);
      pushRuntimeEvent('approval', runtimeEventDetail(message, ['response', 'status', 'toolName', 'toolCallId']));
      return;
    }
    if (type === 'plugin_message') {
      pushRuntimeEvent('plugin', pluginMessageDetail(message.message ?? message));
      return;
    }
    if (type === 'trace:start') {
      applyTraceStart(message);
      return;
    }
    if (type === 'trace:event') {
      applyTraceEvent(message);
      return;
    }
    if (type === 'trace:attributes') {
      applyTraceAttributes(message);
      return;
    }
    if (type === 'trace:end') {
      applyTraceEnd(message);
      return;
    }
    if (type === 'error') {
      activeError = { message: message.message, code: message.code };
      return;
    }
    if (type === 'edit_rejected') {
      activeError = { message: message.message, code: 'EDIT_REJECTED', editId: message.editId };
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
    planTranscriptScroll();
    const index = detail.messages.findIndex((item) => item.messageId === message.messageId);
    if (index === -1) {
      detail = { ...detail, messages: [...detail.messages, message] };
      return;
    }
    const next = [...detail.messages];
    const usage = mergeUsage(next[index].usage, message.usage);
    const state = Object.keys(message.state ?? {}).length > 0 ? message.state : next[index].state;
    next[index] = replace ? { ...message, state, usage } : { ...next[index], ...message, state, usage };
    detail = { ...detail, messages: next };
  }

  function truncateMessagesFromEvent(message: Incoming) {
    if (!detail) return;
    planTranscriptScroll({ markNewActivity: false });
    const truncateFromMessage = stringFrom(message.truncateFromMessage);
    let index = -1;
    if (truncateFromMessage) {
      index = detail.messages.findIndex((item) => item.messageId === truncateFromMessage);
    }
    if (index < 0) {
      const fromIndex = Number(message.fromIndex);
      if (Number.isFinite(fromIndex)) index = Math.max(0, Math.trunc(fromIndex));
    }
    if (index < 0) return;
    detail = { ...detail, messages: detail.messages.slice(0, index) };
  }

  function applyDelta(delta: Incoming) {
    if (!detail) return;
    const messageId = String(delta.messageId ?? '');
    if (!messageId) return;
    planTranscriptScroll();
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

  function applyToolProgress(event: Incoming) {
    if (!detail) return;
    const toolCallId = stringFrom(event.toolCallId ?? event.toolUseId ?? event.id);
    if (!toolCallId) return;
    planTranscriptScroll();
    const progressMessageId = toolProgressMessageId(toolCallId);
    const nextMessages = [...detail.messages];
    let messageIndex = -1;
    let blockIndex = -1;
    let existingBlock: ContentBlock | undefined;

    for (let i = 0; i < nextMessages.length; i += 1) {
      const message = nextMessages[i];
      if (message.role !== 'user') continue;
      const index = message.content.findIndex((block) => block.type === 'tool_result' && toolResultUseID(block) === toolCallId);
      if (index >= 0) {
        messageIndex = i;
        blockIndex = index;
        existingBlock = message.content[index];
        break;
      }
    }

    const run = toolProgressRun(event.progress, asRecord(existingBlock?.run));
    if (!run) return;
    const block: ContentBlock = { type: 'tool_result', toolUseID: toolCallId, run };
    if (existingBlock?.userInput !== undefined) block.userInput = existingBlock.userInput;

    if (messageIndex >= 0) {
      const current = nextMessages[messageIndex];
      const content = [...current.content];
      content[blockIndex] = block;
      nextMessages[messageIndex] = { ...current, content };
    } else {
      nextMessages.push({
        threadId: detail.id,
        messageId: progressMessageId,
        role: 'user',
        content: [block]
      });
    }

    detail = { ...detail, messages: nextMessages };
  }

  function toolProgressMessageId(toolCallId: string) {
    return `M-${toolCallId.replace(/^TU-/, '')}`;
  }

  function toolProgressRun(progress: unknown, existingRun: Record<string, unknown>): Record<string, unknown> | null {
    const decodedProgress = decodeRivetProgress(progress);
    const progressMap = asRecord(decodedProgress);
    const status = stringFrom(progressMap.status).trim().toLowerCase();
    if (status) {
      if (isTerminalToolStatus(status) || status === 'in-progress') {
        return { ...cloneRecord(progressMap), status };
      }
      return null;
    }

    const existingProgress = existingRun.progress;
    const merged = emptyToolProgress(decodedProgress)
      ? cloneProgressValue(existingProgress)
      : mergeToolProgress(existingProgress, decodedProgress);
    const run: Record<string, unknown> = { status: 'in-progress' };
    if (!emptyToolProgress(merged)) run.progress = merged;
    return run;
  }

  function decodeRivetProgress(progress: unknown): unknown {
    const record = asRecord(progress);
    const type = stringFrom(record.type);
    if (!type) return progress;
    if (type === 'snapshot' && 'value' in record) {
      return decodeRivetProgressValue(record.value);
    }
    if (type === 'delta' && Array.isArray(record.blocks)) {
      const text = rivetTextBlocks(record.blocks);
      if (text === undefined) return undefined;
      try {
        return JSON.parse(text);
      } catch {
        return text;
      }
    }
    return progress;
  }

  function decodeRivetProgressValue(value: unknown): unknown {
    return Array.isArray(value) ? rivetTextBlocks(value) : value;
  }

  function rivetTextBlocks(blocks: unknown[]) {
    const text = blocks.map((block) => {
      const record = asRecord(block);
      if (record.type !== 'text') return '';
      return stringFrom(record.text);
    }).join('');
    return text.length > 0 ? text : undefined;
  }

  function isTerminalToolStatus(status: string) {
    return ['done', 'error', 'cancelled', 'rejected-by-user'].includes(status.trim().toLowerCase());
  }

  function emptyToolProgress(value: unknown) {
    if (value === null || value === undefined) return true;
    if (Array.isArray(value)) return value.length === 0;
    if (typeof value === 'object') return Object.keys(value).length === 0;
    return false;
  }

  function mergeToolProgress(existing: unknown, next: unknown): unknown {
    if (emptyToolProgress(next)) return cloneProgressValue(existing);
    if (emptyToolProgress(existing)) return cloneProgressValue(next);
    if (Array.isArray(existing) && Array.isArray(next)) {
      return [...cloneProgressValue(existing) as unknown[], ...cloneProgressValue(next) as unknown[]];
    }
    if (existing && next && typeof existing === 'object' && typeof next === 'object' && !Array.isArray(existing) && !Array.isArray(next)) {
      const out: Record<string, unknown> = { ...asRecord(cloneProgressValue(existing)) };
      for (const [key, value] of Object.entries(asRecord(next))) {
        out[key] = mergeToolProgress(out[key], value);
      }
      return out;
    }
    return cloneProgressValue(next);
  }

  function cloneProgressValue(value: unknown): unknown {
    try {
      return structuredClone(value);
    } catch {
      if (Array.isArray(value)) return [...value];
      if (value && typeof value === 'object') return { ...asRecord(value) };
      return value;
    }
  }

  function mergeBlock(previous: ContentBlock | undefined, incoming: ContentBlock): ContentBlock {
    if (!previous) return { ...incoming };
    if (incoming.type === 'text') {
      return { ...previous, ...incoming, text: `${previous.text ?? ''}${incoming.text ?? ''}` };
    }
    if (incoming.type === 'thinking') {
      return { ...previous, ...incoming, thinking: `${previous.thinking ?? ''}${incoming.thinking ?? ''}` };
    }
    if (incoming.type === 'tool_use') {
      return mergeToolUseBlock(previous, incoming);
    }
    return { ...previous, ...incoming };
  }

  function mergeToolUseBlock(previous: ContentBlock, incoming: ContentBlock): ContentBlock {
    const merged: ContentBlock = { ...previous, ...incoming };
    const delta = stringFrom(asRecord(incoming.inputPartialJSONDelta).json);
    if (delta) {
      const previousPartial = stringFrom(asRecord(previous.inputPartialJSON).json);
      const incomingPartial = stringFrom(asRecord(incoming.inputPartialJSON).json);
      const partial = `${previousPartial || incomingPartial}${delta}`;
      merged.inputPartialJSON = { json: partial };
      merged.complete = false;
      const parsed = parsePartialJSONObject(partial);
      if (Object.keys(parsed).length > 0) {
        merged.input = parsed;
        merged.inputIncomplete = parsed;
      }
    }
    if (merged.complete === true) {
      delete merged.inputPartialJSON;
      delete merged.inputPartialJSONDelta;
      delete merged.inputIncomplete;
    }
    return merged;
  }

  function parsePartialJSONObject(value: string): Record<string, unknown> {
    const trimmed = value.trim();
    if (!trimmed) return {};
    try {
      const parsed = JSON.parse(trimmed);
      return asRecord(parsed);
    } catch {
      const parsed: Record<string, unknown> = {};
      const matcher = /"((?:\\.|[^"\\])+)":\s*("(?:\\.|[^"\\])*"|true|false|null|-?\d+(?:\.\d+)?)/g;
      let match: RegExpExecArray | null;
      while ((match = matcher.exec(trimmed))) {
        try {
          parsed[JSON.parse(`"${match[1]}"`)] = JSON.parse(match[2]);
        } catch {
          continue;
        }
      }
      return parsed;
    }
  }

  function queuedMessageFromAny(raw: unknown): QueuedMessage {
    const item = asRecord(raw);
    const queued = asRecord(item.queuedMessage);
    const source = Object.keys(queued).length ? queued : item;
    const messageId = stringFrom(source.protocolMessageID ?? source.protocolMessageId ?? source.messageId ?? item.protocolMessageID ?? item.protocolMessageId ?? item.messageId);
    const id = stringFrom(item.id ?? item.queuedMessageId ?? messageId);
    const content = Array.isArray(source.content) ? source.content.map((part) => asRecord(part) as ContentBlock) : [];
    return {
      id,
      messageId,
      content,
      preview: contentPreview(content.length > 0 ? content : source.content),
      steer: Boolean(item.steer ?? source.steer)
    };
  }

  function queuedMessageKey(queued: QueuedMessage) {
    return queued.messageId || queued.id;
  }

  function queuedMessageMatches(queued: QueuedMessage, id: string) {
    return Boolean(id) && (queued.id === id || queued.messageId === id);
  }

  function sameQueuedMessage(left: QueuedMessage, right: QueuedMessage) {
    return queuedMessageMatches(left, right.id) || queuedMessageMatches(left, right.messageId);
  }

  function queuedMessageComposerText(queued: QueuedMessage) {
    const text = queued.content
      .map((block) => block.type === 'text' ? block.text ?? '' : '')
      .filter(Boolean)
      .join('');
    return (text || queued.preview).trim();
  }

  function composerAttachmentFromImageBlock(block: ContentBlock): ComposerAttachment | null {
    const source = imageBlockSrc(block);
    const match = source.match(/^data:([^;,]+);base64,(.+)$/);
    if (!match || typeof atob === 'undefined') return null;
    try {
      const bytes = Uint8Array.from(atob(match[2]), (char) => char.charCodeAt(0));
      const mediaType = match[1] || 'image/png';
      if (!composerImageMediaTypes.has(mediaType) || match[2].length > maxComposerImageEncodedBytes) return null;
      const name = imageBlockName(block);
      const file = new File([bytes], name, { type: mediaType });
      return {
        id: crypto.randomUUID(),
        file,
        previewUrl: URL.createObjectURL(file),
        name,
        mediaType,
        size: file.size
      };
    } catch {
      return null;
    }
  }

  function queuedMessageComposerAttachments() {
    const attachments: ComposerAttachment[] = [];
    for (const queued of queuedMessages) {
      for (const block of queued.content) {
        if (!isImageBlock(block) || attachments.length >= maxComposerImages) continue;
        const attachment = composerAttachmentFromImageBlock(block);
        if (attachment) attachments.push(attachment);
      }
    }
    return attachments;
  }

  function removeQueuedMessage(queued: QueuedMessage) {
    const key = queuedMessageKey(queued);
    if (!key) return;
    sendFrame({ type: 'client_remove_queued_msg', queuedMessageId: key });
    queuedMessages = queuedMessages.filter((item) => !queuedMessageMatches(item, key));
    queuedCount = queuedMessages.length;
  }

  function discardQueuedMessages() {
    if (queuedMessages.length === 0) return;
    sendFrame({ type: 'user:message-queue:discard' });
    queuedMessages = [];
    queuedCount = 0;
  }

  function dequeueQueuedMessagesToComposer() {
    if (queuedMessages.length === 0) return;
    const text = queuedMessages.map(queuedMessageComposerText).filter(Boolean).join('\n\n');
    const attachments = queuedMessageComposerAttachments();
    if (text) {
      composer = composer.trim() ? `${composer.trimEnd()}\n\n${text}` : text;
    }
    if (attachments.length > 0) {
      const slots = Math.max(0, maxComposerImages - composerAttachments.length);
      composerAttachments = [...composerAttachments, ...attachments.slice(0, slots)];
      for (const extra of attachments.slice(slots)) {
        URL.revokeObjectURL(extra.previewUrl);
      }
    }
    for (const queued of queuedMessages) {
      sendFrame({ type: 'client_remove_queued_msg', queuedMessageId: queuedMessageKey(queued) });
    }
    queuedMessages = [];
    queuedCount = 0;
    queueMicrotask(() => composerTextarea?.focus());
  }

  function steerQueuedMessage(queued: QueuedMessage) {
    if (queued.steer) return;
    const key = queuedMessageKey(queued);
    if (!key) return;
    sendFrame({ type: 'client_steer_queued_msg', queuedMessageId: key });
    queuedMessages = queuedMessages.map((item) => item === queued ? { ...item, steer: true } : item);
  }

  function canSteerQueuedMessages() {
    return canSendMessage() && queuedMessages.some((item) => !item.steer);
  }

  function steerNextQueuedMessage() {
    const queued = queuedMessages.find((item) => !item.steer) || queuedMessages[0];
    if (!queued) return;
    steerQueuedMessage(queued);
  }

  function steerQueuedMessages() {
    if (!canSteerQueuedMessages()) return;
    for (const queued of queuedMessages) steerQueuedMessage(queued);
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

  function pushRuntimeEvent(label: string, detail: string) {
    const id = `${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    runtimeEvents = [{ id, label, detail: detail || label }, ...runtimeEvents].slice(0, 8);
  }

  function applyTraceStart(message: Incoming) {
    const span = asRecord(message.span);
    const id = stringFrom(span.id);
    if (!id) return;
    const existing = runtimeTraces.find((trace) => trace.id === id);
    const events = Array.isArray(span.events) ? span.events : [];
    const latestEvent = existing?.latestEvent || traceEventLabel(events[events.length - 1]);
    const next: RuntimeTrace = {
      id,
      name: traceNameFromSpan(span, id),
      status: stringFrom(span.endTime) ? 'done' : existing?.status || 'running',
      detail: '',
      startedAt: stringFrom(span.startTime) || existing?.startedAt || '',
      endedAt: stringFrom(span.endTime) || existing?.endedAt || '',
      eventCount: Math.max(existing?.eventCount ?? 0, events.length),
      latestEvent,
      attributes: { ...(existing?.attributes ?? {}), ...asRecord(span.attributes) }
    };
    next.detail = runtimeTraceDetail(next);
    upsertRuntimeTrace(next);
  }

  function applyTraceEvent(message: Incoming) {
    const id = traceIdFromMessage(message);
    if (!id) return;
    const existing = runtimeTraces.find((trace) => trace.id === id);
    if (!existing) return;
    const next = {
      ...existing,
      eventCount: existing.eventCount + 1,
      latestEvent: traceEventLabel(message.event ?? message.value)
    };
    next.detail = runtimeTraceDetail(next);
    upsertRuntimeTrace(next);
  }

  function applyTraceAttributes(message: Incoming) {
    const id = traceIdFromMessage(message);
    if (!id) return;
    const existing = runtimeTraces.find((trace) => trace.id === id);
    if (!existing) return;
    const next = {
      ...existing,
      attributes: { ...existing.attributes, ...asRecord(message.attributes) }
    };
    next.detail = runtimeTraceDetail(next);
    upsertRuntimeTrace(next);
  }

  function applyTraceEnd(message: Incoming) {
    const span = asRecord(message.span);
    const id = traceIdFromMessage(message);
    if (!id) return;
    const existing = runtimeTraces.find((trace) => trace.id === id);
    if (!existing) return;
    const next = {
      ...existing,
      name: traceNameFromSpan(span, existing.name),
      status: 'done',
      endedAt: stringFrom(span.endTime) || existing.endedAt
    };
    next.detail = runtimeTraceDetail(next);
    upsertRuntimeTrace(next);
  }

  function upsertRuntimeTrace(trace: RuntimeTrace) {
    runtimeTraces = [trace, ...runtimeTraces.filter((item) => item.id !== trace.id)].slice(0, 6);
  }

  function traceIdFromMessage(message: Incoming) {
    if (typeof message.span === 'string') return message.span;
    const span = asRecord(message.span);
    return stringFrom(span.id ?? message.spanID ?? message.spanId ?? message.id);
  }

  function traceNameFromSpan(span: Record<string, unknown>, fallback: string) {
    return firstString(span.name, span.label, span.operation, span.type, fallback);
  }

  function traceEventLabel(raw: unknown) {
    const item = asRecord(raw);
    return firstString(raw, item.name, item.type, item.label, item.message, objectSummary(item));
  }

  function runtimeTraceDetail(trace: RuntimeTrace) {
    const parts = [
      trace.latestEvent,
      trace.eventCount > 0 ? `${trace.eventCount} event${trace.eventCount === 1 ? '' : 's'}` : '',
      objectSummary(trace.attributes),
      traceDurationLabel(trace.startedAt, trace.endedAt)
    ].filter(Boolean);
    return parts.slice(0, 3).join(' · ');
  }

  function traceDurationLabel(startedAt: string, endedAt: string) {
    if (!startedAt) return '';
    const started = Date.parse(startedAt);
    if (!Number.isFinite(started)) return '';
    const ended = endedAt ? Date.parse(endedAt) : Date.now();
    if (!Number.isFinite(ended) || ended < started) return '';
    const seconds = Math.max(0, Math.round((ended - started) / 1000));
    if (seconds < 1) return '<1s';
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    const remainder = seconds % 60;
    return remainder > 0 ? `${minutes}m ${remainder}s` : `${minutes}m`;
  }

  function runtimeEventDetail(message: Incoming, fields: string[]) {
    const parts = fields
      .map((field) => stringFrom(message[field]))
      .filter(Boolean);
    return parts.join(' · ') || objectSummary(asRecord(message));
  }

  function runtimeEventRequestId(message: Incoming) {
    return stringFrom(message.requestId ?? message.requestID ?? message.id);
  }

  function filesystemRequestDetail(message: Incoming) {
    const path = filePathFromURI(stringFrom(message.path ?? message.uri ?? message.url));
    const requestId = runtimeEventRequestId(message);
    return [path, requestId].filter(Boolean).join(' · ') || objectSummary(asRecord(message));
  }

  function filesystemResultDetail(message: Incoming) {
    const requestId = runtimeEventRequestId(message);
    const content = stringFrom(message.content ?? message.text ?? message.error);
    const length = content ? `${content.length} chars` : '';
    return [requestId, length].filter(Boolean).join(' · ') || objectSummary(asRecord(message));
  }

  function gitRequestDetail(message: Incoming) {
    const args = Array.isArray(message.args) ? message.args.map(stringFrom).filter(Boolean).join(' ') : '';
    const operation = asRecord(message.operation);
    const op = stringFrom(operation.type ?? message.command ?? message.cmd);
    const requestId = runtimeEventRequestId(message);
    return [args || op, requestId].filter(Boolean).join(' · ') || objectSummary(asRecord(message));
  }

  function gitResultDetail(message: Incoming) {
    const requestId = runtimeEventRequestId(message);
    const exitCode = stringFrom(message.exitCode ?? message.exit_code);
    const stdout = stringFrom(message.stdout).trim();
    const stderr = stringFrom(message.stderr).trim();
    const preview = stdout || stderr || stringFrom(message.error);
    const status = exitCode ? `exit ${exitCode}` : valueLabel(message.ok);
    return [requestId, status, preview.slice(0, 120)].filter(Boolean).join(' · ') || objectSummary(asRecord(message));
  }

  function pluginMessageDetail(raw: unknown) {
    const item = asRecord(raw);
    return firstString(item.message, item.text, item.title, item.name, raw, objectSummary(item));
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

  function isVisibleRailRelationship(_relationship: Relationship) {
    // Amp keeps relationship deltas out of the thread-info rail.
    return false;
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
      input: { denyFeedback: 'Denied from the browser' }
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
    const candidates = [
      stringFrom(environment.workingDirectory) ||
        stringFrom(environment.working_directory) ||
        stringFrom(environment.workspaceRoot) ||
        stringFrom(environment.cwd),
      filePathFromURI(stringFrom(first.uri)) ||
      stringFrom(first.path) ||
      stringFrom(first.root)
    ];
    return candidates.map(filePathFromURI).find(Boolean) || '';
  }

  function filePathFromURI(value: string) {
    if (!value) return '';
    if (!value.startsWith('file://')) return value;
    try {
      const url = new URL(value);
      if (url.protocol === 'file:') return decodeURIComponent(url.pathname);
    } catch {
      try {
        return decodeURIComponent(value.replace(/^file:\/\/(?:localhost)?/, ''));
      } catch {
        return value.replace(/^file:\/\/(?:localhost)?/, '');
      }
    }
    return value.replace(/^file:\/\//, '');
  }

  function toHomeRelativePath(path: string) {
    const normalized = path.replace(/\\/g, '/');
    const match = normalized.match(/^\/(?:Users|home)\/[^/]+(\/.*)?$/);
    if (!match) return normalized;
    return `~${match[1] || ''}`;
  }

  function shortenWorkspacePath(path: string) {
    if (!path.includes('/')) return path;
    const rooted = path.startsWith('/') ? '/' : '';
    const parts = path.split('/').filter(Boolean);
    if (parts.length <= 5) return path;
    return `${rooted}${parts.slice(0, 2).join('/')}/…/${parts.slice(-2).join('/')}`;
  }

  function composerDirectoryLabel() {
    const path = runtimeWorkspacePath();
    const repo = repoFromEnv(environment) || detail?.repo || '';
    const branch = branchFromEnv(environment) || detail?.branch || '';
    const base = path ? shortenWorkspacePath(toHomeRelativePath(path)) : repo;
    if (!base && !branch) return '';
    if (branch) return `${base || 'workspace'} (${branch})`;
    return base;
  }

  function composerToolStatusDetail() {
    if (toolLeases.length > 0) {
      return toolLeases.map((lease) => lease.toolName).filter(Boolean).slice(0, 3).join(', ');
    }
    if (inferenceTools?.tools.length) return inferenceTools.tools.slice(0, 3).join(', ');
    return '';
  }

  function binaryVerbForAgentState(state: string) {
    const normalized = state.trim().toLowerCase();
    const labels: Record<string, string> = {
      auto_compacting: 'Auto-compacting',
      compacting: 'Auto-compacting',
      sending: 'Sending',
      waiting_for_executor: 'Starting',
      starting: 'Starting',
      working: 'Waiting',
      thinking: 'Thinking',
      streaming: 'Streaming',
      running_tools: 'Running tools',
      tool_running: 'Running tools',
      awaiting_approval: 'Waiting for approval',
      error: 'Error'
    };
    return labels[normalized] || '';
  }

  function liveTranscriptVerb() {
    if (!detail) return '';
    const messages = detail.messages;
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      const message = messages[index];
      if (message.role !== 'assistant') continue;
      const state = message.state?.type ?? '';
      if (!['generating', 'streaming', 'start'].includes(state)) {
        const verb = binaryVerbForAgentState(state);
        if (verb) return verb;
      }
      if (message.state?.stopReason === 'tool_use') return 'Running tools';

      const runningTool = message.content.some((block) => {
        if (block.type !== 'tool_use') return false;
        return block.blockState !== 'complete' && block.blockState !== 'done';
      });
      if (runningTool) return 'Running tools';

      const streamingBlock = message.content.find((block) => block.blockState === 'streaming');
      if (streamingBlock?.type === 'thinking' || streamingBlock?.thinking) return 'Thinking';
      if (['generating', 'streaming', 'start'].includes(state)) return 'Streaming';
      break;
    }
    return '';
  }

  function composerStatusMain() {
    if (activeError) return 'Error';
    if (toolApprovals.length > 0) return 'Waiting for approval';
    if (compactionActive) return 'Auto-compacting';
    if (toolLeases.length > 0) return toolLeases.length > 1 ? `Running ${toolLeases.length} tools` : 'Running tools';
    if (retryNotice) return 'Retrying';
    const executorVerb = binaryVerbForAgentState(executorStatuses[0]?.status ?? '');
    if (executorVerb) return executorVerb;
    const verb = binaryVerbForAgentState(agentState);
    if (verb) return verb;
    const transcriptVerb = liveTranscriptVerb();
    if (transcriptVerb) return transcriptVerb;
    if (connection === 'connected' && executorConnected) return 'Ready';
    if (connection === 'connected') return 'Observer';
    if (connection === 'connecting') return 'Connecting…';
    return 'Offline';
  }

  function composerStatusParts() {
    return [composerToolStatusDetail(), composerDirectoryLabel()].filter(Boolean);
  }

  function canSendMessage() {
    return connection === 'connected' && executorConnected;
  }

  function normalizeAgentMode(mode: string) {
    const normalized = mode.trim().toLowerCase();
    return agentModeOptions.includes(normalized) ? normalized : defaultAgentMode;
  }

  function threadSettingsPayload(mode: string, effort: string) {
    const agentMode = normalizeAgentMode(mode);
    const reasoningEffort = normalizeReasoningEffortForMode(agentMode, effort);
    const settings: Record<string, unknown> = {};
    if (reasoningEffort) settings['reasoning.effort'] = reasoningEffort;
    return settings;
  }

  function sendThreadModeFrames(mode: string, effort: string) {
    const agentMode = normalizeAgentMode(mode);
    const reasoningEffort = normalizeReasoningEffortForMode(agentMode, effort);
    sendFrame({ type: 'agent-mode', mode: agentMode });
    if (reasoningEffort) {
      sendFrame({ type: 'reasoning-effort', effort: reasoningEffort });
    }
  }

  function encodeGatewayInput(value: Record<string, unknown>) {
    const bytes = new TextEncoder().encode(JSON.stringify(value));
    let binary = '';
    for (let index = 0; index < bytes.length; index += 0x8000) {
      binary += String.fromCharCode(...bytes.subarray(index, index + 0x8000));
    }
    return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }

  function reasoningEffortOptionsForMode(mode: string) {
    switch (normalizeAgentMode(mode)) {
      case 'smart':
        return ['high', 'xhigh', 'max'];
      case 'deep':
        return ['low', 'medium', 'xhigh'];
      default:
        return [];
    }
  }

  function defaultReasoningEffortForMode(mode: string) {
    switch (normalizeAgentMode(mode)) {
      case 'smart':
        return 'high';
      case 'rush':
        return 'none';
      case 'deep':
        return 'medium';
      case 'nostromo':
        return 'low';
      default:
        return '';
    }
  }

  function normalizeReasoningEffortForMode(mode: string, effort: string) {
    const normalized = effort.trim().toLowerCase();
    const options = reasoningEffortOptionsForMode(mode);
    if (options.includes(normalized)) return normalized;
    return defaultReasoningEffortForMode(mode);
  }

  function threadReasoningEffortFrom(raw: Record<string, unknown>, mode: string, messages: NeoMessage[] = []) {
    const settings = asRecord(raw.settings);
    const direct = stringFrom(raw.reasoningEffort ?? raw.reasoning_effort ?? settings['reasoning.effort'] ?? settings.reasoningEffort);
    if (direct) return normalizeReasoningEffortForMode(mode, direct);
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      const message = messages[index];
      if (message.role === 'user' && message.reasoningEffort) {
        return normalizeReasoningEffortForMode(mode, message.reasoningEffort);
      }
    }
    return defaultReasoningEffortForMode(mode);
  }

  function currentComposerMode() {
    return normalizeAgentMode(detail?.agentMode || selectedSummary?.agentMode || defaultAgentMode);
  }

  function currentComposerReasoningEffort() {
    return normalizeReasoningEffortForMode(
      currentComposerMode(),
      detail?.reasoningEffort || selectedSummary?.reasoningEffort || stringFrom(runtimeSettings['reasoning.effort'])
    );
  }

  function canEditThreadSettings() {
    return Boolean(detail && detail.messages.length === 0 && !loadingThread);
  }

  function applyLocalThreadSettings(mode: string, effort: string) {
    const agentMode = normalizeAgentMode(mode);
    const reasoningEffort = normalizeReasoningEffortForMode(agentMode, effort);
    const threadId = detail?.id || selectedThreadId;
    if (detail) {
      detail = { ...detail, agentMode, reasoningEffort };
    }
    if (threadId) {
      threads = threads.map((thread) => thread.id === threadId ? { ...thread, agentMode, reasoningEffort } : thread);
    }
    const nextSettings: Record<string, unknown> = { ...runtimeSettings, agentMode };
    if (reasoningEffort) {
      nextSettings['reasoning.effort'] = reasoningEffort;
    } else {
      delete nextSettings['reasoning.effort'];
    }
    runtimeSettings = nextSettings;
  }

  function updateThreadSettings(mode: string, effort: string) {
    if (!canEditThreadSettings()) return;
    const agentMode = normalizeAgentMode(mode);
    const reasoningEffort = normalizeReasoningEffortForMode(agentMode, effort);
    applyLocalThreadSettings(agentMode, reasoningEffort);
    sendThreadModeFrames(agentMode, reasoningEffort);
    sendFrame({ type: 'client_update_thread_settings', settings: threadSettingsPayload(agentMode, reasoningEffort) });
  }

  function toggleSettingsMenu(menu: 'mode' | 'effort') {
    settingsMenuOpen = settingsMenuOpen === menu ? null : menu;
  }

  function chooseAgentMode(mode: string) {
    updateThreadSettings(mode, defaultReasoningEffortForMode(mode));
    settingsMenuOpen = null;
  }

  function chooseReasoningEffort(effort: string) {
    updateThreadSettings(currentComposerMode(), effort);
    settingsMenuOpen = null;
  }

  function threadRuntimeLabel(threadId: string) {
    if (threadId !== selectedThreadId) return '';
    if (connection === 'connected' && executorConnected) return 'live';
    if (connection === 'connected') return 'viewing';
    if (connection === 'connecting') return 'connecting…';
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
    const itemData = asRecord(item.data);
    const data = Object.keys(itemData).length ? { ...item, ...itemData } : item;
    const id = stringFrom(data.id ?? item.id);
    if (!id) return null;
    const messages = Array.isArray(data.messages) ? data.messages : [];
    const lastMessage = messages[messages.length - 1] as unknown;
    const preview = previewFromMessage(lastMessage) || stringFrom(data.preview) || stringFrom(data.title);
    const env = asRecord(data.env);
    const repo = stringFrom(data.repository) || repoFromEnv(env) || 'local';
    const agentMode = threadAgentModeFrom(data, messages);
    return {
      id,
      title: stringFrom(data.title) || 'Untitled',
      repo,
      branch: stringFrom(data.branch) || branchFromEnv(env) || 'main',
      preview,
      messageCount: Number(data.messageCount ?? messages.length ?? 0),
      agentMode,
      reasoningEffort: threadReasoningEffortFrom(data, agentMode),
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
    const agentMode = threadAgentModeFrom(thread, messages, { allowThreadActorFallback: false });
    if (!agentMode) {
      throw new Error('agent mode could not be determined from thread');
    }
    const records = Array.isArray(thread.compactionRecords)
      ? thread.compactionRecords.map(asRecord)
      : [];
    return {
      id: stringFrom(thread.id) || selectedThreadId,
      title: stringFrom(thread.title) || 'Untitled',
      repo: repoFromEnv(env) || 'local',
      branch: branchFromEnv(env) || 'main',
      agentMode,
      reasoningEffort: threadReasoningEffortFrom(thread, agentMode, messages),
      messages,
      contextLabel: 'local context',
      contextUsage: contextUsageFrom(thread) ?? contextUsageFromMessages(messages),
      cost: costFrom(thread),
      costBreakdownURL: costBreakdownURLFrom(thread),
      referencedThread: referencedThreadFromMessages(messages),
      compactionRecords: records,
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
  // doesn't repeat the same info that's already shown in the reference card above.
  function stripReferencePrefix(text: string): string {
    return text.replace(/^Continuing work from thread\s+T-[a-f0-9-]+\.?\s*/i, '').trim();
  }

  // Detect "Continuing work from thread T-..." marker in the first user message.
  function referencedThreadFromMessages(messages: NeoMessage[]): { threadId: string; instructions?: string } | undefined {
    const first = messages.find((m) => m.role === 'user');
    if (!first) return undefined;
    const text = userTextFromBlocks(first.content);
    const m = text.match(/Continuing work from thread\s+(T-[a-f0-9-]+)\.?\s*([\s\S]*)$/i);
    if (!m) return undefined;
    const threadId = m[1];
    const rest = m[2].trim();
    // Pull "Instructions:" sentence if present; otherwise take the first short line.
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
      reasoningEffort: summary.reasoningEffort,
      messages: [],
      contextLabel: 'local context',
      compactionRecords: []
    };
  }

  function agentModeFromMessages(messages: unknown[]) {
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      const message = asRecord(messages[index]);
      if (stringFrom(message.role) === 'user') {
        const mode = stringFrom(message.agentMode);
        if (mode) return mode;
      }
    }
    return '';
  }

  function threadMapAgentMode(raw: Record<string, unknown>, messages: unknown[] = []): string {
    const mode = stringFrom(raw.agentMode);
    if (mode) return mode;
    const data = asRecord(raw.data);
    if (Object.keys(data).length > 0) {
      const nestedMessages = Array.isArray(data.messages) ? data.messages : messages;
      const nestedMode: string = threadMapAgentMode(data, nestedMessages);
      if (nestedMode) return nestedMode;
    }
    const messageMode = agentModeFromMessages(messages);
    if (messageMode) return messageMode;
    const meta = asRecord(raw.meta);
    return meta.usesThreadActors === true ? defaultAgentMode : '';
  }

  function threadAgentModeFrom(
    raw: Record<string, unknown>,
    messages: unknown[] = [],
    options: { allowThreadActorFallback?: boolean } = {}
  ) {
    const mode = threadMapAgentMode(raw, messages);
    if (mode) return normalizeAgentMode(mode);
    if (options.allowThreadActorFallback !== false && threadCanUseLocalRuntime(raw)) {
      return defaultAgentMode;
    }
    return '';
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
        if (block.type === 'redacted_thinking') return 'Thinking: redacted';
        if (block.type === 'tool_use' || block.type === 'server_tool_use') return `Using ${block.name ?? 'tool'}`;
        if (isImageBlock(block)) return imageBlockName(block) ? `[image: ${imageBlockName(block)}]` : '[image]';
        return '';
      })
      .filter(Boolean)
      .join('');
  }

  function userTextFromBlocks(blocks: ContentBlock[]) {
    // Trim trailing whitespace so single-word bubbles ("ship", "1", "go on") don't render as 2 lines
    // due to preserved newlines under white-space: pre-wrap.
    return blocks
      .map((block) => block.type === 'text' ? block.text ?? '' : '')
      .filter(Boolean)
      .join('')
      .replace(/[\s\n]+$/g, '');
  }

  function userPreviewFromBlocks(blocks: ContentBlock[]) {
    const text = userTextFromBlocks(blocks).trim();
    const images = imageBlocksFrom(blocks).length;
    const imageLabel = images > 0 ? `${images} image${images === 1 ? '' : 's'}` : '';
    return [text, imageLabel].filter(Boolean).join(' ');
  }

  function isImageBlock(block: ContentBlock) {
    return block.type === 'image' || block.type === 'input_image' || block.type === 'image_url';
  }

  function imageBlocksFrom(blocks: ContentBlock[]) {
    return blocks.filter((block) => isImageBlock(block) && imageBlockSrc(block));
  }

  function imageBlockName(block: ContentBlock) {
    return stringFrom(block.name ?? block.filename ?? block.file_name ?? block.title) || 'image';
  }

  function imageBlockSrc(block: ContentBlock) {
    const source = asRecord(block.source);
    const directData = stringFrom(source.data ?? block.data ?? block.base64);
    const directMediaType = stringFrom(source.media_type ?? source.mediaType ?? block.media_type ?? block.mediaType ?? block.mime_type ?? block.mimeType) || 'image/png';
    if (directData) {
      if (directData.startsWith('data:')) return directData;
      return `data:${directMediaType};base64,${directData}`;
    }
    const imageURL = stringFrom(block.image_url);
    if (imageURL) return imageURL;
    const nestedURL = stringFrom(asRecord(block.image_url).url);
    return nestedURL || stringFrom(block.attachmentUrl ?? block.url ?? block.uri ?? block.href);
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

    // 3. Inline replacements.
    src = src
      // Markdown links FIRST so we can grab the bracketed label whole before backticks rewrite it.
      // [text](url) — capture trimmed label + url. Strip optional surrounding whitespace inside parens.
      .replace(/\[([^\]\n]+)\]\(\s*([^)\s]+(?:\s*#[^)]+)?)\s*\)/g, (_match, label, url) => {
        const trimmedLabel = label.trim();
        const trimmedUrl = url.trim();
        const isWebLink = /^https?:\/\//.test(trimmedUrl);
        // For file-path URLs (anything not http) the label already shows the path nicely — render
        // as an underlined inline code chip (matches ampcode's "[ `path` ](abs-path)" → underlined code chip).
        // For web URLs, render as standard external link.
        if (isWebLink) {
          return `<a class="md-link" href="${trimmedUrl}" target="_blank" rel="noopener">${trimmedLabel}</a>`;
        }
        // Drop surrounding backticks from the label so we can wrap it in our own code chip
        const inner = trimmedLabel.replace(/^`+|`+$/g, '');
        const fileHref = /^file:\/\//.test(trimmedUrl) ? trimmedUrl : (trimmedUrl.startsWith('/') ? `file://${trimmedUrl}` : trimmedUrl);
        return `<a class="md-link md-link--file" href="${fileHref}"><code class="md-code md-code--path">${inner}</code></a>`;
      })
      .replace(/`([^`\n]+?)`/g, '<code class="md-code">$1</code>')
      .replace(/\*\*([^*\n]+?)\*\*/g, '<strong>$1</strong>')
      .replace(/(^|[\s(])\*([^*\n]+?)\*(?=[\s.,;:!?)]|$)/g, '$1<em>$2</em>')
      // file:// URIs (not already inside a link) render as inline code chips.
      .replace(/(?<!["=])file:\/\/([^\s<)"]+)/g, '<code class="md-code md-code--path">$1</code>')
      // Bare http(s) URLs (not already inside a link)
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

  function shortThreadId(id: string): string {
    if (!id || id.length <= 14) return id;
    const m = id.match(/^(T-[a-f0-9]{4})/i);
    const head = m ? m[1] : id.slice(0, 6);
    const tail = id.slice(-4);
    return `${head}…${tail}`;
  }

  // Models prefix thinking blocks with a bold heading ("**Investigating code paths**") followed
  // by prose. Ampcode hides the heading and renders only the prose; do the same.
  function stripThinkingTitle(text: string): string {
    if (!text) return text;
    return text.replace(/^\s*\*\*[^*\n]+\*\*\s*\n+/, '').trimStart();
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
    return cost?.url || thread?.costBreakdownURL || '';
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

  function cloneRecord(value: Record<string, unknown>) {
    try {
      return structuredClone(value);
    } catch {
      return { ...value };
    }
  }

  function newThreadID() {
    return `T-${crypto.randomUUID()}`;
  }

  function stringFrom(value: unknown) {
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
    return typeof value === 'string' ? value : '';
  }

  function numberFrom(value: unknown) {
    const n = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
    return Number.isFinite(n) ? n : NaN;
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
    if (connection === 'connected' && executorConnected) return agentState === 'idle' ? 'executor connected' : agentState;
    if (connection === 'connected') return 'thread connected, no executor';
    return connection;
  }

  function groupTranscriptItems(messages: NeoMessage[], records: Record<string, unknown>[] = []): TranscriptItem[] {
    const items: TranscriptItem[] = [];
    let assistantMessages: NeoMessage[] = [];
    let assistantStart = '';
    const cutMessageIds = new Set(records.map(compactionCutMessageId).filter(Boolean));
    const emittedCompactions = new Set<string>();

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

    const pushCompactionBefore = (message: NeoMessage) => {
      const cutMessageId = message.messageId;
      if (!cutMessageIds.has(cutMessageId) || emittedCompactions.has(cutMessageId)) return;
      flushAssistant();
      emittedCompactions.add(cutMessageId);
      items.push({ kind: 'compaction', cutMessageId, key: `compaction-${cutMessageId}` });
    };

    for (const message of messages) {
      if (isCompactionSummaryMessage(message)) {
        if (cutMessageIds.size === 0 && !emittedCompactions.has(message.messageId)) {
          flushAssistant();
          emittedCompactions.add(message.messageId);
          items.push({ kind: 'compaction', cutMessageId: message.messageId, key: `compaction-summary-${message.messageId}` });
        }
        continue;
      }

      if (shouldSkipInfoTranscriptMessage(message)) continue;
      pushCompactionBefore(message);

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

  function compactionCutMessageId(record: Record<string, unknown>) {
    return stringFrom(record.cutMessageId ?? record.cut_message_id ?? record.messageId ?? record.message_id);
  }

  function isCompactionSummaryMessage(message: NeoMessage) {
    if (message.role !== 'info') return false;
    return message.content.some((block) => {
      if (block.type !== 'summary') return false;
      const summary = asRecord(block.summary);
      const type = stringFrom(summary.type);
      return type === 'message' || type === 'thread';
    });
  }

  function shouldSkipInfoTranscriptMessage(message: NeoMessage) {
    if (message.role !== 'info') return false;
    return !message.content.some(isRenderableInfoBlock);
  }

  function isRenderableInfoBlock(block: ContentBlock) {
    return block.type === 'manual_bash_invocation' && !isHiddenBlock(block);
  }

  function isHiddenBlock(block: ContentBlock) {
    const hidden = block.hidden;
    if (typeof hidden === 'string') return hidden.trim().length > 0 && hidden.trim().toLowerCase() !== 'false';
    return Boolean(hidden);
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
    return message.role === 'user' && userPreviewFromBlocks(message.content).trim().length > 0;
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

        if (message.role !== 'info' && block.type === 'text' && block.text) {
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
      if (message.role !== 'info' && block.type === 'text' && block.text) {
        segments.push({ kind: 'text', block, key: `${message.messageId}-text-${index}` });
      }
    });

    flushWork();
    return segments;
  }

  function isRenderableWorkBlock(block: ContentBlock) {
    if (block.type === 'thinking') return Boolean(block.thinking) || plausibleBlockTimeMillis(block.startTime) > 0;
    if (block.type === 'redacted_thinking') return Boolean(block.data) || plausibleBlockTimeMillis(block.startTime) > 0;
    if (block.type === 'tool_use' || block.type === 'server_tool_use') return true;
    if (block.type === 'manual_bash_invocation') return !isHiddenBlock(block);
    if (block.type === 'tool_result') {
      return Boolean(blockContentPreview(block) || toolResultUseID(block) || Object.keys(toolResultRun(block)).length > 0);
    }
    return false;
  }

  function isProgressTextBlock(message: NeoMessage, block: ContentBlock, blockIndex: number) {
    if (block.type !== 'text' || !block.text) return false;
    if (plausibleBlockTimeMillis(block.startTime) <= 0 && plausibleBlockTimeMillis(block.finalTime) <= 0) return false;
    if (message.state?.stopReason === 'tool_use') return true;
    return message.content.slice(blockIndex + 1).some((next) => next.type === 'tool_use' || next.type === 'server_tool_use');
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

  function thinkingText(block: ContentBlock) {
    if (block.type === 'redacted_thinking') return 'Redacted thinking';
    return block.thinking ?? '';
  }

  function traceTimeLabel(block: ContentBlock) {
    return traceTimeLabelForBlocks([block]);
  }

  function traceTimeLabelForBlocks(blocks: ContentBlock[]) {
    const starts = blocks.map((block) => plausibleBlockTimeMillis(block.startTime)).filter((value) => value > 0);
    if (starts.length === 0) return '';
    const start = Math.min(...starts);
    return formatTraceTime(start);
  }

  function traceTimeLabelForRow(block: ContentBlock, result?: ContentBlock) {
    return result ? traceTimeLabelForBlocks([block, result]) : traceTimeLabel(block);
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

  function formatTraceTime(milliseconds: number) {
    return new Intl.DateTimeFormat(undefined, {
      hour: '2-digit',
      minute: '2-digit',
      hour12: false
    }).format(new Date(milliseconds));
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
  type ToolCategory = 'explore' | 'edit' | 'command' | 'thread' | 'skill' | 'task' | 'web' | 'painter' | 'review' | 'other';
  function normalizedToolName(name: string) {
    return (name || '').toLowerCase().replace(/[\s_-]+/g, '');
  }

  function isCodeReviewToolName(name: string) {
    const n = normalizedToolName(name);
    return n === 'codereview' || n.includes('codereview');
  }

  function toolCategory(name: string): ToolCategory {
    const n = normalizedToolName(name);
    // Order matters — check most specific first
    if (n === 'painter' || n === 'renderaggman' || n === 'viewmedia' || n === 'lookat') return 'painter';
    if (isCodeReviewToolName(name)) return 'review';
    if (n.includes('shell') || n.includes('command') || n.includes('bash') || n.includes('terminal') || n === 'exec' || n === 'run') return 'command';
    if (n.includes('editfile') || n.includes('writefile') || n.includes('createfile') || n.includes('patch') || n.includes('edit') || n === 'write' || n === 'modify' || n === 'update') return 'edit';
    if (n.includes('subagent') || n === 'task' || n === 'agent' || n.includes('spawn')) return 'task';
    if (n.includes('web')) return 'web';
    // Thread reads/searches aggregate into the Explored summary (matches ampcode's "Explored N threads/searches")
    if (n.includes('thread') || n.includes('skill') || n.includes('search') || n.includes('grep') || n.includes('ripgrep')) return 'explore';
    if (n.includes('read') || n.includes('view') || n === 'cat') return 'explore';
    if (n.includes('list') || n.includes('glob') || n.includes('find') || n === 'tree' || n === 'ls' || n.includes('explore') || n.includes('directory')) return 'explore';
    return 'other';
  }

  function shellExploreKind(command: string) {
    const trimmed = command.trim();
    if (!trimmed) return '';
    if (/^(?:env\s+[^=]+=[^\s]+\s+)*(?:rg|grep|ag)\b/.test(trimmed)) return 'grep';
    if (/^(?:env\s+[^=]+=[^\s]+\s+)*(?:find|fd)\b/.test(trimmed)) return 'list';
    if (/^(?:cat|bat|less|more|head|tail|nl|sed\s+-n)\b/.test(trimmed)) return 'read';
    if (/^(?:ls|tree)\b/.test(trimmed)) return 'list';
    return '';
  }

  function toolCategoryForBlock(block: ContentBlock): ToolCategory {
    const category = toolCategory(block.name || '');
    if (category !== 'command') return category;
    return shellExploreKind(commandText(block)) ? 'explore' : category;
  }

  function exploreNoun(name: string): string {
    const n = normalizedToolName(name);
    if (n.includes('findthread') || n.includes('searchthread') || n.includes('threadsearch')) return 'search';
    if (n.includes('readthread')) return 'thread';
    if (n.includes('thread')) return 'thread';
    if (n.includes('search') || n.includes('grep') || n.includes('ripgrep')) return 'search';
    if (n.includes('list') || n.includes('glob') || n.includes('find') || n === 'tree' || n === 'ls' || n.includes('directory')) return 'list';
    if (n.includes('skill')) return 'skill';
    return 'file';
  }

  function exploreNounForBlock(block: ContentBlock): string {
    const shellKind = shellExploreKind(commandText(block));
    if (shellKind === 'grep') return 'search';
    if (shellKind === 'list') return 'list';
    return exploreNoun(block.name || '');
  }
  function pluralize(noun: string, n: number): string {
    if (n === 1) return `1 ${noun}`;
    if (noun === 'search') return `${n} searches`;
    return `${n} ${noun}s`;
  }

  type DisplayRow =
    | { kind: 'thinking'; block: ContentBlock }
    | { kind: 'progress'; block: ContentBlock }
    | { kind: 'explore'; tools: TraceToolEntry[] }
    | { kind: 'edit'; block: ContentBlock; result?: ContentBlock }
    | { kind: 'command'; block: ContentBlock; result?: ContentBlock }
    | { kind: 'painter'; block: ContentBlock; result?: ContentBlock }
    | { kind: 'review'; block: ContentBlock; result?: ContentBlock }
    | { kind: 'tool'; block: ContentBlock; result?: ContentBlock }
    | { kind: 'result'; block: ContentBlock };

  type TraceToolEntry = {
    block: ContentBlock;
    result?: ContentBlock;
  };

  function groupWorkBlocks(blocks: ContentBlock[]): DisplayRow[] {
    const rows: DisplayRow[] = [];
    let buf: TraceToolEntry[] = [];
    const flush = () => { if (buf.length) { rows.push({ kind: 'explore', tools: buf }); buf = []; } };
    const results = collectToolResults(blocks);
    for (const b of blocks) {
      if (b.type === 'thinking' || b.type === 'redacted_thinking') {
        if (thinkingText(b).trim()) {
          flush();
          rows.push({ kind: 'thinking', block: b });
        }
        continue;
      }
      else if (b.type === 'text') {
        if (!b.text?.trim()) continue;
        flush();
        rows.push({ kind: 'progress', block: b });
      }
      else if (b.type === 'tool_use' || b.type === 'server_tool_use') {
        const cat = toolCategoryForBlock(b);
        const result = toolResultForBlock(results, b);
        if (cat === 'explore') buf.push({ block: b, result });
        else if (cat === 'edit') { flush(); rows.push({ kind: 'edit', block: b, result }); }
        else if (cat === 'command') { flush(); rows.push({ kind: 'command', block: b, result }); }
        else if (cat === 'painter') { flush(); rows.push({ kind: 'painter', block: b, result }); }
        else if (cat === 'review') { flush(); rows.push({ kind: 'review', block: b, result }); }
        else { flush(); rows.push({ kind: 'tool', block: b, result }); }
      } else if (b.type === 'manual_bash_invocation') {
        flush();
        rows.push({ kind: 'command', block: b, result: b });
      } else if (b.type === 'tool_result') { continue; }
      else { flush(); rows.push({ kind: 'tool', block: b }); }
    }
    flush();
    return rows;
  }

  function exploreSummary(tools: TraceToolEntry[]): string {
    const counts: Record<string, number> = {};
    for (const t of tools) {
      const noun = exploreNounForBlock(t.block);
      counts[noun] = (counts[noun] || 0) + 1;
    }
    const order = ['file', 'thread', 'search', 'list', 'skill'];
    const parts: string[] = [];
    for (const k of order) if (counts[k]) parts.push(pluralize(k, counts[k]));
    return parts.join(', ');
  }

  function traceTimeLabelForToolEntries(tools: TraceToolEntry[]) {
    return traceTimeLabelForBlocks(tools.flatMap((tool) => tool.result ? [tool.block, tool.result] : [tool.block]));
  }

  function editTarget(block: ContentBlock): string {
    const input = toolInputRecord(block);
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
    if (block.type === 'manual_bash_invocation') {
      const args = block.args;
      if (Array.isArray(args)) return args.map((value) => stringFrom(value)).filter(Boolean).join(' ');
      const record = asRecord(args);
      return stringFrom(record.cmd ?? record.command ?? block.content) || '';
    }
    const input = toolInputRecord(block);
    return stringFrom(input.command ?? input.cmd ?? input.script) || '';
  }

  function collectToolResults(blocks: ContentBlock[]) {
    const results = new Map<string, ContentBlock>();
    for (const block of blocks) {
      if (block.type !== 'tool_result') continue;
      const id = toolResultUseID(block);
      if (id) results.set(id, block);
    }
    return results;
  }

  function toolUseID(block: ContentBlock) {
    const record = asRecord(block);
    return stringFrom(block.id ?? record.toolUseID ?? record.tool_use_id ?? record.toolCallID ?? record.toolCallId);
  }

  function toolResultUseID(block: ContentBlock) {
    const record = asRecord(block);
    return stringFrom(block.toolUseID ?? record.toolUseID ?? record.tool_use_id ?? record.toolCallID ?? record.toolCallId ?? block.id);
  }

  function toolResultForBlock(results: Map<string, ContentBlock>, block: ContentBlock) {
    const id = toolUseID(block);
    return id ? results.get(id) : undefined;
  }

  function toolResultRun(block?: ContentBlock) {
    return asRecord(block?.run ?? block?.toolRun ?? asRecord(block).run ?? asRecord(block).toolRun);
  }

  function toolResultResult(block?: ContentBlock) {
    return asRecord(toolResultRun(block).result);
  }

  function toolResultResultText(block?: ContentBlock) {
    const result = toolResultRun(block).result;
    return typeof result === 'string' ? result : '';
  }

  function toolResultExitCode(block?: ContentBlock) {
    const run = toolResultRun(block);
    const result = toolResultResult(block);
    return numberFrom(result.exitCode ?? result.exit_code ?? run.exitCode ?? run.exit_code);
  }

  function toolResultStatus(block?: ContentBlock) {
    const run = toolResultRun(block);
    return stringFrom(run.status ?? asRecord(block).status).toLowerCase();
  }

  function isFailedToolResult(block?: ContentBlock) {
    if (!block) return false;
    const exitCode = toolResultExitCode(block);
    if (Number.isFinite(exitCode) && exitCode !== 0) return true;
    return ['error', 'failed', 'cancelled', 'rejected-by-user'].includes(toolResultStatus(block));
  }

  function codeReviewTitle(result?: ContentBlock) {
    const status = toolResultStatus(result);
    if (status === 'queued' || status === 'in-progress') return 'Reviewing code';
    if (status === 'error' || status === 'failed') return 'Code review failed';
    return 'Reviewed code';
  }

  function codeReviewCheckPayload(check: Record<string, unknown>) {
    return asRecord(check.result);
  }

  function codeReviewCheckNestedResult(check: Record<string, unknown>) {
    return asRecord(codeReviewCheckPayload(check).result);
  }

  function codeReviewIssueCount(check: Record<string, unknown>) {
    const payload = codeReviewCheckPayload(check);
    const nested = codeReviewCheckNestedResult(check);
    for (const value of [
      check.issueCount,
      check.issue_count,
      check.issuesCount,
      check.issues_count,
      check.findingCount,
      check.finding_count,
      check.findingsCount,
      check.findings_count,
      check.problemCount,
      check.problem_count,
      payload.issueCount,
      payload.issue_count,
      payload.issuesCount,
      payload.issues_count,
      payload.findingCount,
      payload.finding_count,
      nested.issuesFound,
      nested.issues_found,
      nested.issueCount,
      nested.issue_count
    ]) {
      const n = numberFrom(value);
      if (Number.isFinite(n)) return n;
    }
    for (const source of [check, payload, nested]) {
      for (const key of ['issues', 'findings', 'problems', 'diagnostics', 'results']) {
        const value = source[key];
        if (Array.isArray(value)) return value.length;
      }
    }
    return NaN;
  }

  function codeReviewCheckFallbackLabel(key: string) {
    const path = filePathFromURI(key).replace(/\/+$/, '');
    const base = path.split(/[\\/]/).pop() || path || key;
    return base.replace(/\.md$/i, '');
  }

  function codeReviewCheckLabel(key: string, check: Record<string, unknown>) {
    const payload = codeReviewCheckPayload(check);
    const checkMeta = asRecord(payload.check);
    const nested = codeReviewCheckNestedResult(check);
    return firstString(
      check.title,
      check.name,
      check.label,
      checkMeta.name,
      nested.name,
      codeReviewCheckFallbackLabel(key)
    ).replace(/[_-]+/g, ' ');
  }

  function codeReviewErrorText(value: unknown) {
    const record = asRecord(value);
    return firstString(value, record.message, record.error, record.errorMessage);
  }

  function codeReviewCheckStatus(check: Record<string, unknown>) {
    const nested = codeReviewCheckNestedResult(check);
    return stringFrom(check.status ?? nested.status).trim().toLowerCase().replace(/_/g, '-');
  }

  function codeReviewSummaryName(block?: ContentBlock) {
    const input = block ? toolInputRecord(block) : {};
    return stringFrom(input.thinking).trim().toLowerCase() === 'low' ? 'quick code review' : 'code review';
  }

  function codeReviewActions(block?: ContentBlock, result?: ContentBlock) {
    const run = toolResultRun(result);
    const nestedResult = asRecord(run.result);
    const decodedProgress = decodeRivetProgress(run.progress);
    const progress = asRecord(decodedProgress);
    const main = firstRecord(nestedResult.main, progress.main);
    const checks = firstRecord(nestedResult.checks, progress.checks);
    const outputLines = stringFrom(progress.output).split('\n').map((line) => line.trim()).filter(Boolean);
    const status = toolResultStatus(result);
    const reviewKind = codeReviewSummaryName(block);
    const actions: string[] = [];
    const seen = new Set<string>();
    const add = (title: string) => {
      if (!title || seen.has(title)) return;
      seen.add(title);
      actions.push(title);
    };

    if (status === 'queued') add('Code review queued');
    if (status === 'error' || status === 'failed') add(`Code review failed: ${codeReviewErrorText(run.error ?? nestedResult.error) || 'Unknown error'}`);

    let completedChecks = 0;
    let totalChecks = 0;
    for (const [key, rawCheck] of Object.entries(checks)) {
      const check = asRecord(rawCheck);
      if (Object.keys(check).length === 0) continue;
      totalChecks += 1;
      const label = codeReviewCheckLabel(key, check);
      const checkStatus = codeReviewCheckStatus(check);
      if (checkStatus === 'done' || checkStatus === 'complete' || checkStatus === 'completed') {
        completedChecks += 1;
        const count = codeReviewIssueCount(check);
        if (!Number.isFinite(count)) add(`Check ${label}: complete`);
        else if (count === 0) add(`Check ${label}: ok`);
        else add(`Check ${label}: ${count} ${count === 1 ? 'issue' : 'issues'} found`);
      } else if (checkStatus === 'error' || checkStatus === 'failed') {
        completedChecks += 1;
        add(`Check ${label}: error (${codeReviewErrorText(check.error) || 'Unknown error'})`);
      } else if (checkStatus === 'in-progress' || checkStatus === 'running') {
        add(`Check ${label}: ${firstString(check.message) || 'Running check...'}`);
      }
    }

    if (actions.length === 0) {
      for (const line of outputLines) add(line);
    }
    if (stringFrom(main.status).trim().toLowerCase() === 'done' && totalChecks > 0 && completedChecks < totalChecks) {
      add('Main review complete, running checks...');
    }
    if (status === 'done' || status === 'complete' || status === 'completed') add('Code review complete');
    if (actions.length === 0) add(status === 'queued' ? 'Code review queued' : 'Reviewing code changes...');
    return { actions, summary: totalChecks > 0 ? `${completedChecks}/${totalChecks} checks · ${reviewKind}` : reviewKind };
  }

  function toolResultPreview(block?: ContentBlock) {
    if (!block) return '';
    const run = toolResultRun(block);
    const result = toolResultResult(block);
    return firstString(
      result.displayMessage,
      result.output,
      result.stderr,
      result.stdout,
      result.message,
      result.text,
      toolResultResultText(block),
      run.displayMessage,
      run.output,
      run.message,
      run.text,
      run.error,
      blockContentPreview(block)
    );
  }

  type PainterImage = {
    src: string;
    name: string;
    savedPath: string;
  };

  function painterTitle(block: ContentBlock) {
    const name = normalizedToolName(block.name || '');
    if (name === 'renderaggman') return 'Render Agg Man';
    if (name === 'viewmedia') return 'Viewed media';
    if (name === 'lookat') return 'Look At';
    return 'Painter';
  }

  function painterPrompt(block: ContentBlock, result?: ContentBlock) {
    const input = toolInputRecord(block);
    const run = toolResultRun(result);
    const nestedResult = asRecord(run.result);
    return firstString(
      input.prompt,
      input.description,
      input.message,
      input.request,
      run.prompt,
      nestedResult.prompt,
      toolSubtitle(block)
    );
  }

  function readImageFromToolResult(block?: ContentBlock) {
    const result = toolResultResult(block);
    if (result.isImage !== true) return null;
    return painterImageFromAny({
      type: 'image',
      ...result,
      mimeType: stringFrom(asRecord(result.imageInfo).mimeType ?? asRecord(result.imageInfo).mime_type ?? result.mimeType ?? result.mime_type),
      mediaType: stringFrom(asRecord(result.imageInfo).mimeType ?? asRecord(result.imageInfo).mime_type ?? result.mediaType ?? result.media_type),
      data: stringFrom(result.content ?? result.data ?? result.base64),
      url: stringFrom(result.contentURL ?? result.contentUrl ?? result.url ?? result.uri),
      savedPath: stringFrom(result.absolutePath ?? result.path ?? result.filePath ?? result.file_path)
    }, 0);
  }

  function painterImagesFromResult(block?: ContentBlock) {
    const run = toolResultRun(block);
    const result = toolResultResult(block);
    const images: PainterImage[] = [];
    const primary = [run.images, run.image, result.images, result.image, run.result].find((value) => Array.isArray(value) && value.length > 0);
    const append = (value: unknown) => {
      if (Array.isArray(value)) {
        value.forEach(append);
        return;
      }
      const image = painterImageFromAny(value, images.length);
      if (image) {
        images.push(image);
        return;
      }
      const record = asRecord(value);
      for (const key of ['images', 'image', 'outputImages', 'output_images', 'generatedImages', 'generated_images']) {
        if (record[key] !== undefined) append(record[key]);
      }
    };

    if (primary) {
      append(primary);
    } else {
      [run.outputImages, run.output_images, run.generatedImages, run.generated_images, result.outputImages, result.output_images, result.generatedImages, result.generated_images, run.result, run].forEach(append);
    }
    return images;
  }

  function painterImageFromAny(raw: unknown, index: number): PainterImage | null {
    if (typeof raw === 'string') {
      const value = raw.trim();
      if (!value) return null;
      if (value.startsWith('data:image/') || value.startsWith('http://') || value.startsWith('https://')) {
        return { src: value, name: `image-${index + 1}.png`, savedPath: '' };
      }
      if (value.startsWith('file://') || value.startsWith('/')) {
        return { src: '', name: value.split('/').pop() || `image-${index + 1}.png`, savedPath: value };
      }
      return null;
    }

    const record = asRecord(raw);
    if (Object.keys(record).length === 0) return null;
    const source = asRecord(record.source);
    const imageUrl = asRecord(record.image_url);
    const imageInfo = asRecord(record.imageInfo);
    const mediaType = stringFrom(record.mediaType ?? record.media_type ?? record.mimeType ?? record.mime_type ?? source.mediaType ?? source.media_type ?? source.mimeType ?? source.mime_type ?? imageInfo.mimeType ?? imageInfo.mime_type) || 'image/png';
    const data = firstString(record.data, record.base64, record.b64_json, record.content, record.contentBase64, source.data, source.base64, source.b64_json);
    const url = firstString(record.url, record.uri, record.href, record.contentURL, record.contentUrl, record.imageURL, record.imageUrl, record.image_url, source.url, source.uri, imageUrl.url);
    let savedPath = firstString(record.savedPath, record.saved_path, record.absolutePath, record.path, record.file, record.filename, record.filePath, record.file_path);
    const block = {
      type: 'image',
      ...record,
      data: data || record.data,
      mediaType,
      url
    } as ContentBlock;
    let src = imageBlockSrc(block);
    if (src.startsWith('file://') || src.startsWith('/')) {
      savedPath = savedPath || src;
      src = '';
    }
    if (!src && !savedPath) return null;
    const name = firstString(record.name, record.filename, record.file_name, record.title) || savedPath.split('/').pop() || `image-${index + 1}.png`;
    return { src, name, savedPath };
  }

  function prettyToolLabel(name: string): string {
    const n = (name || '').toLowerCase().replace(/[\s_-]+/g, '');
    // 'review' tools render as "Reviewed code" (matches ampcode's "Reviewed code code review").
    if (n.includes('codereview') || n.includes('review')) return 'Reviewed code';

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
    // Everything else (legacy relationship tools, subagent, task, custom tools) renders as
    // "Ran tool <name>" — matches ampcode exactly: "Ran tool" + muted tool name.
    return 'Ran tool';
  }

  // Subtitle for "Ran tool X" rows: the tool's literal name, lowercased + spaced.
  function ranToolName(block: ContentBlock): string {
    const raw = (block.name || '').trim();
    if (!raw) return '';
    return raw.replace(/[_-]+/g, ' ').toLowerCase();
  }

  function traceActionLabel(block: ContentBlock) {
    const name = normalizedToolName(block.name || '');
    const shellKind = shellExploreKind(commandText(block));
    if (name.includes('findthread') || name.includes('searchthread') || name.includes('threadsearch')) return 'Searched threads:';
    if (name.includes('readthread')) return 'Read thread:';
    if (shellKind === 'grep') return 'Grep';
    if (shellKind === 'read') return 'Read:';
    if (shellKind === 'list') return 'Listed:';
    const label = prettyToolLabel(block.name || '');
    return label.endsWith(':') ? label : `${label}:`;
  }

  function toolSubtitle(block: ContentBlock) {
    const input = toolInputRecord(block);
    const shellKind = shellExploreKind(commandText(block));
    if (shellKind === 'grep') {
      const summary = grepCommandSummary(commandText(block));
      if (summary) return summary;
    }

    const fields = [
      input.command, input.cmd, input.script,
      input.query, input.pattern, input.search, input.q,
      input.goal, input.prompt, input.request, input.task, input.message,
      input.path, input.file, input.filename, input.file_path, input.filepath,
      input.url, input.uri,
      input.threadID, input.threadId, input.thread_id, input.thread,
      input.name, input.title, input.description,
    ];
    for (const f of fields) {
      const s = stringFrom(f);
      if (s) {
        const trimmed = s.trim();
        if (trimmed.length > 0) return trimmed.length > 160 ? trimmed.slice(0, 160) + '…' : trimmed;
      }
    }
    const patch = rawPatchFromBlock(block);
    if (patch) {
      const stats = patchStats(displayPatchFromBlock(block));
      return `${stats.files} ${stats.files === 1 ? 'file' : 'files'} changed`;
    }
    return '';
  }

  function grepCommandSummary(command: string) {
    const trimmed = command.trim();
    if (!trimmed) return '';
    const quoted = trimmed.match(/(["'])(.*?)(?<!\\)\1/);
    const pattern = quoted && quoted[2] ? quoted[2].trim() : '';
    if (!quoted || !pattern) return trimmed.length > 160 ? trimmed.slice(0, 160) + '…' : trimmed;
    const withoutQuoted = trimmed.replace(quoted[0], ' ');
    const parts = withoutQuoted.split(/\s+/).filter(Boolean);
    const paths = parts.slice(1).filter((part) => {
      if (!part || part.startsWith('-')) return false;
      if (/^[A-Z_][A-Z0-9_]*=/.test(part)) return false;
      return !part.includes('=');
    });
    const target = paths[paths.length - 1] || '.';
    const summary = `${target} "${pattern}"`;
    return summary.length > 160 ? summary.slice(0, 160) + '…' : summary;
  }

  function rawPatchFromBlock(block: ContentBlock) {
    const input = toolInputRecord(block);
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

  // Format tool input as ampcode-style key: value lines (strings unquoted, primitives bare,
  // nested objects/arrays JSON-stringified inline). Falls back to JSON if input isn't a plain object.
  function toolInputPreview(block: ContentBlock) {
    const input = toolInputRecord(block);
    const keys = Object.keys(input);
    if (keys.length === 0) return partialJSONFromBlock(block);
    const lines: string[] = [];
    for (const key of keys) {
      const value = input[key];
      let rendered: string;
      if (value == null) {
        rendered = 'null';
      } else if (typeof value === 'string') {
        rendered = value;
      } else if (typeof value === 'number' || typeof value === 'boolean') {
        rendered = String(value);
      } else if (Array.isArray(value)) {
        rendered = JSON.stringify(value);
      } else if (typeof value === 'object') {
        rendered = JSON.stringify(value, null, 2);
      } else {
        rendered = String(value);
      }
      lines.push(`${key}: ${rendered}`);
    }
    return lines.join('\n');
  }

  function toolInputRecord(block: ContentBlock) {
    const incomplete = asRecord(block.inputIncomplete);
    if (Object.keys(incomplete).length > 0) return incomplete;
    return asRecord(block.input);
  }

  function partialJSONFromBlock(block: ContentBlock) {
    return stringFrom(asRecord(block.inputPartialJSON).json ?? asRecord(block.inputPartialJSONDelta).json);
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

{#snippet userBubble(blocks: ContentBlock[])}
  {@const text = stripReferencePrefix(userTextFromBlocks(blocks))}
  {@const images = imageBlocksFrom(blocks)}
  <div class:message__bubble--media={images.length > 0 && !text.trim()} class="message__bubble">{#if text.trim()}<div class="message__bubble-text">{text}</div>{/if}{#if images.length > 0}<div class="message-images" aria-label="Attached images">{#each images as block, i (`${imageBlockName(block)}-${i}`)}<figure class="message-image"><img src={imageBlockSrc(block)} alt={imageBlockName(block)} /></figure>{/each}</div>{/if}</div>
{/snippet}

{#snippet traceBlock(block: ContentBlock)}
  {#if (block.type === 'thinking' || block.type === 'redacted_thinking') && thinkingText(block)}
    <details class="trace-block trace-block--thinking">
      <summary class="trace-time-anchor" data-time={traceTimeLabel(block)}>
        <ChevronRight size={14} class="trace-block__chevron" />
        <Sparkles size={14} />
        <span>thinking</span>
        <small>{blockStatus(block)}</small>
      </summary>
      <p>{thinkingText(block)}</p>
    </details>
  {:else if block.type === 'tool_use' || block.type === 'server_tool_use'}
    <details class="trace-block trace-block--tool">
      <summary class="trace-time-anchor" data-time={traceTimeLabel(block)}>
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
      <summary class="trace-time-anchor" data-time={traceTimeLabel(block)}>
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
  <details class="work-group" open={live}>
    <!-- Ampcode parity: the "Worked for X minutes" header has no hover timestamp. -->
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
          {#if thinkingText(row.block)}
            <div class="trace-thinking md">{@html renderMarkdown(stripThinkingTitle(thinkingText(row.block)))}</div>
          {/if}
        {:else if row.kind === 'progress'}
          {#if row.block.text}
            <div class="trace-thinking trace-thinking--progress trace-time-anchor md" data-time={traceTimeLabel(row.block)}>{@html renderMarkdown(stripThinkingTitle(row.block.text))}</div>
          {/if}
        {:else if row.kind === 'explore'}
          <details class="trace-row trace-row--explore">
            <summary class="trace-time-anchor" data-time={traceTimeLabelForToolEntries(row.tools)}>
              <span class="trace-row__label">Explored</span>
              <span class="trace-row__sub">{exploreSummary(row.tools)}</span>
              <ChevronRight size={12} class="trace-row__chevron" />
            </summary>
            <ul class="trace-row__list">
              {#each row.tools as tool, i (tool.block.id ?? i)}
                {@const readImage = readImageFromToolResult(tool.result)}
                <li class="trace-time-anchor" data-time={traceTimeLabelForRow(tool.block, tool.result)}>
                  <span class="trace-row__list-label">{traceActionLabel(tool.block)}</span>
                  <span class="trace-row__list-target">{toolSubtitle(tool.block)}</span>
                  {#if readImage}
                    <figure class="trace-row__image">
                      {#if readImage.src}
                        <img src={readImage.src} alt={readImage.name} />
                      {:else}
                        <div class="painter-image__placeholder"><ImagePlus size={18} /></div>
                      {/if}
                      <figcaption title={readImage.savedPath || readImage.name}>{readImage.savedPath || readImage.name}</figcaption>
                    </figure>
                  {/if}
                </li>
              {/each}
            </ul>
          </details>
        {:else if row.kind === 'edit'}
          {@const stats = patchStats(displayPatchFromBlock(row.block))}
          <details class="trace-row trace-row--edit">
            <summary class="trace-time-anchor" data-time={traceTimeLabel(row.block)}>
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
          {@const commandFailed = isFailedToolResult(row.result)}
          {@const commandExitCode = toolResultExitCode(row.result)}
          {@const commandExitLabel = Number.isFinite(commandExitCode) ? `exit code ${commandExitCode}` : 'exit code -1'}
          {@const commandOutput = toolResultPreview(row.result)}
          {@const commandFullText = commandText(row.block) || prettyToolLabel(row.block.name || 'command')}
          <details class="trace-row trace-row--cmd" class:trace-row--failed={commandFailed}>
            <summary class="trace-time-anchor" data-time={traceTimeLabelForRow(row.block, row.result)}><code class="trace-row__cmd"><span class="trace-row__prompt">$</span><span class="trace-row__cmd-text">{commandFullText}</span></code><ChevronRight size={12} class="trace-row__chevron" /></summary>
            <div class="code-panel code-panel--cmd">
              <div class="code-panel__head">
                <span class="code-panel__cmd">$ {commandFullText}</span>
                {#if commandFailed}
                  <span class="code-panel__exit">{commandExitLabel}</span>
                {/if}
              </div>
              {#if commandOutput}
                <pre class="code-panel__out">{commandOutput}</pre>
              {/if}
            </div>
          </details>
        {:else if row.kind === 'painter'}
          {@const painterImages = painterImagesFromResult(row.result)}
          {@const painterText = toolResultPreview(row.result)}
          {@const prompt = painterPrompt(row.block, row.result)}
          <details class="trace-row trace-row--painter" open={painterImages.length > 0}>
            <summary class="trace-time-anchor" data-time={traceTimeLabelForRow(row.block, row.result)}>
              <span class="trace-row__label">{painterTitle(row.block)}</span>
              {#if prompt}
                <span class="trace-row__sub">{prompt}</span>
              {/if}
              <ChevronRight size={12} class="trace-row__chevron" />
            </summary>
            <div class="painter-panel">
              {#if painterImages.length > 0}
                <div class="painter-images" aria-label="Generated images">
                  {#each painterImages as image, i (`${image.name}-${i}`)}
                    <figure class="painter-image">
                      {#if image.src}
                        <img src={image.src} alt={image.name} />
                      {:else}
                        <div class="painter-image__placeholder"><ImagePlus size={18} /></div>
                      {/if}
                      <figcaption>
                        <span title={image.savedPath || image.name}>{image.savedPath || image.name}</span>
                        {#if image.src}
                          <a class="painter-image__action" href={image.src} download={image.name} title="Download image" aria-label="Download image"><Download size={12} /></a>
                        {/if}
                      </figcaption>
                    </figure>
                  {/each}
                </div>
              {:else if toolInputPreview(row.block)}
                <pre class="code-panel">{toolInputPreview(row.block)}</pre>
              {/if}
              {#if painterText && painterText !== prompt}
                <div class="painter-panel__text">{painterText}</div>
              {/if}
            </div>
          </details>
        {:else if row.kind === 'review'}
          {@const review = codeReviewActions(row.block, row.result)}
          <details class="trace-row trace-row--review" open={toolResultStatus(row.result) === 'in-progress' || toolResultStatus(row.result) === 'queued'}>
            <summary class="trace-time-anchor" data-time={traceTimeLabelForRow(row.block, row.result)}>
              <span class="trace-row__label">{codeReviewTitle(row.result)}</span>
              <span class="trace-row__sub">{review.summary}</span>
              <ChevronRight size={12} class="trace-row__chevron" />
            </summary>
            <ul class="trace-row__list">
              {#each review.actions as action}
                <li>
                  <span class="trace-row__list-label">Review</span>
                  <span class="trace-row__list-target">{action}</span>
                </li>
              {/each}
            </ul>
          </details>
        {:else}
          {@const ranLabel = prettyToolLabel(row.block.name || '')}
          {@const ranSub = ranLabel === 'Ran tool' ? ranToolName(row.block) : toolSubtitle(row.block)}
          <details class="trace-row trace-row--ran">
            <summary class="trace-time-anchor" data-time={traceTimeLabel(row.block)}>
              <span class="trace-row__label">{ranLabel}</span>
              {#if ranSub}
                <span class="trace-row__sub">{ranSub}</span>
              {/if}
              <ChevronRight size={12} class="trace-row__chevron" />
            </summary>
            {#if toolInputPreview(row.block)}
              <pre class="code-panel">{toolInputPreview(row.block)}</pre>
            {/if}
          </details>
        {/if}
      {/each}
    </div>
  </details>
{/snippet}

{#snippet threadInspector()}
  <div class="inspector-card">
    <div class="inspector-fields">
      <div class="inspector-field"><Sparkles size={13} /> <span>{currentComposerMode()}{currentComposerReasoningEffort() ? ` · ${currentComposerReasoningEffort()}` : ''}</span></div>
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
      <div class:status-ok={executorConnected} class="inspector-field"><Wifi size={13} /> <span>{executorConnected ? 'executor connected' : 'no executor attached'}</span></div>
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

    <div class="inspector-section inspector-section--dev">
      <button
        class:dev-toggle--active={devMode}
        class="dev-toggle"
        type="button"
        role="switch"
        aria-checked={devMode}
        title="Toggle dev diagnostics"
        onclick={() => { devMode = !devMode; }}
      >
        <Wrench size={13} />
        <span class="dev-toggle__label">Dev diagnostics</span>
        {#if devSignalCount > 0}
          <span class="dev-toggle__count">{devSignalCount}</span>
        {/if}
        <span class="dev-toggle__track" aria-hidden="true"><span class="dev-toggle__thumb"></span></span>
      </button>
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
        {#each queuedMessages as queued (queuedMessageKey(queued))}
          <p class="runtime-pill">
            <span>{queued.steer ? 'steer' : 'queued'}</span>
            {queued.preview || queued.messageId}
          </p>
        {/each}
      </div>
    </section>
  {/if}

  {#if devMode && (inferenceTools || toolLeases.length > 0 || executorStatuses.length > 0 || runtimeEvents.length > 0 || runtimeTraces.length > 0 || retryNotice)}
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
        {#each runtimeTraces as trace (trace.id)}
          <p class="runtime-pill">
            <span>{trace.status === 'done' ? 'trace' : 'trace running'}</span>
            {trace.name}{trace.detail ? ` · ${trace.detail}` : ''}
          </p>
        {/each}
        {#each runtimeEvents as event (event.id)}
          <p class="runtime-pill">
            <span>{event.label}</span>
            {event.detail}
          </p>
        {/each}
      </div>
    </section>
  {/if}

  {#if devMode && artifacts.length > 0}
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

  {#if railRelationships.length > 0}
    <section class="runtime-section runtime-section--relationships">
      <h2>Relationships <span class="runtime-section__count">{railRelationships.length}</span></h2>
      <div class="relationships-list">
        {#each railRelationships as relationship (`${relationship.threadID}-${relationship.type}-${relationship.role}`)}
          <button
            class="relationship-item"
            type="button"
            onclick={() => { void selectThread(relationship.threadID); }}
            title={relationship.comment ? `${relationship.threadID}: ${relationship.comment}` : relationship.threadID}
          >
            <span class="relationship-item__type">{relationship.type}</span>
            <span class="relationship-item__id">{shortThreadId(relationship.threadID)}</span>
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
      {#if mobilePane === 'thread' && (detail || selectedThreadId)}
        <button
          class="icon-button icon-button--mobile-only"
          type="button"
          title="Thread info"
          aria-label="Open thread info"
          onclick={() => { mobileInspectorOpen = true; }}
        >
          <Info size={15} />
        </button>
      {/if}
      <button
        class="icon-button"
        type="button"
        title="New thread"
        aria-label="New thread"
        disabled={newThreadStarting}
        onclick={() => { void startNewThread(); }}
      >
        {#if newThreadStarting}
          <Loader2 size={15} class="spin" />
        {:else}
          <SquarePen size={15} />
        {/if}
      </button>
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
                      class:thread-card__runtime--live={connection === 'connected' && executorConnected}
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
            class:connection-chip--live={connection === 'connected' && executorConnected}
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
          {#if referencedThread}
            <button
              class="reference-card"
              type="button"
              onclick={() => { void selectThread(referencedThread.threadId); }}
              title="Open source thread"
            >
              <ArrowLeft size={14} />
              <div class="reference-card__body">
                <div class="reference-card__head">Referenced thread <span class="reference-card__id">{referencedThread.threadId}</span></div>
                {#if referencedThread.instructions}
                  <div class="reference-card__instr">Instructions: "{referencedThread.instructions}"</div>
                {/if}
              </div>
            </button>
          {/if}
          {#each transcriptItems as item (item.key)}
            {#if item.kind === 'user'}
              <article class="message message--user" data-message-id={item.message.messageId}>
                {@render userBubble(item.message.content)}
              </article>
            {:else if item.kind === 'compaction'}
              <div class="compaction-row" data-cut-message-id={item.cutMessageId}>
                <span class="compaction-row__line"></span>
                <span class="compaction-row__label">Compacted</span>
                <span class="compaction-row__line"></span>
              </div>
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

        {#if newActivityBelow}
          <button
            class:latest-chip--attachments={composerAttachments.length > 0}
            class:latest-chip--queue={queuedMessages.length > 0}
            class="latest-chip"
            type="button"
            onclick={jumpToLatest}
          >
            <ArrowDown size={13} />
            New activity
          </button>
        {/if}

        <div
          class:composer-dock--attachments={composerAttachments.length > 0}
          class:composer-dock--queue={queuedMessages.length > 0}
          class="composer-dock"
          aria-hidden="true"
        ></div>
        <footer class="composer-footer">
          <form class="composer" onsubmit={(event) => { event.preventDefault(); void sendMessage(); }}>
          <div
            class:composer-status--live={connection === 'connected' && executorConnected}
            class:composer-status--connecting={connection === 'connecting' || (connection === 'connected' && !executorConnected)}
            class="composer-status"
          >
            <span class="runtime-dot"></span>
            <span class="composer-status__main">{composerStatusMain()}</span>
            {#each composerStatusParts() as part}
              <span class="composer-status__sep">·</span>
              <span class="composer-status__part">{part}</span>
            {/each}
          </div>
          {#if queuedMessages.length > 0}
            <div class="queue-strip" aria-label="Queued messages">
              <div class="queue-strip__head">
                <span class="queue-strip__title">{queuedMessages.length} queued</span>
                <div class="queue-strip__actions">
                  <button
                    class="queue-strip__button"
                    type="button"
                    title="Dequeue prompts into the composer"
                    aria-label="Dequeue prompts into the composer"
                    onclick={dequeueQueuedMessagesToComposer}
                  >
                    <Download size={13} />
                  </button>
                  <button
                    class="queue-strip__button"
                    type="button"
                    title="Steer all queued prompts"
                    aria-label="Steer all queued prompts"
                    disabled={!canSteerQueuedMessages()}
                    onclick={steerQueuedMessages}
                  >
                    <ArrowUp size={13} />
                  </button>
                  <button
                    class="queue-strip__button"
                    type="button"
                    title="Discard queued prompts"
                    aria-label="Discard queued prompts"
                    onclick={discardQueuedMessages}
                  >
                    <Trash2 size={13} />
                  </button>
                </div>
              </div>
              <div class="queue-strip__list">
                {#each queuedMessages as queued (queuedMessageKey(queued))}
                  <div class:queue-strip__item--steer={queued.steer} class="queue-strip__item">
                    {#if queued.steer}
                      <span class="queue-strip__steer queue-strip__steer--active" title="Queued prompt will steer the actor">
                        <ArrowUp size={12} />
                      </span>
                    {:else}
                      <button
                        class="queue-strip__steer"
                        type="button"
                        title="Steer with this queued prompt"
                        aria-label="Steer with this queued prompt"
                        disabled={!canSendMessage()}
                        onclick={() => steerQueuedMessage(queued)}
                      >
                        <ArrowUp size={12} />
                      </button>
                    {/if}
                    <span class="queue-strip__kind">{queued.steer ? 'steer' : 'queued'}</span>
                    <span class="queue-strip__preview">{queued.preview || queued.messageId}</span>
                    <button
                      class="queue-strip__remove"
                      type="button"
                      title="Remove queued prompt"
                      aria-label="Remove queued prompt"
                      onclick={() => removeQueuedMessage(queued)}
                    >
                      <X size={12} />
                    </button>
                  </div>
                {/each}
              </div>
            </div>
          {/if}
          <div
            class="composer-body"
            role="group"
            aria-label="Message composer drop zone"
            ondragover={(event) => {
              event.preventDefault();
              if (event.dataTransfer) event.dataTransfer.dropEffect = 'copy';
            }}
            ondrop={handleComposerDrop}
          >
            <textarea
              bind:this={composerTextarea}
              bind:value={composer}
              rows="2"
              placeholder={canSendMessage() ? shouldQueueOutgoingMessage() ? queueIsFull() ? 'Queue is full for this thread...' : 'Queue a message for this thread...' : 'Send a message to this thread...' : connection === 'connected' ? 'Open this thread locally to send...' : 'Connect to send a message...'}
              onpaste={handleComposerPaste}
              oninput={detectThreadMentionTrigger}
              onkeydown={handleComposerKeydown}
            ></textarea>
            {#if composerAttachments.length > 0}
              <div class="composer-attachments" aria-label="Attached images">
                {#each composerAttachments as attachment (attachment.id)}
                  <div class="composer-attachment">
                    <img src={attachment.previewUrl} alt={attachment.name} />
                    <span class="composer-attachment__name" title={attachment.name}>{attachment.name}</span>
                    <span class="composer-attachment__size">{formatBytes(attachment.size)}</span>
                    <button
                      class="composer-attachment__remove"
                      type="button"
                      title="Remove image"
                      aria-label="Remove {attachment.name}"
                      disabled={composerUploadActive}
                      onclick={() => removeComposerAttachment(attachment.id)}
                    >
                      <X size={12} />
                    </button>
                  </div>
                {/each}
              </div>
            {/if}
            <div class="composer-actions">
              <div class="composer-actions__left">
                <input
                  bind:this={attachmentInput}
                  class="composer-file-input"
                  type="file"
                  accept={composerImageAccept}
                  multiple
                  onchange={handleAttachmentInput}
                />
                <button
                  class="composer-tool-button"
                  type="button"
                  title="Attach image"
                  aria-label="Attach image"
                  disabled={!canSendMessage() || composerUploadActive || composerAttachments.length >= maxComposerImages}
                  onclick={() => attachmentInput?.click()}
                >
                  <ImagePlus size={14} />
                </button>
                {#if shouldQueueOutgoingMessage()}
                  <span class="composer-queue-mode">queue</span>
                {/if}
                {#if canEditThreadSettings()}
                  <div class="composer-settings" aria-label="New thread settings">
                    <div class="composer-menu">
                      <button
                        class:composer-menu__trigger--active={settingsMenuOpen === 'mode'}
                        class="composer-menu__trigger"
                        type="button"
                        aria-haspopup="menu"
                        aria-expanded={settingsMenuOpen === 'mode'}
                        title="Agent mode"
                        onclick={() => toggleSettingsMenu('mode')}
                      >
                        <span class="composer-menu__eyebrow">mode</span>
                        <span>{agentModeLabels[currentComposerMode()] ?? currentComposerMode()}</span>
                        <ChevronRight size={12} class="composer-menu__chevron" />
                      </button>
                      {#if settingsMenuOpen === 'mode'}
                        <div class="composer-menu__panel" role="menu" aria-label="Agent mode">
                          {#each visibleAgentModeOptions as mode}
                            <button
                              class:composer-menu__item--active={currentComposerMode() === mode}
                              class="composer-menu__item"
                              type="button"
                              role="menuitemradio"
                              aria-checked={currentComposerMode() === mode}
                              onclick={() => chooseAgentMode(mode)}
                            >
                              <span>{agentModeLabels[mode] ?? mode}</span>
                            </button>
                          {/each}
                        </div>
                      {/if}
                    </div>
                    {#if reasoningEffortOptionsForMode(currentComposerMode()).length > 0}
                      <div class="composer-menu composer-menu--effort">
                        <button
                          class:composer-menu__trigger--active={settingsMenuOpen === 'effort'}
                          class="composer-menu__trigger"
                          type="button"
                          aria-haspopup="menu"
                          aria-expanded={settingsMenuOpen === 'effort'}
                          title="Reasoning effort"
                          onclick={() => toggleSettingsMenu('effort')}
                        >
                          <span class="composer-menu__eyebrow">effort</span>
                          <span>{reasoningEffortLabels[currentComposerReasoningEffort()] ?? currentComposerReasoningEffort()}</span>
                          <ChevronRight size={12} class="composer-menu__chevron" />
                        </button>
                        {#if settingsMenuOpen === 'effort'}
                          <div class="composer-menu__panel" role="menu" aria-label="Reasoning effort">
                            {#each reasoningEffortOptionsForMode(currentComposerMode()) as effort}
                              <button
                                class:composer-menu__item--active={currentComposerReasoningEffort() === effort}
                                class="composer-menu__item"
                                type="button"
                                role="menuitemradio"
                                aria-checked={currentComposerReasoningEffort() === effort}
                                onclick={() => chooseReasoningEffort(effort)}
                              >
                                <span>{reasoningEffortLabels[effort] ?? effort}</span>
                              </button>
                            {/each}
                          </div>
                        {/if}
                      </div>
                    {/if}
                  </div>
                {/if}
                {#if previousThreadHint}
                  <button
                    class="previous-thread-hint"
                    type="button"
                    title={`Reference ${previousThreadHint.title || previousThreadHint.id}`}
                    onclick={acceptPreviousThreadHint}
                  >
                    <span>press</span>
                    <kbd>enter</kbd>
                    <span>to reference the previous thread</span>
                  </button>
                {/if}
              </div>
              <div class="composer-actions__right">
                {#if canInterruptActor()}
                  <button
                    class="interrupt-button"
                    type="button"
                    title="Interrupt actor"
                    aria-label="Interrupt actor"
                    onclick={interruptActor}
                  >
                    <CircleStop size={15} />
                  </button>
                {/if}
                <button
                  class:send-button--queue={shouldQueueOutgoingMessage()}
                  class="send-button"
                  type="submit"
                  disabled={!canSubmitComposer()}
                  title={shouldQueueOutgoingMessage() ? queueIsFull() ? 'Queue full' : 'Queue message' : 'Send message'}
                  aria-label={shouldQueueOutgoingMessage() ? queueIsFull() ? 'Queue full' : 'Queue message' : 'Send message'}
                >
                  {#if composerUploadActive}
                    <Loader2 size={14} class="spin" />
                  {:else}
                    <ArrowUp size={14} />
                  {/if}
                </button>
              </div>
            </div>
          </div>
          {#if mentionPickerOpen}
            <div class="mention-picker" role="dialog" aria-label="Select a thread to mention">
              <div class="mention-picker__search">
                <Search size={14} />
                <input
                  bind:this={mentionSearchInput}
                  bind:value={mentionSearch}
                  placeholder="Search threads..."
                  oninput={() => { mentionActiveIndex = 0; }}
                  onkeydown={handleMentionPickerKeydown}
                />
              </div>
              <div class="mention-picker__list" role="listbox" aria-label="Threads">
                {#if mentionThreadOptions.length > 0}
                  {#each mentionThreadOptions as thread, index (thread.id)}
                    <button
                      class:mention-picker__item--active={index === mentionActiveIndex}
                      class="mention-picker__item"
                      type="button"
                      role="option"
                      aria-selected={index === mentionActiveIndex}
                      onmouseenter={() => { mentionActiveIndex = index; }}
                      onclick={() => insertThreadMention(thread)}
                    >
                      <span class="mention-picker__title">{thread.title}</span>
                      <span class="mention-picker__meta">{thread.repo}:{thread.branch} · {thread.updatedLabel}</span>
                      <span class="mention-picker__id">@{thread.id}</span>
                    </button>
                  {/each}
                {:else}
                  <div class="mention-picker__empty">
                    {loadingThreads ? 'Loading threads...' : 'No matching threads'}
                  </div>
                {/if}
              </div>
            </div>
          {/if}
          </form>
        </footer>
      {:else}
        <div class="empty-state">
          <PanelRight size={28} />
          <h1>Select a thread</h1>
          <p>Pick a thread from the list to view it here.</p>
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
    --neo-mono: "Berkeley Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
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
    letter-spacing: 0;
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
    min-height: 40px;
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
    /* 16px prevents iOS Safari from zooming in on focus. We bump the field min-height to compensate. */
    font-size: 16px;
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
  .icon-button:disabled { cursor: not-allowed; opacity: 0.45; }
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
    font-family: var(--neo-mono);
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
    font-size: 16px;
    line-height: 20px;
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
    letter-spacing: 0;
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

  /* Transcript column — ampcode parity:
   *   mx-auto max-w-2xl px-4 flex flex-col gap-4 pb-4
   *   max-width 672px, 16px gap between turns, 16px horizontal padding, 16px bottom padding. */
  .transcript {
    display: flex;
    flex-direction: column;
    gap: 16px;
    width: 100%;
    max-width: 672px;
    margin: 0 auto;
    padding-bottom: 16px;
    padding-left: 16px;
    padding-right: 16px;
    box-sizing: border-box;
  }

  .message {
    min-width: 0;
    animation: message-enter 180ms ease-out;
  }
  .compaction-row {
    display: flex;
    align-items: center;
    gap: 12px;
    min-width: 0;
    padding: 2px 0;
    color: var(--neo-muted);
    font-size: 13px;
    font-style: italic;
    line-height: 20px;
    animation: message-enter 180ms ease-out;
  }
  .compaction-row__line {
    flex: 1 1 0;
    height: 1px;
    min-width: 24px;
    background: color-mix(in srgb, var(--neo-border) 68%, transparent);
  }
  .compaction-row__label {
    flex: 0 0 auto;
    white-space: nowrap;
  }
  .message--user {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    width: 100%;
    min-width: 0;
    padding: 4px 0;
    margin-bottom: 16px;
  }

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
  .message__bubble--media {
    padding: 6px;
    background: color-mix(in srgb, var(--neo-ink) 7%, transparent);
  }
  .message__bubble-text + .message-images { margin-top: 8px; }
  .message-images {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
    gap: 6px;
    width: min(360px, 74vw);
  }
  .message-image {
    margin: 0;
    overflow: hidden;
    border: 1px solid var(--neo-border);
    border-radius: 8px;
    background: var(--neo-bg);
    aspect-ratio: 4 / 3;
  }
  .message-image img {
    display: block;
    width: 100%;
    height: 100%;
    object-fit: cover;
  }

  .message__agent {
    display: block;
    color: var(--neo-ink);
  }
  /* Inner column of assistant segments (work groups, thinking, prose) — matches ampcode's
   * `flex min-w-0 flex-col gap-2` (gap-2 = 8px between segments within a single turn). */
  .message__agent > div {
    display: flex;
    flex-direction: column;
    gap: 8px;
    min-width: 0;
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
    font-family: var(--neo-mono);
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
    font-family: var(--neo-mono);
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

  /* File path code chips can wrap inside the message column. */
  .md :global(code.md-code--path) { word-break: break-all; }

  /* Generic markdown link (http/https) */
  .md :global(a.md-link) {
    color: var(--neo-ink);
    text-decoration: underline;
    text-underline-offset: 2px;
    text-decoration-color: color-mix(in srgb, currentColor 40%, transparent);
    word-break: break-all;
  }
  .md :global(a.md-link:hover) { text-decoration-color: currentColor; }

  /* File-link variant: wraps a code chip and gets a tighter underline directly under the chip. */
  .md :global(a.md-link--file) {
    text-decoration: underline;
    text-decoration-color: var(--neo-muted);
    text-underline-offset: 3px;
  }
  .md :global(a.md-link--file:hover) { text-decoration-color: var(--neo-ink); }
  .md :global(a.md-link--file > code.md-code) {
    /* The chip becomes the visible label of the link — keep the gray bg from .md-code. */
    color: var(--neo-ink);
  }
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

  /* Work group is a segment within a turn; the parent supplies the 8px gap (ampcode parity). */
  .work-group { margin: 0; }
  .trace-time-anchor {
    position: relative;
  }
  /* Hover timestamp on tool rows — matches ampcode's transcript-row-timestamp:
   * absolute top-1.5 left-full ml-2 w-14 text-[10px] leading-4 with
   * `transition: opacity 600ms ease-in;` and opacity 0 → ~0.6 on hover. */
  .trace-time-anchor[data-time]:not([data-time=""])::before {
    content: attr(data-time);
    position: absolute;
    top: 0.28em;
    right: calc(100% + 18px);
    z-index: 2;
    color: var(--neo-soft);
    font-family: var(--neo-mono);
    font-size: 10px;
    font-weight: 400;
    font-variant-numeric: tabular-nums;
    letter-spacing: 0;
    line-height: 16px;
    opacity: 0;
    pointer-events: none;
    white-space: nowrap;
    transition: opacity 600ms ease-in;
  }
  .trace-time-anchor[data-time]:not([data-time=""]):hover::before {
    opacity: 0.6;
  }
  .work-group > summary {
    display: flex;
    align-items: center;
    gap: 12px;
    margin: 4px 0;
    cursor: pointer;
    list-style: none;
    color: var(--neo-muted);
    font-size: 12px;
    line-height: 16px;
  }
  .work-group > summary::-webkit-details-marker { display: none; }
  .work-group__line { flex: 1; height: 1px; background: var(--neo-border); }
  .work-group__button {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    min-height: 20px;
    padding: 2px 6px;
    border-radius: 6px;
    color: var(--neo-muted);
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .work-group > summary:hover .work-group__button {
    background: color-mix(in srgb, var(--neo-ink) 4%, transparent);
    color: var(--neo-ink);
  }
  .work-group__button span {
    color: inherit;
    font-weight: 400;
  }
  :global(.work-group__chevron) { transition: transform 140ms ease; }
  .work-group[open] :global(.work-group__chevron) { transform: rotate(90deg); }
  /* Body of an expanded work group — matches ampcode's inner `flex min-w-0 flex-col gap-2`
   * (8px between rows: thinking, explored, edited, ran, etc.). */
  .work-group__body { display: flex; flex-direction: column; gap: 8px; margin-top: 8px; }

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
  .trace-block__subline {
    overflow: hidden;
    padding: 4px 0 4px 16px;
    margin-top: 2px;
    color: var(--neo-muted);
    font-family: var(--neo-mono);
    font-size: 12.5px;
    line-height: 1.5;
    overflow-wrap: anywhere;
  }
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

  .trace-thinking {
    margin: 0;
    font-size: 13px;
    line-height: 20px;
    color: var(--neo-ink);
  }
  .trace-thinking.md { font-size: 13px; line-height: 20px; }
  .trace-thinking--progress { color: color-mix(in srgb, var(--neo-ink) 82%, var(--neo-muted)); }
  /* Trace rows — ampcode parity:
   *   `flex flex-col rounded-sm text-foreground/75 group/row hover:text-foreground`
   *   13px / 20px, no vertical margin (parent supplies gap), muted by default,
   *   becomes full ink on hover. */
  .trace-row {
    margin: 0;
    border: 0;
    background: transparent;
    font-size: 13px;
    line-height: 20px;
    color: color-mix(in srgb, var(--neo-ink) 75%, transparent);
    transition: color 150ms cubic-bezier(0.4, 0, 0.2, 1);
    min-width: 0;
    max-width: 100%;
    overflow: hidden;
  }
  .trace-row:hover { color: var(--neo-ink); }
  .trace-row > summary {
    display: flex;
    align-items: baseline;
    gap: 4px;
    padding: 2px 0;
    cursor: pointer;
    list-style: none;
    color: inherit;
    min-width: 0;
    max-width: 100%;
    overflow: hidden;
  }
  .trace-row > summary::-webkit-details-marker { display: none; }
  .trace-row__label { color: inherit; font-weight: 400; white-space: nowrap; }
  .trace-row--explore > summary .trace-row__label {
    color: inherit;
    font-weight: 400;
  }
  .trace-row__sub {
    color: var(--neo-muted);
    font-weight: 500;
    white-space: nowrap;
  }
  :global(.trace-row__chevron) {
    color: var(--neo-muted);
    opacity: 0.85;
    margin-left: 0;
    transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1), transform 140ms ease;
  }
  .trace-row:hover > summary :global(.trace-row__chevron) { opacity: 1; }
  .trace-row[open] > summary :global(.trace-row__chevron) { opacity: 1; transform: rotate(90deg); }

  .trace-row__list {
    list-style: none;
    margin: 4px 0 12px 0;
    padding: 0;
    color: var(--neo-muted);
    font-size: 13.25px;
    line-height: 21px;
  }
  .trace-row__list li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 4px;
    padding: 0;
    min-width: 0;
  }
  .trace-row__list-label {
    color: var(--neo-muted);
    flex-shrink: 0;
    white-space: nowrap;
  }
  .trace-row__list-target {
    color: var(--neo-muted);
    font-family: inherit;
    font-size: inherit;
    min-width: 0;
    flex: 1 1 auto;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .trace-row__image {
    flex-basis: 100%;
    width: min(220px, 100%);
    margin: 6px 0 4px 42px;
    color: var(--neo-muted);
  }
  .trace-row__image img,
  .trace-row__image .painter-image__placeholder {
    display: block;
    width: 100%;
    aspect-ratio: 1 / 1;
    border-radius: 6px;
    background: color-mix(in srgb, var(--neo-ink) 5%, transparent);
    outline: 1px solid color-mix(in srgb, var(--neo-ink) 12%, transparent);
    object-fit: cover;
  }
  .trace-row__image figcaption {
    padding-top: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 12px;
    line-height: 16px;
  }

  .trace-row__file {
    font-family: var(--neo-mono);
    font-size: 13.5px;
    color: var(--neo-accent, var(--neo-ink));
    text-decoration: underline;
    text-underline-offset: 2px;
    text-decoration-color: color-mix(in srgb, currentColor 30%, transparent);
    cursor: pointer;
  }
  .trace-row__file:hover { text-decoration-color: currentColor; }
  .trace-row__diff {
    font-family: var(--neo-mono);
    font-size: 12.5px;
    font-weight: 400;
    display: inline-flex;
    gap: 4px;
  }
  .trace-row--edit :global(.patch-shell) {
    margin: 6px 0 10px 16px;
  }

  /* Shell command row — ampcode parity:
   *   <code> = mono wrapper with `flex items-baseline gap-1.25 font-mono text-[12.5px]`
   *     <span> = `$` prompt (inherits row color foreground/75)
   *     <span class="trace-row__cmd-text"> = `min-w-0 truncate text-muted-foreground
   *       group-hover/row:text-foreground` — single-line ellipsis, muted, swaps to
   *       full ink when the row is hovered. Stays truncated even when open. */
  .trace-row__cmd {
    display: flex;
    align-items: baseline;
    gap: 5px;
    flex: 1 1 auto;
    min-width: 0;
    max-width: 100%;
    font-family: var(--neo-mono);
    font-size: 12.5px;
    background: transparent;
    padding: 0;
    line-height: 1.6;
    overflow: hidden;
  }
  .trace-row__prompt { color: inherit; flex-shrink: 0; }
  .trace-row__cmd-text {
    flex: 1 1 auto;
    min-width: 0;
    color: var(--neo-muted);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    transition: color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .trace-row--cmd:hover .trace-row__cmd-text { color: var(--neo-ink); }
  .trace-row--cmd .code-panel { margin: 4px 0 8px 0; }
  .trace-row--failed > summary { opacity: 1; }
  .trace-row--failed .trace-row__prompt { color: var(--neo-danger); }

  .trace-row--ran > summary { display: flex; align-items: baseline; gap: 6px; min-width: 0; }
  .trace-row--ran .trace-row__label { color: var(--neo-ink); }
  .trace-row--ran .trace-row__sub {
    color: var(--neo-muted);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
    flex: 1 1 auto;
  }
  .trace-row--ran .code-panel { margin: 4px 0 8px 16px; }

  .trace-row--painter > summary {
    display: flex;
    align-items: baseline;
    gap: 6px;
    min-width: 0;
  }
  .trace-row--painter .trace-row__label { color: var(--neo-ink); }
  .trace-row--painter .trace-row__sub {
    min-width: 0;
    flex: 1 1 auto;
    overflow: hidden;
    color: var(--neo-muted);
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .painter-panel {
    display: grid;
    gap: 8px;
    margin: 6px 0 10px 16px;
  }
  .painter-images {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(148px, 220px));
    gap: 8px;
    align-items: start;
  }
  .painter-image {
    min-width: 0;
    margin: 0;
  }
  .painter-image img,
  .painter-image__placeholder {
    display: block;
    width: 100%;
    aspect-ratio: 1 / 1;
    border-radius: 6px;
    background: color-mix(in srgb, var(--neo-ink) 5%, transparent);
    outline: 1px solid color-mix(in srgb, var(--neo-ink) 12%, transparent);
    object-fit: cover;
  }
  .painter-image__placeholder {
    display: grid;
    place-items: center;
    color: var(--neo-muted);
  }
  .painter-image figcaption {
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    padding-top: 4px;
    color: var(--neo-muted);
    font-size: 12px;
    line-height: 16px;
  }
  .painter-image figcaption span {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .painter-image__action {
    display: inline-flex;
    flex: 0 0 auto;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    border-radius: 4px;
    color: var(--neo-muted);
    text-decoration: none;
  }
  .painter-image__action:hover {
    background: color-mix(in srgb, var(--neo-ink) 8%, transparent);
    color: var(--neo-ink);
  }
  .painter-panel__text {
    color: var(--neo-muted);
    font-size: 12.5px;
    line-height: 18px;
  }

  @media (max-width: 760px) {
    .trace-time-anchor[data-time]:not([data-time=""])::before {
      top: -16px;
      right: auto;
      left: 0;
    }
  }

  /* Expanded tool body — matches ampcode's
   * `mb-1 max-h-[min(75vh,300px)] overflow-auto bg-foreground/5 py-2 px-3 rounded-md
   *  shadow outline-1 outline-black/10` + inner `<pre class="text-[12.5px]
   *  text-foreground/75 font-mono whitespace-pre-wrap break-all">`. */
  .code-panel {
    overflow: auto;
    max-height: min(75vh, 300px);
    margin: 0 0 4px;
    border-radius: 6px;
    background: color-mix(in srgb, var(--neo-ink) 5%, transparent);
    outline: 1px solid color-mix(in srgb, #000 60%, transparent);
    box-shadow: 0 1px 2px 0 color-mix(in srgb, #000 15%, transparent);
    color: color-mix(in srgb, var(--neo-ink) 75%, transparent);
    padding: 8px 12px;
    font-family: var(--neo-mono);
    font-size: 12.5px;
    line-height: 1.5;
    white-space: pre-wrap;
    word-break: break-all;
  }
  /* Command-panel composition: an outer rounded bubble holding a HEAD that
   * echoes the full command (wrapped, never truncated), a 1px separator, then
   * the OUT `<pre>` with the stdout. Matches ampcode's exact structure:
   *   bubble: `mb-1 max-h-[min(75vh,300px)] overflow-auto bg-foreground/5 py-2 px-3
   *            rounded-md shadow outline-1 outline-black/60`
   *   head:  `mb-2 flex flex-wrap items-baseline gap-x-2 gap-y-1 border-b border-border
   *            pb-2 text-[12.5px] text-foreground`
   *   inner: `min-w-0 flex-1 font-mono whitespace-pre-wrap break-all`
   *   out:   `text-[12.5px] text-foreground/75 font-mono whitespace-pre-wrap break-all` */
  .code-panel--cmd { padding: 0; }
  .code-panel__head {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 4px 8px;
    padding: 8px 12px;
    border-bottom: 1px solid var(--neo-border);
    color: var(--neo-ink);
    font-size: 12.5px;
  }
  .code-panel--cmd .code-panel__head:last-child { border-bottom: 0; padding-bottom: 8px; }
  .code-panel__cmd {
    flex: 1 1 auto;
    min-width: 0;
    font-family: var(--neo-mono);
    white-space: pre-wrap;
    word-break: break-all;
  }
  /* "exit code N" badge in the head row — matches ampcode's
   * `shrink-0 font-normal text-destructive` (12.5px inherits from head). */
  .code-panel__exit {
    flex: 0 0 auto;
    margin-left: auto;
    font-weight: 400;
    color: var(--neo-danger);
    white-space: nowrap;
  }
  .code-panel__out {
    margin: 0;
    padding: 8px 12px;
    font-family: var(--neo-mono);
    font-size: 12.5px;
    color: color-mix(in srgb, var(--neo-ink) 75%, transparent);
    white-space: pre-wrap;
    word-break: break-all;
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
  .latest-chip {
    position: fixed;
    left: 50%;
    bottom: 204px;
    z-index: 34;
    display: inline-flex;
    align-items: center;
    gap: 7px;
    min-height: 32px;
    padding: 6px 11px;
    border: 1px solid var(--neo-border-strong);
    border-radius: 999px;
    background: color-mix(in srgb, var(--neo-bg) 92%, var(--neo-ink));
    color: var(--neo-ink);
    box-shadow: 0 12px 28px color-mix(in srgb, var(--neo-shadow) 18%, transparent);
    font-size: 13px;
    font-weight: 600;
    line-height: 18px;
    transform: translateX(-50%);
    cursor: pointer;
  }
  .latest-chip:hover {
    background: var(--neo-card-hover);
  }
  .latest-chip--attachments { bottom: 268px; }
  .latest-chip--queue { bottom: 302px; }
  .latest-chip--attachments.latest-chip--queue { bottom: 366px; }
  /* Spacer that reserves vertical space at the end of the transcript so the sticky composer never covers the last message. */
  .composer-dock {
    height: 188px;
    flex-shrink: 0;
  }
  .composer-dock--attachments { height: 252px; }
  .composer-dock--queue { height: 286px; }
  .composer-dock--attachments.composer-dock--queue { height: 350px; }
  @media (max-width: 640px) {
    .composer-footer { padding: 6px; }
    .latest-chip { bottom: 188px; }
    .latest-chip--attachments { bottom: 258px; }
    .latest-chip--queue { bottom: 294px; }
    .latest-chip--attachments.latest-chip--queue { bottom: 362px; }
    .composer-dock { height: 172px; }
    .composer-dock--attachments { height: 242px; }
    .composer-dock--queue { height: 278px; }
    .composer-dock--attachments.composer-dock--queue { height: 346px; }
  }

  /* Composer: rounded-2xl card with status bar above + body below (matches ampcode `divide-y` pattern). */
  .composer {
    position: relative;
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
    overflow: visible;
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
  .queue-strip {
    display: grid;
    gap: 5px;
    padding: 7px 8px;
    border-bottom: 1px solid var(--neo-border);
    background: color-mix(in srgb, var(--neo-ink) 4%, var(--neo-bg));
  }
  .queue-strip__head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    min-width: 0;
  }
  .queue-strip__title {
    color: var(--neo-muted);
    font-size: 12px;
    font-weight: 600;
    line-height: 16px;
  }
  .queue-strip__actions {
    display: inline-flex;
    align-items: center;
    gap: 2px;
  }
  .queue-strip__button,
  .queue-strip__steer,
  .queue-strip__remove {
    display: inline-grid;
    place-items: center;
    border: 0;
    background: transparent;
    color: var(--neo-muted);
    cursor: pointer;
  }
  .queue-strip__button {
    width: 24px;
    height: 24px;
    border-radius: 50%;
  }
  .queue-strip__button:hover:not(:disabled),
  .queue-strip__steer:hover:not(:disabled),
  .queue-strip__remove:hover {
    background: var(--neo-card-hover);
    color: var(--neo-ink);
  }
  .queue-strip__button:disabled,
  .queue-strip__steer:disabled {
    cursor: not-allowed;
    opacity: 0.35;
  }
  .queue-strip__list {
    display: grid;
    gap: 3px;
    max-height: 82px;
    overflow: auto;
  }
  .queue-strip__item {
    display: grid;
    grid-template-columns: 20px auto minmax(0, 1fr) 20px;
    align-items: center;
    gap: 8px;
    min-width: 0;
    border-radius: 6px;
    padding: 2px 4px 2px 6px;
    color: var(--neo-muted);
    font-size: 12px;
    line-height: 17px;
  }
  .queue-strip__item--steer {
    color: var(--neo-ink);
    background: var(--neo-success-soft);
  }
  .queue-strip__steer {
    width: 20px;
    height: 20px;
    border-radius: 50%;
  }
  .queue-strip__steer--active {
    color: var(--neo-ink);
    animation: queue-steer-pulse 900ms ease-in-out infinite alternate;
  }
  .queue-strip__kind {
    color: var(--neo-soft);
    font-family: var(--neo-mono);
    font-size: 11px;
    white-space: nowrap;
  }
  .queue-strip__preview {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .queue-strip__remove {
    width: 20px;
    height: 20px;
    border-radius: 50%;
  }
  @keyframes queue-steer-pulse {
    from { opacity: 0.45; }
    to { opacity: 1; }
  }
  /* Body: textarea + actions row. Solid muted overlay so it stands out from the card bg (matches ampcode visually). */
  .composer-body {
    display: flex;
    flex-direction: column;
    background: color-mix(in srgb, var(--neo-ink) 7%, var(--neo-bg));
    border-bottom-right-radius: 15px;
    border-bottom-left-radius: 15px;
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
    /* 16px prevents iOS Safari from zooming in on focus. */
    font-size: 16px;
    line-height: 22px;
    padding: 12px;
    width: 100%;
  }
  .composer textarea::placeholder { color: var(--neo-muted); }
  .mention-picker {
    position: absolute;
    left: 8px;
    right: 8px;
    bottom: calc(100% + 8px);
    z-index: 40;
    border: 1px solid var(--neo-border);
    border-radius: 8px;
    background: color-mix(in srgb, var(--neo-bg) 92%, var(--neo-ink));
    box-shadow: 0 12px 30px color-mix(in srgb, #000 28%, transparent);
    overflow: hidden;
  }
  .mention-picker__search {
    position: relative;
    display: flex;
    align-items: center;
    height: 34px;
    border-bottom: 1px solid var(--neo-border);
    color: var(--neo-muted);
  }
  .mention-picker__search :global(svg) {
    position: absolute;
    left: 10px;
    color: var(--neo-muted);
  }
  .mention-picker__search input {
    width: 100%;
    height: 100%;
    border: 0;
    outline: 0;
    background: transparent;
    color: var(--neo-ink);
    font: inherit;
    font-size: 16px;
    line-height: 20px;
    padding: 0 10px 0 32px;
  }
  .mention-picker__search input::placeholder { color: var(--neo-muted); }
  .mention-picker__list {
    max-height: min(34vh, 238px);
    overflow: auto;
    padding: 4px;
  }
  .mention-picker__item {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: 1px 10px;
    width: 100%;
    border: 0;
    border-radius: 6px;
    background: transparent;
    color: var(--neo-ink);
    font: inherit;
    text-align: left;
    padding: 7px 8px;
    cursor: pointer;
  }
  .mention-picker__item:hover,
  .mention-picker__item--active {
    background: var(--neo-card-hover);
  }
  .mention-picker__title {
    grid-column: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-weight: 600;
  }
  .mention-picker__meta {
    grid-column: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--neo-muted);
    font-size: 11px;
    line-height: 15px;
  }
  .mention-picker__id {
    grid-column: 2;
    grid-row: 1 / span 2;
    align-self: center;
    max-width: 18ch;
    overflow: hidden;
    text-overflow: ellipsis;
    color: var(--neo-muted);
    font-family: var(--neo-mono);
    font-size: 11px;
    white-space: nowrap;
  }
  .mention-picker__empty {
    color: var(--neo-muted);
    padding: 12px;
    font-size: 12px;
  }
  .composer-attachments {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    padding: 0 8px 8px;
  }
  .composer-attachment {
    display: grid;
    grid-template-columns: 32px minmax(0, 1fr) auto 20px;
    align-items: center;
    gap: 7px;
    max-width: min(100%, 260px);
    padding: 4px;
    border: 1px solid var(--neo-border);
    border-radius: 8px;
    background: color-mix(in srgb, var(--neo-ink) 6%, transparent);
    color: var(--neo-muted);
    font-size: 11px;
    line-height: 14px;
  }
  .composer-attachment img {
    width: 32px;
    height: 32px;
    border-radius: 5px;
    object-fit: cover;
    background: var(--neo-bg);
  }
  .composer-attachment__name {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--neo-ink);
  }
  .composer-attachment__size {
    white-space: nowrap;
    color: var(--neo-soft);
  }
  .composer-attachment__remove,
  .composer-tool-button {
    display: inline-grid;
    place-items: center;
    border: 0;
    background: transparent;
    color: var(--neo-muted);
    cursor: pointer;
  }
  .composer-attachment__remove {
    width: 20px;
    height: 20px;
    border-radius: 50%;
  }
  .composer-attachment__remove:hover:not(:disabled),
  .composer-tool-button:hover:not(:disabled) {
    background: var(--neo-card-hover);
    color: var(--neo-ink);
  }
  .composer-attachment__remove:disabled,
  .composer-tool-button:disabled {
    cursor: not-allowed;
    opacity: 0.35;
  }
  .composer-file-input { display: none; }
  /* Bottom action row: attach (left) + send (right) */
  .composer-actions {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 6px 8px;
    gap: 8px;
  }
  .composer-actions__left {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
    min-width: 0;
  }
  .composer-actions__right {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    flex: 0 0 auto;
  }
  .composer-tool-button {
    width: 28px;
    height: 28px;
    border-radius: 50%;
    flex: 0 0 auto;
  }
  .composer-queue-mode {
    display: inline-flex;
    align-items: center;
    height: 22px;
    border-radius: 999px;
    color: var(--neo-muted);
    font-family: var(--neo-mono);
    font-size: 10.5px;
    line-height: 1;
    padding: 0 3px;
  }
  .composer-settings {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    min-width: 0;
  }
  .composer-menu {
    position: relative;
    flex: 0 0 auto;
  }
  .composer-menu__trigger {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    height: 28px;
    border: 1px solid transparent;
    border-radius: 999px;
    background: transparent;
    color: var(--neo-muted);
    cursor: pointer;
    font: inherit;
    font-size: 12px;
    line-height: 16px;
    padding: 0 7px;
    white-space: nowrap;
  }
  .composer-menu__trigger:hover,
  .composer-menu__trigger--active {
    border-color: var(--neo-border);
    background: var(--neo-card-hover);
    color: var(--neo-ink);
  }
  .composer-menu__eyebrow {
    color: var(--neo-soft);
    font-family: var(--neo-mono);
    font-size: 10.5px;
  }
  :global(.composer-menu__chevron) {
    color: var(--neo-soft);
    transform: rotate(-90deg);
  }
  .composer-menu__trigger--active :global(.composer-menu__chevron) {
    transform: rotate(90deg);
  }
  .composer-menu__panel {
    position: absolute;
    left: 0;
    bottom: calc(100% + 8px);
    z-index: 45;
    display: grid;
    gap: 2px;
    width: 148px;
    padding: 4px;
    border: 1px solid var(--neo-border);
    border-radius: 8px;
    background: color-mix(in srgb, var(--neo-bg) 92%, var(--neo-ink));
    box-shadow: 0 12px 30px color-mix(in srgb, #000 28%, transparent);
  }
  .composer-menu--effort .composer-menu__panel {
    width: 124px;
  }
  .composer-menu__item {
    display: flex;
    align-items: center;
    width: 100%;
    min-height: 28px;
    border: 0;
    border-radius: 6px;
    background: transparent;
    color: var(--neo-ink);
    font: inherit;
    font-size: 12px;
    line-height: 16px;
    padding: 5px 7px;
    text-align: left;
    cursor: pointer;
  }
  .composer-menu__item:hover,
  .composer-menu__item--active {
    background: var(--neo-card-hover);
  }
  .composer-menu__item--active {
    color: var(--neo-ink);
    font-weight: 600;
  }
  .previous-thread-hint {
    display: inline-flex;
    align-items: center;
    min-width: 0;
    gap: 5px;
    border: 0;
    background: transparent;
    color: var(--neo-muted);
    font: inherit;
    font-size: 12px;
    line-height: 16px;
    padding: 3px 4px;
    cursor: pointer;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .previous-thread-hint:hover { color: var(--neo-ink); }
  .previous-thread-hint kbd {
    flex: 0 0 auto;
    border-radius: 4px;
    border: 1px solid var(--neo-border-strong);
    padding: 0 4px;
    color: var(--neo-ink);
    font-family: var(--neo-mono);
    font-size: 11px;
    font-weight: 600;
    line-height: 15px;
  }
  .previous-thread-hint span:last-child {
    overflow: hidden;
    text-overflow: ellipsis;
  }
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
  .send-button--queue:not(:disabled) {
    background: var(--neo-success);
    color: var(--neo-bg);
  }
  .interrupt-button {
    display: inline-grid;
    width: 28px;
    height: 28px;
    place-items: center;
    border: 0;
    border-radius: 50%;
    background: color-mix(in srgb, var(--neo-danger) 16%, transparent);
    color: var(--neo-danger);
    cursor: pointer;
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .interrupt-button:hover {
    background: var(--neo-danger);
    color: var(--neo-bg);
  }

  /* Hidden on mobile (parity with ampcode `hidden lg:flex`). Shown at lg+ flush to right edge. */
  .inspector { display: none; }
  @media (min-width: 1024px) {
    .inspector {
      display: flex;
      flex-direction: column;
      gap: 12px;
      width: 100%;
      max-width: 21em;
      flex-shrink: 0;
      align-self: flex-start;
      position: sticky;
      /* Clear the sticky topbar (47px) + small gap so the inspector doesn't slide under it. */
      top: 56px;
      margin-top: 8px;
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
    font-family: var(--neo-mono);
  }

  /* Sub-section (heading + content) used for "Open in CLI" etc. */
  .inspector-section { display: flex; flex-direction: column; gap: 6px; }
  .inspector-section__head { color: var(--neo-ink); font-size: 12px; font-weight: 500; }
  .inspector-section--dev {
    padding-top: 2px;
  }
  .dev-toggle {
    display: grid;
    grid-template-columns: auto minmax(0, 1fr) auto auto;
    align-items: center;
    gap: 6px;
    width: 100%;
    border: 0;
    border-radius: 6px;
    background: transparent;
    color: var(--neo-muted);
    font: inherit;
    font-size: 12px;
    line-height: 16px;
    padding: 4px 0;
    text-align: left;
    cursor: pointer;
  }
  .dev-toggle:hover,
  .dev-toggle--active {
    color: var(--neo-ink);
  }
  .dev-toggle > :global(svg) {
    color: currentColor;
  }
  .dev-toggle__label {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .dev-toggle__count {
    min-width: 16px;
    border-radius: 999px;
    background: var(--neo-card-hover);
    color: var(--neo-muted);
    font-family: var(--neo-mono);
    font-size: 10px;
    line-height: 16px;
    text-align: center;
  }
  .dev-toggle__track {
    position: relative;
    width: 26px;
    height: 14px;
    border-radius: 999px;
    background: var(--neo-border-strong);
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .dev-toggle__thumb {
    position: absolute;
    top: 2px;
    left: 2px;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: var(--neo-bg);
    transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .dev-toggle--active .dev-toggle__track {
    background: var(--neo-accent);
  }
  .dev-toggle--active .dev-toggle__thumb {
    transform: translateX(12px);
  }
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
    font-family: var(--neo-mono);
  }
  .runtime-section {
    border-top: 1px solid var(--neo-border);
    padding-top: 12px;
  }
  .runtime-section h2 {
    margin: 0 0 8px;
    color: var(--neo-ink);
    font-size: 12px;
    font-weight: 500;
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
    font-family: var(--neo-mono);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0;
  }
  .runtime-pill--link {
    color: var(--neo-ink);
    font: inherit;
    font-family: var(--neo-mono);
    font-size: 12px;
    text-align: left;
    width: 100%;
    cursor: pointer;
    word-break: break-all;
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1), border-color 150ms cubic-bezier(0.4, 0, 0.2, 1);
  }
  .runtime-pill--link:hover { background: var(--neo-card-hover); border-color: var(--neo-border-strong); }

  /* Compact, scrollable relationships list */
  .runtime-section--relationships h2 { display: flex; align-items: center; gap: 6px; }
  .runtime-section__count {
    color: var(--neo-muted);
    font-weight: 400;
    font-size: 11px;
    background: var(--neo-card-hover);
    padding: 1px 6px;
    border-radius: 10px;
    line-height: 14px;
  }
  .relationships-list {
    display: flex;
    flex-direction: column;
    gap: 2px;
    max-height: 200px;
    overflow-y: auto;
    overscroll-behavior: contain;
    padding-right: 2px;
    margin: 0 -4px;
  }
  .relationships-list::-webkit-scrollbar { width: 4px; }
  .relationships-list::-webkit-scrollbar-thumb { background: var(--neo-border-strong); border-radius: 2px; }
  .relationships-list::-webkit-scrollbar-track { background: transparent; }
  .relationship-item {
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    padding: 4px 6px;
    border: 0;
    border-radius: 4px;
    background: transparent;
    color: var(--neo-ink);
    font: inherit;
    font-size: 12px;
    line-height: 16px;
    text-align: left;
    cursor: pointer;
    transition: background-color 150ms cubic-bezier(0.4, 0, 0.2, 1);
    min-width: 0;
  }
  .relationship-item:hover { background: var(--neo-card-hover); }
  .relationship-item__type {
    color: var(--neo-muted);
    font-family: var(--neo-mono);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.02em;
    flex-shrink: 0;
    min-width: 52px;
  }
  .relationship-item__id {
    color: var(--neo-ink);
    font-family: var(--neo-mono);
    font-size: 11.5px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
    flex: 1;
  }
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
    font-family: var(--neo-mono);
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
  .diff-stats { font-family: var(--neo-mono); font-size: 13px; gap: 6px; }
  :global(.diff-add) { color: var(--neo-success); }
  :global(.diff-del) { color: var(--neo-danger); }
  :global(.diff-mod) { color: #d4a045; }

  .reference-card {
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
  .reference-card:hover { background: var(--neo-card-hover); }
  .reference-card > :global(svg) { color: var(--neo-muted); flex-shrink: 0; margin-top: 2px; }
  .reference-card__body { min-width: 0; flex: 1; }
  .reference-card__head { color: var(--neo-ink); font-size: 12px; opacity: 0.7; }
  .reference-card__id {
    font-family: var(--neo-mono);
    font-weight: 600;
    color: var(--neo-ink);
    opacity: 1;
    word-break: break-all;
  }
  .reference-card__instr {
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
    font-family: var(--neo-mono);
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
    .composer textarea { min-height: 56px; padding: 10px 12px; /* keep 16px font-size for iOS no-zoom */ }
    .composer-actions { padding: 6px 8px; }
    .send-button,
    .interrupt-button { width: 30px; height: 30px; }
  }
</style>
