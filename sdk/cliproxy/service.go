// Package cliproxy provides the core service implementation for the CLI Proxy API.
// It includes service lifecycle management, authentication handling, file watching,
// and integration with various AI service providers through a unified interface.
package cliproxy

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/api"
	chatgptweb "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/redisqueue"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/watcher"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/wsrelay"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v7/sdk/access"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v7/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

// Service wraps the proxy server lifecycle so external programs can embed the CLI proxy.
// It manages the complete lifecycle including authentication, file watching, HTTP server,
// and integration with various AI service providers.
type Service struct {
	// cfg holds the current application configuration.
	cfg *config.Config

	// cfgMu protects concurrent access to the configuration.
	cfgMu sync.RWMutex

	// configUpdateMu serializes config updates across watcher + home.
	configUpdateMu sync.Mutex

	// shutdownStarted marks that Shutdown has begun, so late-starting subsystems
	// (e.g. the amp thread actor proxy) can stop themselves instead of leaking.
	shutdownStarted bool

	// configPath is the path to the configuration file.
	configPath string

	// tokenProvider handles loading token-based clients.
	tokenProvider TokenClientProvider

	// apiKeyProvider handles loading API key-based clients.
	apiKeyProvider APIKeyClientProvider

	// watcherFactory creates file watcher instances.
	watcherFactory WatcherFactory

	// hooks provides lifecycle callbacks.
	hooks Hooks

	// serverOptions contains additional server configuration options.
	serverOptions []api.ServerOption

	// server is the HTTP API server instance.
	server *api.Server

	// pprofServer manages the optional pprof HTTP debug server.
	pprofServer *pprofServer

	// ampThreadActorProxy serves Amp's localhost Rivet endpoint when Amp is routed through CLIProxyAPI.
	ampThreadActorProxy *ampThreadActorProxy

	// serverErr channel for server startup/shutdown errors.
	serverErr chan error

	// watcher handles file system monitoring.
	watcher *WatcherWrapper

	// watcherCancel cancels the watcher context.
	watcherCancel context.CancelFunc

	// authUpdates channel for authentication updates.
	authUpdates chan watcher.AuthUpdate

	// authQueueStop cancels the auth update queue processing.
	authQueueStop context.CancelFunc

	// authManager handles legacy authentication operations.
	authManager *sdkAuth.Manager

	// accessManager handles request authentication providers.
	accessManager *sdkaccess.Manager

	// coreManager handles core authentication and execution.
	coreManager *coreauth.Manager

	// shutdownOnce ensures shutdown is called only once.
	shutdownOnce sync.Once

	// wsGateway manages websocket Gemini providers.
	wsGateway *wsrelay.Manager

	homeClient *home.Client
	homeCancel context.CancelFunc

	chatGPTWebCatalogMu sync.Mutex
	chatGPTWebCatalogs  map[string]chatGPTWebCatalogEntry
	chatGPTWebRefreshMu sync.Mutex
	chatGPTWebRefreshCh chan struct{}
	// Capacity-one trigger channel so repeated config reloads coalesce
	// onto a single in-flight catalog refresh batch plus one pending rerun.
	chatGPTWebRefreshTrigger chan struct{}
	modelRefreshes           singleflight.Group
	modelRegistrationMu      sync.Mutex
	modelRegistrations       map[string]*modelRegistrationLock
	runtimeContextMu         sync.RWMutex
	runtimeContext           context.Context
}

var serviceShutdownTimeout = 30 * time.Second
var chatGPTWebCatalogCacheTTL = 5 * time.Minute
var chatGPTWebCatalogRetryInterval = 5 * time.Minute
var chatGPTWebRefreshConcurrency = 4

const chatGPTWebCatalogRefreshInterval = 3 * time.Hour

type chatGPTWebCatalogEntry struct {
	models        []*ModelInfo
	fingerprint   [32]byte
	fetchedAt     time.Time
	discovered    bool
	refreshFailed bool
}

type modelRegistrationLock struct {
	mu   sync.Mutex
	refs int
}

func newServiceShutdownContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), serviceShutdownTimeout)
}

// RegisterUsagePlugin registers a usage plugin on the global usage manager.
// This allows external code to monitor API usage and token consumption.
//
// Parameters:
//   - plugin: The usage plugin to register
func (s *Service) RegisterUsagePlugin(plugin usage.Plugin) {
	usage.RegisterPlugin(plugin)
}

// newDefaultAuthManager creates a default authentication manager with all supported providers.
func newDefaultAuthManager() *sdkAuth.Manager {
	return sdkAuth.NewManager(
		sdkAuth.GetTokenStore(),
		sdkAuth.NewGeminiAuthenticator(),
		sdkAuth.NewCodexAuthenticator(),
		sdkAuth.NewClaudeAuthenticator(),
	)
}

func (s *Service) ensureAuthUpdateQueue(ctx context.Context) {
	if s == nil {
		return
	}
	if s.authUpdates == nil {
		s.authUpdates = make(chan watcher.AuthUpdate, 256)
	}
	if s.authQueueStop != nil {
		return
	}
	queueCtx, cancel := context.WithCancel(ctx)
	s.authQueueStop = cancel
	go s.consumeAuthUpdates(queueCtx)
}

func (s *Service) consumeAuthUpdates(ctx context.Context) {
	ctx = coreauth.WithWatcherReplay(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-s.authUpdates:
			if !ok {
				return
			}
			s.handleAuthUpdate(ctx, update)
		labelDrain:
			for {
				select {
				case nextUpdate := <-s.authUpdates:
					s.handleAuthUpdate(ctx, nextUpdate)
				default:
					break labelDrain
				}
			}
		}
	}
}

func (s *Service) emitAuthUpdate(ctx context.Context, update watcher.AuthUpdate) {
	if s == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.watcher != nil && s.watcher.DispatchRuntimeAuthUpdate(update) {
		return
	}
	if s.authUpdates != nil {
		select {
		case s.authUpdates <- update:
			return
		default:
			log.Debugf("auth update queue saturated, applying inline action=%v id=%s", update.Action, update.ID)
		}
	}
	s.handleAuthUpdate(ctx, update)
}

func (s *Service) handleAuthUpdate(ctx context.Context, update watcher.AuthUpdate) {
	if s == nil {
		return
	}
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	if cfg == nil || s.coreManager == nil {
		return
	}
	switch update.Action {
	case watcher.AuthUpdateActionAdd, watcher.AuthUpdateActionModify:
		if update.Auth == nil || update.Auth.ID == "" {
			return
		}
		s.applyCoreAuthAddOrUpdate(ctx, update.Auth)
	case watcher.AuthUpdateActionDelete:
		id := update.ID
		if id == "" && update.Auth != nil {
			id = update.Auth.ID
		}
		if id == "" {
			return
		}
		s.applyCoreAuthRemoval(ctx, id)
	default:
		log.Debugf("received unknown auth update action: %v", update.Action)
	}
}

func (s *Service) ensureWebsocketGateway() {
	if s == nil {
		return
	}
	if s.wsGateway != nil {
		return
	}
	opts := wsrelay.Options{
		Path:           "/v1/ws",
		OnConnected:    s.wsOnConnected,
		OnDisconnected: s.wsOnDisconnected,
		LogDebugf:      log.Debugf,
		LogInfof:       log.Infof,
		LogWarnf:       log.Warnf,
	}
	s.wsGateway = wsrelay.NewManager(opts)
}

func (s *Service) wsOnConnected(channelID string) {
	if s == nil || channelID == "" {
		return
	}
	if !strings.HasPrefix(strings.ToLower(channelID), "aistudio-") {
		return
	}
	if s.coreManager != nil {
		if existing, ok := s.coreManager.GetByID(channelID); ok && existing != nil {
			if !existing.Disabled && existing.Status == coreauth.StatusActive {
				return
			}
		}
	}
	now := time.Now().UTC()
	auth := &coreauth.Auth{
		ID:         channelID,  // keep channel identifier as ID
		Provider:   "aistudio", // logical provider for switch routing
		Label:      channelID,  // display original channel id
		Status:     coreauth.StatusActive,
		CreatedAt:  now,
		UpdatedAt:  now,
		Attributes: map[string]string{"runtime_only": "true"},
		Metadata:   map[string]any{"email": channelID}, // metadata drives logging and usage tracking
	}
	log.Infof("websocket provider connected: %s", channelID)
	s.emitAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionAdd,
		ID:     auth.ID,
		Auth:   auth,
	})
}

func (s *Service) wsOnDisconnected(channelID string, reason error) {
	if s == nil || channelID == "" {
		return
	}
	if reason != nil {
		if strings.Contains(reason.Error(), "replaced by new connection") {
			log.Infof("websocket provider replaced: %s", channelID)
			return
		}
		log.Warnf("websocket provider disconnected: %s (%v)", channelID, reason)
	} else {
		log.Infof("websocket provider disconnected: %s", channelID)
	}
	ctx := context.Background()
	s.emitAuthUpdate(ctx, watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionDelete,
		ID:     channelID,
	})
}

func (s *Service) applyCoreAuthAddOrUpdate(ctx context.Context, auth *coreauth.Auth) {
	if s == nil || s.coreManager == nil || auth == nil || auth.ID == "" {
		return
	}
	auth = auth.Clone()
	unlockRegistration := s.lockModelRegistration(auth.ID)
	defer unlockRegistration()
	s.ensureExecutorsForAuth(auth)

	// IMPORTANT: Update coreManager FIRST, before model registration.
	// This ensures that configuration changes (proxy_url, prefix, etc.) take effect
	// immediately for API calls, rather than waiting for model registration to complete.
	op := "register"
	var err error
	var merged *coreauth.Auth
	if existing, ok := s.coreManager.GetByID(auth.ID); ok {
		auth.CreatedAt = existing.CreatedAt
		if !existing.Disabled && existing.Status != coreauth.StatusDisabled && !auth.Disabled && auth.Status != coreauth.StatusDisabled {
			auth.LastRefreshedAt = existing.LastRefreshedAt
			auth.NextRefreshAfter = existing.NextRefreshAfter
			if len(auth.ModelStates) == 0 && len(existing.ModelStates) > 0 {
				auth.ModelStates = existing.ModelStates
			}
		}
		op = "update"
		merged, err = s.coreManager.Update(ctx, auth)
	} else {
		merged, err = s.coreManager.Register(ctx, auth)
	}
	if err != nil {
		log.Errorf("failed to %s auth %s: %v", op, auth.ID, err)
		current, ok := s.coreManager.GetByID(auth.ID)
		if !ok || current.Disabled {
			GlobalModelRegistry().UnregisterClient(auth.ID)
			return
		}
		auth = current
	} else if merged != nil {
		// Update may preserve newer runtime metadata from a concurrent write;
		// use the manager-returned snapshot so downstream catalog/model state
		// matches what the manager actually stored.
		auth = merged
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
		s.prepareChatGPTWebCatalogForAuth(auth)
	} else {
		s.chatGPTWebCatalogMu.Lock()
		delete(s.chatGPTWebCatalogs, auth.ID)
		s.chatGPTWebCatalogMu.Unlock()
	}

	// Register models after auth is updated in coreManager.
	s.registerModelsForAuth(auth)
	s.coreManager.ReconcileRegistryModelStates(ctx, auth.ID)

	// Refresh the scheduler entry so that the auth's supportedModelSet is rebuilt
	// from the now-populated global model registry. Without this, newly added auths
	// have an empty supportedModelSet (because Register/Update upserts into the
	// scheduler before registerModelsForAuth runs) and are invisible to the scheduler.
	s.coreManager.RefreshSchedulerEntry(auth.ID)
	if !auth.Disabled && auth.Status != coreauth.StatusDisabled && strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
		refreshCtx := s.chatGPTWebRefreshContext(ctx)
		if refreshCtx != nil {
			s.queueModelRegistrationRefresh(refreshCtx, auth.Clone())
		}
	}
}

func (s *Service) applyCoreAuthRemoval(ctx context.Context, id string) {
	if s == nil || id == "" {
		return
	}
	if s.coreManager == nil {
		return
	}
	unlockRegistration := s.lockModelRegistration(id)
	defer unlockRegistration()
	s.chatGPTWebCatalogMu.Lock()
	delete(s.chatGPTWebCatalogs, id)
	s.chatGPTWebCatalogMu.Unlock()
	GlobalModelRegistry().UnregisterClient(id)
	if existing, ok := s.coreManager.GetByID(id); ok && existing != nil {
		existing.Disabled = true
		existing.Status = coreauth.StatusDisabled
		if _, err := s.coreManager.Update(ctx, existing); err != nil {
			log.Errorf("failed to disable auth %s: %v", id, err)
		}
		if strings.EqualFold(strings.TrimSpace(existing.Provider), "codex") {
			executor.CloseCodexWebsocketSessionsForAuthID(existing.ID, "auth_removed")
			s.ensureExecutorsForAuth(existing)
		}
	}
}

func (s *Service) lockModelRegistration(authID string) func() {
	s.modelRegistrationMu.Lock()
	if s.modelRegistrations == nil {
		s.modelRegistrations = make(map[string]*modelRegistrationLock)
	}
	lock := s.modelRegistrations[authID]
	if lock == nil {
		lock = &modelRegistrationLock{}
		s.modelRegistrations[authID] = lock
	}
	lock.refs++
	s.modelRegistrationMu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		s.modelRegistrationMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(s.modelRegistrations, authID)
		}
		s.modelRegistrationMu.Unlock()
	}
}

func (s *Service) applyRetryConfig(cfg *config.Config) {
	if s == nil || s.coreManager == nil || cfg == nil {
		return
	}
	maxInterval := time.Duration(cfg.MaxRetryInterval) * time.Second
	s.coreManager.SetRetryConfig(cfg.RequestRetry, maxInterval, cfg.MaxRetryCredentials)
}

func openAICompatInfoFromAuth(a *coreauth.Auth) (providerKey string, compatName string, ok bool) {
	if a == nil {
		return "", "", false
	}
	if len(a.Attributes) > 0 {
		providerKey = strings.TrimSpace(a.Attributes["provider_key"])
		compatName = strings.TrimSpace(a.Attributes["compat_name"])
		if compatName != "" {
			if providerKey == "" {
				providerKey = compatName
			}
			return strings.ToLower(providerKey), compatName, true
		}
	}
	if strings.EqualFold(strings.TrimSpace(a.Provider), "openai-compatibility") {
		return "openai-compatibility", strings.TrimSpace(a.Label), true
	}
	return "", "", false
}

func (s *Service) ensureExecutorsForAuth(a *coreauth.Auth) {
	s.ensureExecutorsForAuthWithMode(a, false)
}

func (s *Service) ensureExecutorsForAuthWithMode(a *coreauth.Auth, forceReplace bool) {
	if s == nil || s.coreManager == nil || a == nil {
		return
	}
	if strings.EqualFold(strings.TrimSpace(a.Provider), "codex") {
		if !forceReplace {
			existingExecutor, hasExecutor := s.coreManager.Executor("codex")
			if hasExecutor {
				_, isCodexAutoExecutor := existingExecutor.(*executor.CodexAutoExecutor)
				if isCodexAutoExecutor {
					return
				}
			}
		}
		s.coreManager.RegisterExecutor(executor.NewCodexAutoExecutor(s.cfg))
		return
	}
	// Skip disabled auth entries when (re)binding executors.
	// Disabled auths can linger during config reloads (e.g., removed OpenAI-compat entries)
	// and must not override active provider executors.
	if a.Disabled {
		return
	}
	if compatProviderKey, _, isCompat := openAICompatInfoFromAuth(a); isCompat {
		if compatProviderKey == "" {
			compatProviderKey = strings.ToLower(strings.TrimSpace(a.Provider))
		}
		if compatProviderKey == "" {
			compatProviderKey = "openai-compatibility"
		}
		s.coreManager.RegisterExecutor(executor.NewOpenAICompatExecutor(compatProviderKey, s.cfg))
		return
	}
	switch strings.ToLower(strings.TrimSpace(a.Provider)) {
	case "gemini":
		s.coreManager.RegisterExecutor(executor.NewGeminiExecutor(s.cfg))
	case "vertex":
		s.coreManager.RegisterExecutor(executor.NewGeminiVertexExecutor(s.cfg))
	case "gemini-cli":
		s.coreManager.RegisterExecutor(executor.NewGeminiCLIExecutor(s.cfg))
	case "aistudio":
		if s.wsGateway != nil {
			s.coreManager.RegisterExecutor(executor.NewAIStudioExecutor(s.cfg, a.ID, s.wsGateway))
		}
		return
	case "antigravity":
		s.coreManager.RegisterExecutor(executor.NewAntigravityExecutor(s.cfg))
	case "claude":
		s.coreManager.RegisterExecutor(executor.NewClaudeExecutor(s.cfg))
	case "kimi":
		s.coreManager.RegisterExecutor(executor.NewKimiExecutor(s.cfg))
	case "chatgpt-web":
		if !forceReplace {
			existingExecutor, hasExecutor := s.coreManager.Executor("chatgpt-web")
			if hasExecutor {
				if _, isChatGPTWebExecutor := existingExecutor.(*executor.ChatGPTWebExecutor); isChatGPTWebExecutor {
					return
				}
			}
		}
		s.coreManager.RegisterExecutor(executor.NewChatGPTWebExecutor(s.cfg, s.coreManager.UpdateMetadata))
	default:
		providerKey := strings.ToLower(strings.TrimSpace(a.Provider))
		if providerKey == "" {
			providerKey = "openai-compatibility"
		}
		s.coreManager.RegisterExecutor(executor.NewOpenAICompatExecutor(providerKey, s.cfg))
	}
}

func (s *Service) registerResolvedModelsForAuth(a *coreauth.Auth, providerKey string, models []*ModelInfo) {
	if a == nil || a.ID == "" {
		return
	}
	if len(models) == 0 {
		GlobalModelRegistry().UnregisterClient(a.ID)
		return
	}
	GlobalModelRegistry().RegisterClient(a.ID, providerKey, models)
}

// rebindExecutors refreshes provider executors so they observe the latest configuration.
func (s *Service) rebindExecutors() {
	if s == nil || s.coreManager == nil {
		return
	}
	auths := s.coreManager.List()
	reboundCodex := false
	for _, auth := range auths {
		if auth != nil && strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
			if reboundCodex {
				continue
			}
			reboundCodex = true
		}
		s.ensureExecutorsForAuthWithMode(auth, true)
	}
}

func (s *Service) applyConfigUpdate(newCfg *config.Config) {
	if s == nil {
		return
	}

	s.configUpdateMu.Lock()
	defer s.configUpdateMu.Unlock()

	previousStrategy := ""
	var previousSessionAffinity bool
	var previousSessionAffinityTTL string
	s.cfgMu.RLock()
	if s.cfg != nil {
		previousStrategy = strings.ToLower(strings.TrimSpace(s.cfg.Routing.Strategy))
		previousSessionAffinity = s.cfg.Routing.SessionAffinity
		previousSessionAffinityTTL = s.cfg.Routing.SessionAffinityTTL
	}
	s.cfgMu.RUnlock()

	if newCfg == nil {
		s.cfgMu.RLock()
		newCfg = s.cfg
		s.cfgMu.RUnlock()
	}
	if newCfg == nil {
		return
	}

	nextStrategy := strings.ToLower(strings.TrimSpace(newCfg.Routing.Strategy))
	normalizeStrategy := func(strategy string) string {
		switch strategy {
		case "fill-first", "fillfirst", "ff":
			return "fill-first"
		default:
			return "round-robin"
		}
	}
	previousStrategy = normalizeStrategy(previousStrategy)
	nextStrategy = normalizeStrategy(nextStrategy)

	nextSessionAffinity := newCfg.Routing.SessionAffinity
	nextSessionAffinityTTL := newCfg.Routing.SessionAffinityTTL

	selectorChanged := previousStrategy != nextStrategy ||
		previousSessionAffinity != nextSessionAffinity ||
		previousSessionAffinityTTL != nextSessionAffinityTTL

	if s.coreManager != nil && selectorChanged {
		var selector coreauth.Selector
		switch nextStrategy {
		case "fill-first":
			selector = &coreauth.FillFirstSelector{}
		default:
			selector = &coreauth.RoundRobinSelector{}
		}

		if nextSessionAffinity {
			ttl := time.Hour
			if ttlStr := strings.TrimSpace(nextSessionAffinityTTL); ttlStr != "" {
				if parsed, err := time.ParseDuration(ttlStr); err == nil && parsed > 0 {
					ttl = parsed
				}
			}
			selector = coreauth.NewSessionAffinitySelectorWithConfig(coreauth.SessionAffinityConfig{
				Fallback: selector,
				TTL:      ttl,
			})
		}

		s.coreManager.SetSelector(selector)
	}

	s.applyRetryConfig(newCfg)
	s.applyPprofConfig(newCfg)
	s.runtimeContextMu.RLock()
	runtimeCtx := s.runtimeContext
	s.runtimeContextMu.RUnlock()
	s.applyAmpThreadActorProxyConfig(runtimeCtx, newCfg)
	if s.server != nil {
		s.server.UpdateClients(newCfg)
	}
	s.cfgMu.Lock()
	s.cfg = newCfg
	s.cfgMu.Unlock()
	if s.coreManager != nil {
		s.coreManager.SetConfig(newCfg)
		s.coreManager.SetOAuthModelAlias(newCfg.OAuthModelAlias)
	}
	s.rebindExecutors()
	s.triggerChatGPTWebCatalogRefresh()
}

// triggerChatGPTWebCatalogRefresh asks the service-owned refresh coordinator
// to re-discover ChatGPT web catalogs against the latest config. Triggers
// coalesce: one batch runs at a time and at most one rerun stays pending, so
// rapid hot reloads cannot accumulate refresh goroutines or queued upstream
// work. Each batch snapshots eligible auths when it starts, so a coalesced
// rerun still observes the newest config.
func (s *Service) triggerChatGPTWebCatalogRefresh() {
	if s == nil || s.coreManager == nil {
		return
	}
	refreshCtx := s.chatGPTWebRefreshContext(nil)
	if refreshCtx == nil {
		return
	}
	s.chatGPTWebRefreshMu.Lock()
	trigger := s.chatGPTWebRefreshTrigger
	if trigger == nil {
		trigger = make(chan struct{}, 1)
		s.chatGPTWebRefreshTrigger = trigger
		go func() {
			s.runChatGPTWebRefreshCoordinator(refreshCtx, trigger)
			s.chatGPTWebRefreshMu.Lock()
			if s.chatGPTWebRefreshTrigger == trigger {
				s.chatGPTWebRefreshTrigger = nil
			}
			s.chatGPTWebRefreshMu.Unlock()
		}()
	}
	s.chatGPTWebRefreshMu.Unlock()
	select {
	case trigger <- struct{}{}:
	default:
	}
}

// runChatGPTWebRefreshCoordinator drains refresh triggers until the service
// shuts down. A trigger consumed while a batch is running schedules exactly
// one rerun against a fresh eligibility snapshot.
func (s *Service) runChatGPTWebRefreshCoordinator(ctx context.Context, trigger <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-trigger:
		}
		eligible := make([]*coreauth.Auth, 0)
		for _, auth := range s.coreManager.List() {
			if auth == nil || auth.Disabled || auth.Status == coreauth.StatusDisabled || !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
				continue
			}
			s.prepareChatGPTWebCatalogForAuth(auth)
			eligible = append(eligible, auth)
		}
		s.runBoundedChatGPTWebRefreshes(ctx, eligible)
	}
}

func forceHomeRuntimeConfig(cfg *config.Config) {
	if cfg == nil {
		return
	}
	cfg.APIKeys = nil
	cfg.UsageStatisticsEnabled = true
	cfg.DisableCooling = true
	cfg.WebsocketAuth = false
	cfg.EnableGeminiCLIEndpoint = false
	cfg.RemoteManagement.AllowRemote = false
	cfg.RemoteManagement.DisableControlPanel = true
}

func (s *Service) registerHomeExecutors() {
	if s == nil || s.coreManager == nil || s.cfg == nil {
		return
	}

	// Register baseline executors so home-dispatched auth entries can execute without
	// requiring any local auth-dir credentials.
	s.coreManager.RegisterExecutor(executor.NewCodexAutoExecutor(s.cfg))
	s.coreManager.RegisterExecutor(executor.NewClaudeExecutor(s.cfg))
	s.coreManager.RegisterExecutor(executor.NewGeminiExecutor(s.cfg))
	s.coreManager.RegisterExecutor(executor.NewGeminiVertexExecutor(s.cfg))
	s.coreManager.RegisterExecutor(executor.NewGeminiCLIExecutor(s.cfg))
	s.coreManager.RegisterExecutor(executor.NewAIStudioExecutor(s.cfg, "", s.wsGateway))
	s.coreManager.RegisterExecutor(executor.NewAntigravityExecutor(s.cfg))
	s.coreManager.RegisterExecutor(executor.NewKimiExecutor(s.cfg))
	s.coreManager.RegisterExecutor(executor.NewOpenAICompatExecutor("openai-compatibility", s.cfg))
}

func (s *Service) applyHomeOverlay(remoteCfg *config.Config) {
	if s == nil || remoteCfg == nil {
		return
	}

	s.cfgMu.RLock()
	baseCfg := s.cfg
	s.cfgMu.RUnlock()
	if baseCfg == nil {
		return
	}

	merged := *remoteCfg
	merged.Host = baseCfg.Host
	merged.Port = baseCfg.Port
	merged.TLS = baseCfg.TLS
	merged.Home = baseCfg.Home
	forceHomeRuntimeConfig(&merged)

	s.applyConfigUpdate(&merged)
}

func (s *Service) startHomeUsageForwarder(ctx context.Context, client *home.Client) {
	if s == nil || client == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	sleep := func(d time.Duration) bool {
		if d <= 0 {
			return true
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if !client.HeartbeatOK() {
				if !sleep(time.Second) {
					return
				}
				continue
			}

			items := redisqueue.PopOldest(64)
			if len(items) == 0 {
				if !sleep(500 * time.Millisecond) {
					return
				}
				continue
			}

			for i := range items {
				if errPush := client.LPushUsage(ctx, items[i]); errPush != nil {
					for j := i; j < len(items); j++ {
						redisqueue.Enqueue(items[j])
					}
					if !sleep(time.Second) {
						return
					}
					break
				}
			}
		}
	}()
}

func (s *Service) startHomeSubscriber(ctx context.Context) {
	if s == nil {
		return
	}
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	if cfg == nil || !cfg.Home.Enabled {
		return
	}

	if s.homeCancel != nil {
		s.homeCancel()
		s.homeCancel = nil
	}
	if s.homeClient != nil {
		s.homeClient.Close()
		s.homeClient = nil
	}

	homeCtx := ctx
	if homeCtx == nil {
		homeCtx = context.Background()
	}
	homeCtx, cancel := context.WithCancel(homeCtx)
	s.homeCancel = cancel

	client := home.New(cfg.Home)
	s.homeClient = client
	home.SetCurrent(client)

	go client.StartConfigSubscriber(homeCtx, func(raw []byte) error {
		parsed, err := config.ParseConfigBytes(raw)
		if err != nil {
			log.Warnf("failed to parse home config payload: %v", err)
			return err
		}
		s.applyHomeOverlay(parsed)
		return nil
	})
	s.startHomeUsageForwarder(homeCtx, client)
}

// Run starts the service and blocks until the context is cancelled or the server stops.
// It initializes all components including authentication, file watching, HTTP server,
// and starts processing requests. The method blocks until the context is cancelled.
//
// Parameters:
//   - ctx: The context for controlling the service lifecycle
//
// Returns:
//   - error: An error if the service fails to start or run
func (s *Service) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("cliproxy: service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, runCancel := context.WithCancel(ctx)
	var chatGPTWebCatalogDone <-chan struct{}
	s.runtimeContextMu.Lock()
	s.runtimeContext = ctx
	s.runtimeContextMu.Unlock()

	usage.StartDefault(ctx)
	homeEnabled := s.cfg != nil && s.cfg.Home.Enabled
	if homeEnabled {
		forceHomeRuntimeConfig(s.cfg)
		redisqueue.SetUsageStatisticsEnabled(true)
	}

	defer func() {
		runCancel()
		if chatGPTWebCatalogDone != nil {
			<-chatGPTWebCatalogDone
		}
		shutdownCtx, shutdownCancel := newServiceShutdownContext()
		defer shutdownCancel()
		if err := s.Shutdown(shutdownCtx); err != nil {
			log.Errorf("service shutdown returned error: %v", err)
		}
	}()

	if !homeEnabled {
		if errEnsureAuthDir := s.ensureAuthDir(); errEnsureAuthDir != nil {
			return errEnsureAuthDir
		}
	}

	s.applyRetryConfig(s.cfg)

	if s.coreManager != nil && !homeEnabled {
		if errLoad := s.coreManager.Load(ctx); errLoad != nil {
			log.Warnf("failed to load auth store: %v", errLoad)
		}
	}

	if !homeEnabled {
		tokenResult, err := s.tokenProvider.Load(ctx, s.cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		if tokenResult == nil {
			tokenResult = &TokenClientResult{}
		}

		apiKeyResult, err := s.apiKeyProvider.Load(ctx, s.cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		if apiKeyResult == nil {
			apiKeyResult = &APIKeyClientResult{}
		}
	}

	// legacy clients removed; no caches to refresh

	// handlers no longer depend on legacy clients; pass nil slice initially
	s.server = api.NewServer(s.cfg, s.coreManager, s.accessManager, s.configPath, s.serverOptions...)

	if s.authManager == nil {
		s.authManager = newDefaultAuthManager()
	}

	if homeEnabled {
		s.startHomeSubscriber(ctx)
	}

	s.ensureWebsocketGateway()
	if s.server != nil && s.wsGateway != nil {
		s.server.AttachWebsocketRoute(s.wsGateway.Path(), s.wsGateway.Handler())
		s.server.SetWebsocketAuthChangeHandler(func(oldEnabled, newEnabled bool) {
			if oldEnabled == newEnabled {
				return
			}
			if !oldEnabled && newEnabled {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if errStop := s.wsGateway.Stop(ctx); errStop != nil {
					log.Warnf("failed to reset websocket connections after ws-auth change %t -> %t: %v", oldEnabled, newEnabled, errStop)
					return
				}
				log.Debugf("ws-auth enabled; existing websocket sessions terminated to enforce authentication")
				return
			}
			log.Debugf("ws-auth disabled; existing websocket sessions remain connected")
		})
	}

	if homeEnabled {
		s.registerHomeExecutors()
		// Home mode does not expose in-process Redis RESP usage output; usage is forwarded to home instead.
		redisqueue.SetEnabled(true)
	}

	if s.hooks.OnBeforeStart != nil {
		s.hooks.OnBeforeStart(s.cfg)
	}

	// Register callback for startup and periodic model catalog refresh.
	// When remote model definitions change, re-register models for affected providers.
	// This intentionally rebuilds per-auth model availability from the latest catalog
	// snapshot instead of preserving prior registry suppression state.
	registry.SetModelRefreshCallback(func(changedProviders []string) {
		if s == nil || s.coreManager == nil || len(changedProviders) == 0 {
			return
		}

		providerSet := make(map[string]bool, len(changedProviders))
		for _, p := range changedProviders {
			providerSet[strings.ToLower(strings.TrimSpace(p))] = true
		}

		auths := s.coreManager.List()
		refreshed := 0
		for _, item := range auths {
			if item == nil || item.ID == "" {
				continue
			}
			auth, ok := s.coreManager.GetByID(item.ID)
			if !ok || auth == nil || auth.Disabled {
				continue
			}
			provider := strings.ToLower(strings.TrimSpace(auth.Provider))
			if !providerSet[provider] {
				continue
			}
			if s.refreshModelRegistrationForAuth(ctx, auth) {
				refreshed++
			}
		}

		if refreshed > 0 {
			log.Infof("re-registered models for %d auth(s) due to model catalog changes: %v", refreshed, changedProviders)
		}
	})
	catalogDone := make(chan struct{})
	chatGPTWebCatalogDone = catalogDone
	go func() {
		defer close(catalogDone)
		s.runChatGPTWebCatalogUpdater(ctx)
	}()

	s.serverErr = make(chan error, 1)
	go func() {
		if errStart := s.server.Start(); errStart != nil {
			s.serverErr <- errStart
		} else {
			s.serverErr <- nil
		}
	}()

	time.Sleep(100 * time.Millisecond)
	fmt.Printf("API server started successfully on: %s:%d\n", s.cfg.Host, s.cfg.Port)

	s.applyPprofConfig(s.cfg)
	s.startAmpThreadActorProxy(ctx, s.cfg)

	if s.hooks.OnAfterStart != nil {
		s.hooks.OnAfterStart(s)
	}

	if !homeEnabled {
		var watcherWrapper *WatcherWrapper
		reloadCallback := func(newCfg *config.Config) { s.applyConfigUpdate(newCfg) }

		watcherWrapper, errCreate := s.watcherFactory(s.configPath, s.cfg.AuthDir, reloadCallback)
		if errCreate != nil {
			return fmt.Errorf("cliproxy: failed to create watcher: %w", errCreate)
		}
		s.watcher = watcherWrapper
		s.ensureAuthUpdateQueue(ctx)
		if s.authUpdates != nil {
			watcherWrapper.SetAuthUpdateQueue(s.authUpdates)
		}
		watcherWrapper.SetConfig(s.cfg)

		watcherCtx, watcherCancel := context.WithCancel(context.Background())
		s.watcherCancel = watcherCancel
		if errStart := watcherWrapper.Start(watcherCtx); errStart != nil {
			return fmt.Errorf("cliproxy: failed to start watcher: %w", errStart)
		}
		log.Info("file watcher started for config and auth directory changes")
	}

	// Prefer core auth manager auto refresh if available.
	if s.coreManager != nil && !homeEnabled {
		interval := 15 * time.Minute
		s.coreManager.StartAutoRefresh(context.Background(), interval)
		log.Infof("core auth auto-refresh started (interval=%s)", interval)
	}

	select {
	case <-ctx.Done():
		log.Debug("service context cancelled, shutting down...")
		return ctx.Err()
	case errServer := <-s.serverErr:
		return errServer
	}
}

// Shutdown gracefully stops background workers and the HTTP server.
// It ensures all resources are properly cleaned up and connections are closed.
// The shutdown is idempotent and can be called multiple times safely.
//
// Parameters:
//   - ctx: The context for controlling the shutdown timeout
//
// Returns:
//   - error: An error if shutdown fails
func (s *Service) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	var shutdownErr error
	s.shutdownOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}

		s.configUpdateMu.Lock()
		s.shutdownStarted = true
		s.configUpdateMu.Unlock()

		if s.server != nil {
			s.server.ClosePublicListeners()
			if err := s.server.ShutdownAmpModule(ctx); err != nil {
				log.Errorf("failed to stop amp module: %v", err)
				shutdownErr = err
			}
		}

		if s.homeCancel != nil {
			s.homeCancel()
			s.homeCancel = nil
		}
		if s.homeClient != nil {
			s.homeClient.Close()
			s.homeClient = nil
		}
		home.ClearCurrent()

		// legacy refresh loop removed; only stopping core auth manager below

		if s.watcherCancel != nil {
			s.watcherCancel()
		}
		if s.coreManager != nil {
			s.coreManager.StopAutoRefresh()
		}
		if s.watcher != nil {
			if err := s.watcher.Stop(); err != nil {
				log.Errorf("failed to stop file watcher: %v", err)
				shutdownErr = err
			}
		}
		if s.wsGateway != nil {
			if err := s.wsGateway.Stop(ctx); err != nil {
				log.Errorf("failed to stop websocket gateway: %v", err)
				if shutdownErr == nil {
					shutdownErr = err
				}
			}
		}
		if s.authQueueStop != nil {
			s.authQueueStop()
			s.authQueueStop = nil
		}

		if errShutdownPprof := s.shutdownPprof(ctx); errShutdownPprof != nil {
			log.Errorf("failed to stop pprof server: %v", errShutdownPprof)
			if shutdownErr == nil {
				shutdownErr = errShutdownPprof
			}
		}

		s.configUpdateMu.Lock()
		actorProxy := s.ampThreadActorProxy
		s.ampThreadActorProxy = nil
		s.configUpdateMu.Unlock()
		if actorProxy != nil {
			if errShutdownActorProxy := actorProxy.Shutdown(ctx); errShutdownActorProxy != nil {
				log.Errorf("failed to stop amp thread actor proxy: %v", errShutdownActorProxy)
				if shutdownErr == nil {
					shutdownErr = errShutdownActorProxy
				}
			}
		}

		// no legacy clients to persist

		if s.server != nil {
			shutdownCtx, cancel := context.WithTimeout(ctx, serviceShutdownTimeout)
			defer cancel()
			if err := s.server.Stop(shutdownCtx); err != nil {
				log.Errorf("error stopping API server: %v", err)
				if shutdownErr == nil {
					shutdownErr = err
				}
			}
		}

		usage.StopDefault()
	})
	return shutdownErr
}

func (s *Service) ensureAuthDir() error {
	info, err := os.Stat(s.cfg.AuthDir)
	if err != nil {
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(s.cfg.AuthDir, 0o755); mkErr != nil {
				return fmt.Errorf("cliproxy: failed to create auth directory %s: %w", s.cfg.AuthDir, mkErr)
			}
			log.Infof("created missing auth directory: %s", s.cfg.AuthDir)
			return nil
		}
		return fmt.Errorf("cliproxy: error checking auth directory %s: %w", s.cfg.AuthDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("cliproxy: auth path exists but is not a directory: %s", s.cfg.AuthDir)
	}
	return nil
}

func (s *Service) runChatGPTWebCatalogUpdater(ctx context.Context) {
	refreshTicker := time.NewTicker(chatGPTWebCatalogRefreshInterval)
	retryTicker := time.NewTicker(chatGPTWebCatalogRetryInterval)
	defer refreshTicker.Stop()
	defer retryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-refreshTicker.C:
			refreshed, discovered := s.refreshChatGPTWebModelRegistrations(ctx)
			if refreshed > 0 {
				stale := refreshed - discovered
				if stale < 0 {
					stale = 0
				}
				log.Infof("refreshed ChatGPT web model catalogs for %d auth(s) (%d discovered, %d stale/fallback)", refreshed, discovered, stale)
			}
		case <-retryTicker.C:
			s.retryFailedChatGPTWebModelRegistrations(ctx)
		}
	}
}

func (s *Service) retryFailedChatGPTWebModelRegistrations(ctx context.Context) int {
	if s == nil || s.coreManager == nil {
		return 0
	}
	now := time.Now()
	eligible := make([]*coreauth.Auth, 0)
	for _, auth := range s.coreManager.List() {
		if auth == nil || auth.Disabled || auth.Status == coreauth.StatusDisabled || !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
			continue
		}
		fingerprint := s.chatGPTWebAuthFingerprint(auth)
		s.chatGPTWebCatalogMu.Lock()
		cached, ok := s.chatGPTWebCatalogs[auth.ID]
		retry := ok && cached.fingerprint == fingerprint && cached.refreshFailed && now.Sub(cached.fetchedAt) >= chatGPTWebCatalogRetryInterval
		s.chatGPTWebCatalogMu.Unlock()
		if retry {
			eligible = append(eligible, auth)
		}
	}
	return s.runBoundedChatGPTWebRefreshes(ctx, eligible)
}

func (s *Service) refreshChatGPTWebModelRegistrations(ctx context.Context) (refreshed, discovered int) {
	if s == nil || s.coreManager == nil {
		return 0, 0
	}
	eligible := make([]*coreauth.Auth, 0)
	for _, auth := range s.coreManager.List() {
		if auth == nil || auth.Disabled || auth.Status == coreauth.StatusDisabled || !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
			continue
		}
		eligible = append(eligible, auth)
	}
	refreshed = s.runBoundedChatGPTWebRefreshes(ctx, eligible)
	for _, auth := range eligible {
		fingerprint := s.chatGPTWebAuthFingerprint(auth)
		s.chatGPTWebCatalogMu.Lock()
		cached, ok := s.chatGPTWebCatalogs[auth.ID]
		if ok && cached.fingerprint == fingerprint && cached.discovered && !cached.refreshFailed {
			discovered++
		}
		s.chatGPTWebCatalogMu.Unlock()
	}
	return refreshed, discovered
}

// runBoundedChatGPTWebRefreshes refreshes the given auths concurrently
// through the shared concurrency bound and waits for the batch to finish.
func (s *Service) runBoundedChatGPTWebRefreshes(ctx context.Context, auths []*coreauth.Auth) int {
	if len(auths) == 0 {
		return 0
	}
	workers := chatGPTWebRefreshConcurrency
	if len(auths) < workers {
		workers = len(auths)
	}
	jobs := make(chan *coreauth.Auth)
	var wg sync.WaitGroup
	var refreshed atomic.Int64
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for a := range jobs {
				if ctx.Err() != nil {
					continue
				}
				if s.refreshModelRegistrationForAuth(ctx, a) {
					refreshed.Add(1)
				}
			}
		}()
	}
out:
	for _, auth := range auths {
		if ctx.Err() != nil {
			break
		}
		select {
		case jobs <- auth:
		case <-ctx.Done():
			break out
		}
	}
	close(jobs)
	wg.Wait()
	return int(refreshed.Load())
}

func (s *Service) chatGPTWebModelsForAuth(auth *coreauth.Auth) []*ModelInfo {
	fallback := registry.GetChatGPTWebModels()
	if s == nil || s.coreManager == nil || auth == nil || auth.ID == "" {
		return fallback
	}
	fingerprint := s.chatGPTWebAuthFingerprint(auth)

	s.chatGPTWebCatalogMu.Lock()
	cached, cachedOK := s.chatGPTWebCatalogs[auth.ID]
	if cachedOK && cached.fingerprint == fingerprint && cached.discovered {
		models := append([]*ModelInfo(nil), cached.models...)
		s.chatGPTWebCatalogMu.Unlock()
		return models
	}
	s.chatGPTWebCatalogMu.Unlock()
	return fallback
}

// chatGPTWebAuthFingerprint hashes the effective credential and proxy inputs
// for one auth using the service's current config.
func (s *Service) chatGPTWebAuthFingerprint(auth *coreauth.Auth) [32]byte {
	return chatGPTWebFingerprint(chatGPTWebCredentialFingerprint(auth), s.chatGPTWebEffectiveProxyURL(auth))
}

// chatGPTWebEffectiveProxyURL resolves the effective proxy URL for an auth
// under the service's current config.
func (s *Service) chatGPTWebEffectiveProxyURL(auth *coreauth.Auth) string {
	if auth != nil {
		if proxyURL := strings.TrimSpace(auth.ProxyURL); proxyURL != "" {
			return proxyURL
		}
	}
	if s == nil {
		return ""
	}
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	if cfg == nil {
		return ""
	}
	return strings.TrimSpace(cfg.ProxyURL)
}

func chatGPTWebCredentialFingerprint(auth *coreauth.Auth) [32]byte {
	if auth == nil {
		return [32]byte{}
	}
	// Mirror chatGPTWebCookie: a present-but-unusable cookie must not
	// suppress the session_token fallback.
	cookie := ""
	if v, ok := auth.Metadata["cookie"].(string); ok && v != "" {
		if minimized := chatgptweb.MinimizeCookie(v); minimized != "" {
			cookie = minimized
		}
	}
	if cookie == "" {
		if v, ok := auth.Metadata["session_token"].(string); ok && v != "" {
			cookie = chatgptweb.MinimizeCookie(v)
		}
	}
	// Match chatGPTWebUserAgent: default only when the key is absent; a
	// present-but-invalid value must invalidate the cache with its own
	// fingerprint instead of silently hashing the default.
	rawUA, uaExists := auth.Metadata["user_agent"]
	userAgent := chatgptweb.DefaultUserAgent
	if uaExists {
		if value, ok := rawUA.(string); ok {
			if normalized, err := chatgptweb.NormalizeUserAgent(value); err == nil {
				userAgent = normalized
			} else {
				userAgent = "\x00invalid-ua\x00" + value
			}
		} else {
			userAgent = "\x00invalid-ua\x00non-string"
		}
	}
	return sha256.Sum256([]byte(cookie + "\x00" + userAgent))
}

func chatGPTWebFingerprint(credential [32]byte, proxyURL string) [32]byte {
	return sha256.Sum256([]byte(string(credential[:]) + "\x00" + proxyURL))
}

func (s *Service) prepareChatGPTWebCatalogForAuth(auth *coreauth.Auth) {
	if s == nil || auth == nil || auth.ID == "" {
		return
	}
	fingerprint := s.chatGPTWebAuthFingerprint(auth)
	s.chatGPTWebCatalogMu.Lock()
	defer s.chatGPTWebCatalogMu.Unlock()
	if s.chatGPTWebCatalogs == nil {
		s.chatGPTWebCatalogs = make(map[string]chatGPTWebCatalogEntry)
	}
	if cached, ok := s.chatGPTWebCatalogs[auth.ID]; !ok || cached.fingerprint != fingerprint {
		s.chatGPTWebCatalogs[auth.ID] = chatGPTWebCatalogEntry{fingerprint: fingerprint}
	}
}

func (s *Service) chatGPTWebAuthMatchesFingerprint(authID string, fingerprint [32]byte) bool {
	if s == nil || s.coreManager == nil || authID == "" {
		return false
	}
	active, ok := s.coreManager.GetByID(authID)
	return ok && active != nil && !active.Disabled && active.Status != coreauth.StatusDisabled && strings.EqualFold(strings.TrimSpace(active.Provider), "chatgpt-web") && s.chatGPTWebAuthFingerprint(active) == fingerprint
}

func (s *Service) chatGPTWebRefreshContext(fallback context.Context) context.Context {
	if s == nil {
		return fallback
	}
	s.runtimeContextMu.RLock()
	runtimeCtx := s.runtimeContext
	s.runtimeContextMu.RUnlock()
	if runtimeCtx != nil {
		return runtimeCtx
	}
	return fallback
}

func (s *Service) modelRegistrationRefreshKey(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	key := auth.ID + "\x00" + provider
	if provider == "chatgpt-web" {
		fingerprint := s.chatGPTWebAuthFingerprint(auth)
		key += "\x00" + string(fingerprint[:])
	}
	return key
}

func (s *Service) queueModelRegistrationRefresh(ctx context.Context, auth *coreauth.Auth) {
	if s == nil || auth == nil || auth.ID == "" {
		return
	}
	key := s.modelRegistrationRefreshKey(auth)
	s.modelRefreshes.DoChan(key, func() (any, error) {
		return s.refreshModelRegistrationForAuthUncoalesced(ctx, auth), nil
	})
}

// acquireChatGPTWebRefreshSlot bounds concurrent ChatGPT web catalog
// discoveries so a config reload cannot fan out one upstream request per
// account at once.
func (s *Service) acquireChatGPTWebRefreshSlot(ctx context.Context) (func(), bool) {
	s.chatGPTWebRefreshMu.Lock()
	if s.chatGPTWebRefreshCh == nil {
		s.chatGPTWebRefreshCh = make(chan struct{}, chatGPTWebRefreshConcurrency)
	}
	ch := s.chatGPTWebRefreshCh
	s.chatGPTWebRefreshMu.Unlock()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, true
	case <-ctx.Done():
		return nil, false
	}
}

func (s *Service) refreshChatGPTWebModelsForAuth(ctx context.Context, auth *coreauth.Auth) []*ModelInfo {
	fallback := registry.GetChatGPTWebModels()
	if s == nil || s.coreManager == nil || auth == nil || auth.ID == "" {
		return fallback
	}
	fingerprint := s.chatGPTWebAuthFingerprint(auth)

	s.chatGPTWebCatalogMu.Lock()
	cached, cachedOK := s.chatGPTWebCatalogs[auth.ID]
	if !cachedOK || cached.fingerprint != fingerprint {
		if s.chatGPTWebCatalogs == nil {
			s.chatGPTWebCatalogs = make(map[string]chatGPTWebCatalogEntry)
		}
		cached = chatGPTWebCatalogEntry{fingerprint: fingerprint}
		s.chatGPTWebCatalogs[auth.ID] = cached
		cachedOK = false
	}
	if cachedOK && cached.fingerprint == fingerprint && time.Since(cached.fetchedAt) < chatGPTWebCatalogCacheTTL {
		models := append([]*ModelInfo(nil), cached.models...)
		s.chatGPTWebCatalogMu.Unlock()
		return models
	}
	s.chatGPTWebCatalogMu.Unlock()

	release, acquired := s.acquireChatGPTWebRefreshSlot(ctx)
	if !acquired {
		return fallback
	}
	defer release()

	providerExecutor, ok := s.coreManager.Executor("chatgpt-web")
	webExecutor, okWeb := providerExecutor.(*executor.ChatGPTWebExecutor)
	if !ok || !okWeb {
		return fallback
	}
	// Capture the credential and proxy fingerprint before the blocking
	// discovery so a config update that happens mid-fetch cannot let a
	// stale result publish under the new proxy generation. The proxy comes
	// from the executor's own config snapshot so the fingerprint matches
	// the proxy DiscoverModels will actually use.
	startCredential := chatGPTWebCredentialFingerprint(auth)
	startProxyURL := webExecutor.EffectiveProxyURL(auth)
	startFingerprint := chatGPTWebFingerprint(startCredential, startProxyURL)
	models, discoveredAuth, err := webExecutor.DiscoverModels(ctx, auth)
	// Reject the result unless the executor's effective proxy still matches
	// the starting value; a catalog fetched through a stale proxy must not
	// publish under a new proxy generation.
	if chatGPTWebFingerprint(startCredential, webExecutor.EffectiveProxyURL(auth)) != startFingerprint {
		return fallback
	}
	discoveredCredential := chatGPTWebCredentialFingerprint(discoveredAuth)
	discoveredFingerprint := chatGPTWebFingerprint(discoveredCredential, startProxyURL)
	if err != nil {
		if !s.chatGPTWebAuthMatchesFingerprint(auth.ID, discoveredFingerprint) {
			return fallback
		}
		s.chatGPTWebCatalogMu.Lock()
		cached, cachedOK = s.chatGPTWebCatalogs[auth.ID]
		if !cachedOK || cached.fingerprint != fingerprint && cached.fingerprint != discoveredFingerprint {
			s.chatGPTWebCatalogMu.Unlock()
			return fallback
		}
		cached.fingerprint = discoveredFingerprint
		cached.refreshFailed = true
		if cached.discovered {
			cached.fetchedAt = time.Now()
			s.chatGPTWebCatalogs[auth.ID] = cached
		} else {
			cached = chatGPTWebCatalogEntry{
				models:        append([]*ModelInfo(nil), fallback...),
				fingerprint:   discoveredFingerprint,
				fetchedAt:     time.Now(),
				refreshFailed: true,
			}
			s.chatGPTWebCatalogs[auth.ID] = cached
			cachedOK = false
		}
		s.chatGPTWebCatalogMu.Unlock()
		if cachedOK && cached.fingerprint == discoveredFingerprint && cached.discovered {
			log.WithError(err).WithField("auth_id", auth.ID).Warn("ChatGPT web model catalog refresh failed; keeping the last successful catalog")
			return append([]*ModelInfo(nil), cached.models...)
		}
		log.WithError(err).WithField("auth_id", auth.ID).Warn("ChatGPT web model catalog discovery failed; using built-in models")
		return fallback
	}

	if !s.chatGPTWebAuthMatchesFingerprint(auth.ID, discoveredFingerprint) {
		return fallback
	}
	s.chatGPTWebCatalogMu.Lock()
	cached, cachedOK = s.chatGPTWebCatalogs[auth.ID]
	if !cachedOK || cached.fingerprint != fingerprint && cached.fingerprint != discoveredFingerprint {
		s.chatGPTWebCatalogMu.Unlock()
		return fallback
	}
	s.chatGPTWebCatalogs[auth.ID] = chatGPTWebCatalogEntry{
		models:      append([]*ModelInfo(nil), models...),
		fingerprint: discoveredFingerprint,
		fetchedAt:   time.Now(),
		discovered:  true,
	}
	s.chatGPTWebCatalogMu.Unlock()
	return models
}

// registerModelsForAuth (re)binds provider models in the global registry using the core auth ID as client identifier.
func (s *Service) registerModelsForAuth(a *coreauth.Auth) {
	if a == nil || a.ID == "" {
		return
	}
	if a.Disabled || a.Status == coreauth.StatusDisabled {
		GlobalModelRegistry().UnregisterClient(a.ID)
		return
	}
	authKind := strings.ToLower(strings.TrimSpace(a.Attributes["auth_kind"]))
	if authKind == "" {
		if kind, _ := a.AccountInfo(); strings.EqualFold(kind, "api_key") {
			authKind = "apikey"
		}
	}
	if a.Attributes != nil {
		if v := strings.TrimSpace(a.Attributes["gemini_virtual_primary"]); strings.EqualFold(v, "true") {
			GlobalModelRegistry().UnregisterClient(a.ID)
			return
		}
	}
	// Unregister legacy client ID (if present) to avoid double counting
	if a.Runtime != nil {
		if idGetter, ok := a.Runtime.(interface{ GetClientID() string }); ok {
			if rid := idGetter.GetClientID(); rid != "" && rid != a.ID {
				GlobalModelRegistry().UnregisterClient(rid)
			}
		}
	}
	provider := strings.ToLower(strings.TrimSpace(a.Provider))
	compatProviderKey, compatDisplayName, compatDetected := openAICompatInfoFromAuth(a)
	if compatDetected {
		provider = "openai-compatibility"
	}
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	excluded := oauthExcludedModels(cfg, provider, authKind)
	// The synthesizer pre-merges per-account and global exclusions into the "excluded_models" attribute.
	// If this attribute is present, it represents the complete list of exclusions and overrides the global config.
	if a.Attributes != nil {
		if val, ok := a.Attributes["excluded_models"]; ok && strings.TrimSpace(val) != "" {
			excluded = strings.Split(val, ",")
		}
	}
	var models []*ModelInfo
	switch provider {
	case "gemini":
		models = registry.GetGeminiModels()
		if entry := resolveConfigGeminiKey(cfg, a); entry != nil {
			if len(entry.Models) > 0 {
				models = buildGeminiConfigModels(entry)
			}
			if authKind == "apikey" {
				excluded = entry.ExcludedModels
			}
		}
		models = applyExcludedModels(models, excluded)
	case "vertex":
		// Vertex AI Gemini supports the same model identifiers as Gemini.
		models = registry.GetGeminiVertexModels()
		if entry := resolveConfigVertexCompatKey(cfg, a); entry != nil {
			if len(entry.Models) > 0 {
				models = buildVertexCompatConfigModels(entry)
			}
			if authKind == "apikey" {
				excluded = entry.ExcludedModels
			}
		}
		models = applyExcludedModels(models, excluded)
	case "gemini-cli":
		models = registry.GetGeminiCLIModels()
		models = applyExcludedModels(models, excluded)
	case "aistudio":
		models = registry.GetAIStudioModels()
		models = applyExcludedModels(models, excluded)
	case "antigravity":
		models = registry.GetAntigravityModels()
		models = applyExcludedModels(models, excluded)
	case "claude":
		models = registry.GetClaudeModels()
		if entry := resolveConfigClaudeKey(cfg, a); entry != nil {
			if len(entry.Models) > 0 {
				models = buildClaudeConfigModels(entry)
			}
			if authKind == "apikey" {
				excluded = entry.ExcludedModels
			}
		}
		models = applyExcludedModels(models, excluded)
	case "codex":
		codexPlanType := ""
		if a.Attributes != nil {
			codexPlanType = strings.TrimSpace(a.Attributes["plan_type"])
		}
		switch strings.ToLower(codexPlanType) {
		case "pro":
			models = registry.GetCodexProModels()
		case "plus":
			models = registry.GetCodexPlusModels()
		case "team", "business", "go":
			models = registry.GetCodexTeamModels()
		case "free":
			models = registry.GetCodexFreeModels()
		default:
			models = registry.GetCodexProModels()
		}
		if entry := resolveConfigCodexKey(cfg, a); entry != nil {
			if len(entry.Models) > 0 {
				models = buildCodexConfigModels(entry)
			}
			if authKind == "apikey" {
				excluded = entry.ExcludedModels
			}
		}
		models = applyExcludedModels(models, excluded)
	case "kimi":
		models = registry.GetKimiModels()
		models = applyExcludedModels(models, excluded)
	case "chatgpt-web":
		models = s.chatGPTWebModelsForAuth(a)
		// Exclusions may name either the namespaced catalog ID
		// (chatgpt-web/<slug>) or the bare upstream slug; match both.
		models = applyExcludedModelsMatching(models, excluded, chatGPTWebModelCandidates)
	default:
		// Handle OpenAI-compatibility providers by name using config
		if cfg != nil {
			providerKey := provider
			compatName := strings.TrimSpace(a.Provider)
			isCompatAuth := false
			if compatDetected {
				if compatProviderKey != "" {
					providerKey = compatProviderKey
				}
				if compatDisplayName != "" {
					compatName = compatDisplayName
				}
				isCompatAuth = true
			}
			if strings.EqualFold(providerKey, "openai-compatibility") {
				isCompatAuth = true
				if a.Attributes != nil {
					if v := strings.TrimSpace(a.Attributes["compat_name"]); v != "" {
						compatName = v
					}
					if v := strings.TrimSpace(a.Attributes["provider_key"]); v != "" {
						providerKey = strings.ToLower(v)
						isCompatAuth = true
					}
				}
				if providerKey == "openai-compatibility" && compatName != "" {
					providerKey = strings.ToLower(compatName)
				}
			} else if a.Attributes != nil {
				if v := strings.TrimSpace(a.Attributes["compat_name"]); v != "" {
					compatName = v
					isCompatAuth = true
				}
				if v := strings.TrimSpace(a.Attributes["provider_key"]); v != "" {
					providerKey = strings.ToLower(v)
					isCompatAuth = true
				}
			}
			for i := range cfg.OpenAICompatibility {
				compat := &cfg.OpenAICompatibility[i]
				if compat.Disabled {
					continue
				}
				if strings.EqualFold(compat.Name, compatName) {
					isCompatAuth = true
					// Convert compatibility models to registry models
					ms := make([]*ModelInfo, 0, len(compat.Models))
					for j := range compat.Models {
						m := compat.Models[j]
						// Use alias as model ID, fallback to name if alias is empty
						modelID := m.Alias
						if modelID == "" {
							modelID = m.Name
						}
						thinking := m.Thinking
						if thinking == nil {
							thinking = &registry.ThinkingSupport{Levels: []string{"low", "medium", "high"}}
						}
						ms = append(ms, &ModelInfo{
							ID:          modelID,
							Object:      "model",
							Created:     time.Now().Unix(),
							OwnedBy:     compat.Name,
							Type:        "openai-compatibility",
							DisplayName: modelID,
							UserDefined: false,
							Thinking:    thinking,
						})
					}
					// Register and return
					if len(ms) > 0 {
						if providerKey == "" {
							providerKey = "openai-compatibility"
						}
						s.registerResolvedModelsForAuth(a, providerKey, applyModelPrefixes(ms, a.Prefix, cfg.ForceModelPrefix))
					} else {
						// Ensure stale registrations are cleared when model list becomes empty.
						GlobalModelRegistry().UnregisterClient(a.ID)
					}
					return
				}
			}
			if isCompatAuth {
				// No matching provider found or models removed entirely; drop any prior registration.
				GlobalModelRegistry().UnregisterClient(a.ID)
				return
			}
		}
	}
	models = applyOAuthModelAlias(cfg, provider, authKind, models)
	if len(models) > 0 {
		key := provider
		if key == "" {
			key = strings.ToLower(strings.TrimSpace(a.Provider))
		}
		s.registerResolvedModelsForAuth(a, key, applyModelPrefixes(models, a.Prefix, cfg != nil && cfg.ForceModelPrefix))
		return
	}

	GlobalModelRegistry().UnregisterClient(a.ID)
}

// refreshModelRegistrationForAuth re-applies the latest model registration for
// one auth and reconciles any concurrent auth changes that race with the
// refresh. Callers are expected to pre-filter provider membership.
//
// Re-registration is deliberate: registry cooldown/suspension state is treated
// as part of the previous registration snapshot and is cleared when the auth is
// rebound to the refreshed model catalog.
func (s *Service) refreshModelRegistrationForAuth(ctx context.Context, current *coreauth.Auth) bool {
	if s == nil || s.coreManager == nil || current == nil || current.ID == "" {
		return false
	}
	value, _, _ := s.modelRefreshes.Do(s.modelRegistrationRefreshKey(current), func() (any, error) {
		return s.refreshModelRegistrationForAuthUncoalesced(ctx, current), nil
	})
	refreshed, _ := value.(bool)
	return refreshed
}

func (s *Service) refreshModelRegistrationForAuthUncoalesced(ctx context.Context, current *coreauth.Auth) bool {
	if s == nil || s.coreManager == nil || current == nil || current.ID == "" {
		return false
	}

	if !current.Disabled && current.Status != coreauth.StatusDisabled {
		s.ensureExecutorsForAuth(current)
	}
	if !current.Disabled && current.Status != coreauth.StatusDisabled && strings.EqualFold(strings.TrimSpace(current.Provider), "chatgpt-web") {
		s.refreshChatGPTWebModelsForAuth(ctx, current)
	}

	unlockRegistration := s.lockModelRegistration(current.ID)
	defer unlockRegistration()
	latest, ok := s.latestAuthForModelRegistration(current.ID)
	if !ok || latest.Disabled || latest.Status == coreauth.StatusDisabled {
		GlobalModelRegistry().UnregisterClient(current.ID)
		s.coreManager.RefreshSchedulerEntry(current.ID)
		return false
	}

	// Re-apply the latest auth snapshot so concurrent auth updates cannot leave
	// stale model registrations behind. This may duplicate registration work when
	// no auth fields changed, but keeps the refresh path simple and correct.
	s.ensureExecutorsForAuth(latest)
	s.registerModelsForAuth(latest)
	s.coreManager.ReconcileRegistryModelStates(context.Background(), latest.ID)
	s.coreManager.RefreshSchedulerEntry(current.ID)
	return true
}

// latestAuthForModelRegistration returns the latest auth snapshot regardless of
// provider membership. Callers use this after a registration attempt to restore
// whichever state currently owns the client ID in the global registry.
func (s *Service) latestAuthForModelRegistration(authID string) (*coreauth.Auth, bool) {
	if s == nil || s.coreManager == nil || authID == "" {
		return nil, false
	}
	auth, ok := s.coreManager.GetByID(authID)
	if !ok || auth == nil || auth.ID == "" {
		return nil, false
	}
	return auth, true
}

func resolveConfigClaudeKey(cfg *config.Config, auth *coreauth.Auth) *config.ClaudeKey {
	if auth == nil || cfg == nil {
		return nil
	}
	var attrKey, attrBase string
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range cfg.ClaudeKey {
		entry := &cfg.ClaudeKey[i]
		cfgKey := strings.TrimSpace(entry.APIKey)
		cfgBase := strings.TrimSpace(entry.BaseURL)
		if attrKey != "" && attrBase != "" {
			if strings.EqualFold(cfgKey, attrKey) && strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
			continue
		}
		if attrKey != "" && strings.EqualFold(cfgKey, attrKey) {
			if cfgBase == "" || strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
		}
		if attrKey == "" && attrBase != "" && strings.EqualFold(cfgBase, attrBase) {
			return entry
		}
	}
	if attrKey != "" {
		for i := range cfg.ClaudeKey {
			entry := &cfg.ClaudeKey[i]
			if strings.EqualFold(strings.TrimSpace(entry.APIKey), attrKey) {
				return entry
			}
		}
	}
	return nil
}

func resolveConfigGeminiKey(cfg *config.Config, auth *coreauth.Auth) *config.GeminiKey {
	if auth == nil || cfg == nil {
		return nil
	}
	var attrKey, attrBase string
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range cfg.GeminiKey {
		entry := &cfg.GeminiKey[i]
		cfgKey := strings.TrimSpace(entry.APIKey)
		cfgBase := strings.TrimSpace(entry.BaseURL)
		if attrKey != "" && strings.EqualFold(cfgKey, attrKey) {
			if cfgBase == "" || strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
			continue
		}
		if attrKey == "" && attrBase != "" && strings.EqualFold(cfgBase, attrBase) {
			return entry
		}
	}
	return nil
}

func resolveConfigVertexCompatKey(cfg *config.Config, auth *coreauth.Auth) *config.VertexCompatKey {
	if auth == nil || cfg == nil {
		return nil
	}
	var attrKey, attrBase string
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range cfg.VertexCompatAPIKey {
		entry := &cfg.VertexCompatAPIKey[i]
		cfgKey := strings.TrimSpace(entry.APIKey)
		cfgBase := strings.TrimSpace(entry.BaseURL)
		if attrKey != "" && strings.EqualFold(cfgKey, attrKey) {
			if cfgBase == "" || strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
			continue
		}
		if attrKey == "" && attrBase != "" && strings.EqualFold(cfgBase, attrBase) {
			return entry
		}
	}
	if attrKey != "" {
		for i := range cfg.VertexCompatAPIKey {
			entry := &cfg.VertexCompatAPIKey[i]
			if strings.EqualFold(strings.TrimSpace(entry.APIKey), attrKey) {
				return entry
			}
		}
	}
	return nil
}

func resolveConfigCodexKey(cfg *config.Config, auth *coreauth.Auth) *config.CodexKey {
	if auth == nil || cfg == nil {
		return nil
	}
	var attrKey, attrBase string
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range cfg.CodexKey {
		entry := &cfg.CodexKey[i]
		cfgKey := strings.TrimSpace(entry.APIKey)
		cfgBase := strings.TrimSpace(entry.BaseURL)
		if attrKey != "" && strings.EqualFold(cfgKey, attrKey) {
			if cfgBase == "" || strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
			continue
		}
		if attrKey == "" && attrBase != "" && strings.EqualFold(cfgBase, attrBase) {
			return entry
		}
	}
	return nil
}

func oauthExcludedModels(cfg *config.Config, provider, authKind string) []string {
	if cfg == nil {
		return nil
	}
	authKindKey := strings.ToLower(strings.TrimSpace(authKind))
	providerKey := strings.ToLower(strings.TrimSpace(provider))
	if authKindKey == "apikey" {
		return nil
	}
	return cfg.OAuthExcludedModels[providerKey]
}

func applyExcludedModels(models []*ModelInfo, excluded []string) []*ModelInfo {
	return applyExcludedModelsMatching(models, excluded, func(model *ModelInfo) []string {
		return []string{model.ID}
	})
}

func applyExcludedModelsMatching(models []*ModelInfo, excluded []string, ids func(*ModelInfo) []string) []*ModelInfo {
	if len(models) == 0 || len(excluded) == 0 {
		return models
	}

	patterns := make([]string, 0, len(excluded))
	for _, item := range excluded {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			patterns = append(patterns, strings.ToLower(trimmed))
		}
	}
	if len(patterns) == 0 {
		return models
	}

	filtered := make([]*ModelInfo, 0, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		blocked := false
		for _, id := range ids(model) {
			modelID := strings.ToLower(strings.TrimSpace(id))
			for _, pattern := range patterns {
				if matchWildcard(pattern, modelID) {
					blocked = true
					break
				}
			}
			if blocked {
				break
			}
		}
		if !blocked {
			filtered = append(filtered, model)
		}
	}
	return filtered
}

func chatGPTWebModelCandidates(model *ModelInfo) []string {
	if model == nil {
		return nil
	}
	ids := []string{model.ID}
	if slug := strings.TrimSpace(model.Version); slug != "" {
		ids = append(ids, slug)
	}
	if slug := strings.TrimPrefix(model.ID, "chatgpt-web/"); slug != model.ID {
		ids = append(ids, slug)
	}
	return ids
}

func applyModelPrefixes(models []*ModelInfo, prefix string, forceModelPrefix bool) []*ModelInfo {
	trimmedPrefix := strings.TrimSpace(prefix)
	if trimmedPrefix == "" || len(models) == 0 {
		return models
	}

	out := make([]*ModelInfo, 0, len(models)*2)
	seen := make(map[string]struct{}, len(models)*2)

	addModel := func(model *ModelInfo) {
		if model == nil {
			return
		}
		id := strings.TrimSpace(model.ID)
		if id == "" {
			return
		}
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		out = append(out, model)
	}

	for _, model := range models {
		if model == nil {
			continue
		}
		baseID := strings.TrimSpace(model.ID)
		if baseID == "" {
			continue
		}
		if !forceModelPrefix || trimmedPrefix == baseID {
			addModel(model)
		}
		clone := *model
		clone.ID = trimmedPrefix + "/" + baseID
		addModel(&clone)
	}
	return out
}

// matchWildcard performs case-insensitive wildcard matching where '*' matches any substring.
func matchWildcard(pattern, value string) bool {
	if pattern == "" {
		return false
	}

	// Fast path for exact match (no wildcard present).
	if !strings.Contains(pattern, "*") {
		return pattern == value
	}

	parts := strings.Split(pattern, "*")
	// Handle prefix.
	if prefix := parts[0]; prefix != "" {
		if !strings.HasPrefix(value, prefix) {
			return false
		}
		value = value[len(prefix):]
	}

	// Handle suffix.
	if suffix := parts[len(parts)-1]; suffix != "" {
		if !strings.HasSuffix(value, suffix) {
			return false
		}
		value = value[:len(value)-len(suffix)]
	}

	// Handle middle segments in order.
	for i := 1; i < len(parts)-1; i++ {
		segment := parts[i]
		if segment == "" {
			continue
		}
		idx := strings.Index(value, segment)
		if idx < 0 {
			return false
		}
		value = value[idx+len(segment):]
	}

	return true
}

type modelEntry interface {
	GetName() string
	GetAlias() string
}

func buildConfigModels[T modelEntry](models []T, ownedBy, modelType string) []*ModelInfo {
	if len(models) == 0 {
		return nil
	}
	now := time.Now().Unix()
	out := make([]*ModelInfo, 0, len(models))
	seen := make(map[string]struct{}, len(models))
	for i := range models {
		model := models[i]
		name := strings.TrimSpace(model.GetName())
		alias := strings.TrimSpace(model.GetAlias())
		if alias == "" {
			alias = name
		}
		if alias == "" {
			continue
		}
		key := strings.ToLower(alias)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		display := name
		if display == "" {
			display = alias
		}
		info := &ModelInfo{
			ID:          alias,
			Object:      "model",
			Created:     now,
			OwnedBy:     ownedBy,
			Type:        modelType,
			DisplayName: display,
			UserDefined: true,
		}
		if name != "" {
			if upstream := registry.LookupStaticModelInfo(name); upstream != nil && upstream.Thinking != nil {
				info.Thinking = upstream.Thinking
			}
		}
		out = append(out, info)
	}
	return out
}

func buildVertexCompatConfigModels(entry *config.VertexCompatKey) []*ModelInfo {
	if entry == nil {
		return nil
	}
	return buildConfigModels(entry.Models, "google", "vertex")
}

func buildGeminiConfigModels(entry *config.GeminiKey) []*ModelInfo {
	if entry == nil {
		return nil
	}
	return buildConfigModels(entry.Models, "google", "gemini")
}

func buildClaudeConfigModels(entry *config.ClaudeKey) []*ModelInfo {
	if entry == nil {
		return nil
	}
	return buildConfigModels(entry.Models, "anthropic", "claude")
}

func buildCodexConfigModels(entry *config.CodexKey) []*ModelInfo {
	if entry == nil {
		return nil
	}
	return registry.WithCodexBuiltins(buildConfigModels(entry.Models, "openai", "openai"))
}

func rewriteModelInfoName(name, oldID, newID string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return name
	}
	oldID = strings.TrimSpace(oldID)
	newID = strings.TrimSpace(newID)
	if oldID == "" || newID == "" {
		return name
	}
	if strings.EqualFold(oldID, newID) {
		return name
	}
	if strings.EqualFold(trimmed, oldID) {
		return newID
	}
	if strings.HasSuffix(trimmed, "/"+oldID) {
		prefix := strings.TrimSuffix(trimmed, oldID)
		return prefix + newID
	}
	if trimmed == "models/"+oldID {
		return "models/" + newID
	}
	return name
}

func applyOAuthModelAlias(cfg *config.Config, provider, authKind string, models []*ModelInfo) []*ModelInfo {
	if cfg == nil || len(models) == 0 {
		return models
	}
	channel := coreauth.OAuthModelAliasChannel(provider, authKind)
	if channel == "" || len(cfg.OAuthModelAlias) == 0 {
		return models
	}
	aliases := cfg.OAuthModelAlias[channel]
	if len(aliases) == 0 {
		return models
	}

	type aliasEntry struct {
		alias string
		fork  bool
	}

	forward := make(map[string][]aliasEntry, len(aliases))
	for i := range aliases {
		name := strings.TrimSpace(aliases[i].Name)
		alias := strings.TrimSpace(aliases[i].Alias)
		if name == "" || alias == "" {
			continue
		}
		if strings.EqualFold(name, alias) {
			continue
		}
		key := strings.ToLower(name)
		forward[key] = append(forward[key], aliasEntry{alias: alias, fork: aliases[i].Fork})
	}
	if len(forward) == 0 {
		return models
	}

	out := make([]*ModelInfo, 0, len(models))
	seen := make(map[string]struct{}, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		id := strings.TrimSpace(model.ID)
		if id == "" {
			continue
		}
		key := strings.ToLower(id)
		entries := forward[key]
		if len(entries) == 0 {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, model)
			continue
		}

		keepOriginal := false
		for _, entry := range entries {
			if entry.fork {
				keepOriginal = true
				break
			}
		}
		if keepOriginal {
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				out = append(out, model)
			}
		}

		addedAlias := false
		for _, entry := range entries {
			mappedID := strings.TrimSpace(entry.alias)
			if mappedID == "" {
				continue
			}
			if strings.EqualFold(mappedID, id) {
				continue
			}
			aliasKey := strings.ToLower(mappedID)
			if _, exists := seen[aliasKey]; exists {
				continue
			}
			seen[aliasKey] = struct{}{}
			clone := *model
			clone.ID = mappedID
			if clone.Name != "" {
				clone.Name = rewriteModelInfoName(clone.Name, id, mappedID)
			}
			out = append(out, &clone)
			addedAlias = true
		}

		if !keepOriginal && !addedAlias {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, model)
		}
	}
	return out
}
