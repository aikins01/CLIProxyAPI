package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	chatgptweb "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

var chatGPTWebRefreshLead = 4 * time.Minute

// ChatGPTWebAuthenticator captures a pasted ChatGPT browser session cookie
// and stores it as a chatgpt-web auth record.
type ChatGPTWebAuthenticator struct{}

// NewChatGPTWebAuthenticator constructs a new chatgpt-web authenticator.
func NewChatGPTWebAuthenticator() Authenticator {
	return &ChatGPTWebAuthenticator{}
}

// Provider returns the provider key for chatgpt-web.
func (ChatGPTWebAuthenticator) Provider() string {
	return "chatgpt-web"
}

// RefreshLead returns the interval for persisting browser session rotations.
func (ChatGPTWebAuthenticator) RefreshLead() *time.Duration {
	return &chatGPTWebRefreshLead
}

// Login prompts for a ChatGPT session cookie and persists it. The cookie can
// be a full Cookie header copied from browser dev tools or a bare
// __Secure-next-auth.session-token value.
func (a ChatGPTWebAuthenticator) Login(ctx context.Context, cfg *config.Config, opts *LoginOptions) (*coreauth.Auth, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cliproxy auth: configuration is required")
	}
	if opts == nil {
		opts = &LoginOptions{}
	}
	if opts.Prompt == nil {
		return nil, fmt.Errorf("cliproxy auth: chatgpt-web login requires an interactive prompt")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fmt.Println("Paste your ChatGPT session cookie.")
	fmt.Println("  Easiest: chatgpt.com → DevTools → Network → any backend-api request → copy the full 'Cookie:' request header.")
	fmt.Println("  Include __Secure-next-auth.session-token, oai-did, and cf_clearance if present.")
	fmt.Println("  Paste it, then press Return.")
	raw, err := opts.Prompt("Cookie: ")
	if err != nil {
		return nil, fmt.Errorf("chatgpt-web: read cookie: %w", err)
	}
	// Persist only the credentials the session exchange needs, never the
	// whole pasted browser header.
	cookie := chatgptweb.MinimizeCookie(chatgptweb.ParseCookieInput(raw))
	if cookie == "" {
		return nil, fmt.Errorf("chatgpt-web: cookie must contain __Secure-next-auth.session-token")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fmt.Println("Paste the User-Agent request header from the same Chromium browser request.")
	fmt.Println("  Paste it, then press Return.")
	rawUserAgent, err := opts.Prompt("User-Agent: ")
	if err != nil {
		return nil, fmt.Errorf("chatgpt-web: read browser User-Agent: %w", err)
	}
	userAgent, err := chatgptweb.NormalizeUserAgent(rawUserAgent)
	if err != nil {
		return nil, err
	}

	// Verify the cookie against the session endpoint before persisting it;
	// an expired cookie would otherwise fail only at first request. Use the
	// proxy-aware uTLS client because Cloudflare binds cf_clearance to the
	// browser TLS fingerprint.
	client := helps.NewUtlsHTTPClient(cfg, nil, 30*time.Second)
	sess, err := chatgptweb.ResolveLoginSession(ctx, client, cookie, userAgent)
	if err != nil {
		return nil, err
	}
	// The exchange can rotate the session token via Set-Cookie; persist the
	// rotated credential, still minimized.
	cookie = chatgptweb.MinimizeCookie(sess.Cookie)
	if cookie == "" {
		return nil, fmt.Errorf("chatgpt-web: session exchange returned no usable session token")
	}
	fmt.Println("If ChatGPT requires Turnstile, the Web executor can submit without a Turnstile token.")
	fmt.Println("  This opt-in is stored on the credential and applies to future Turnstile challenges;")
	fmt.Println("  it is unreliable and may increase anti-abuse scrutiny.")
	rawSubmitWithoutTurnstile, err := opts.Prompt("Submit without Turnstile when challenged? [y/N]: ")
	if err != nil {
		// The opt-in is optional: a non-interactive session that cannot answer
		// keeps the default instead of failing the verified login.
		fmt.Printf("  Prompt unavailable (%v); keeping the default (no).\n", err)
		rawSubmitWithoutTurnstile = ""
	}
	submitWithoutTurnstile := false
	switch strings.ToLower(strings.TrimSpace(rawSubmitWithoutTurnstile)) {
	case "", "n", "no":
	case "y", "yes":
		submitWithoutTurnstile = true
	default:
		return nil, fmt.Errorf("chatgpt-web: Turnstile compatibility option must be yes or no")
	}

	email := ""
	if opts.Metadata != nil {
		email = strings.TrimSpace(opts.Metadata["email"])
	}
	if email == "" {
		// The verified session knows the account email; use it for
		// labeling only, never any account/user ID.
		email = strings.TrimSpace(sess.Email)
	}
	label := "ChatGPT Web"
	if email != "" {
		label = email
	}

	now := time.Now()
	fileName := fmt.Sprintf("chatgpt-web-%s.json", uuid.NewString())
	metadata := map[string]any{
		"type":         a.Provider(),
		"cookie":       cookie,
		"user_agent":   userAgent,
		"label":        label,
		"timestamp":    now.UnixMilli(),
		"last_refresh": now.UnixMilli(),
	}
	if email != "" {
		metadata["email"] = email
	}
	if submitWithoutTurnstile {
		metadata["submit_without_turnstile"] = true
	}

	return &coreauth.Auth{
		ID:       fileName,
		Provider: a.Provider(),
		FileName: fileName,
		Label:    label,
		Metadata: metadata,
	}, nil
}
