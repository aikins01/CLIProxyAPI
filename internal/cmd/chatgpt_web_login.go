package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v7/sdk/auth"
	"golang.org/x/term"
)

// DoChatGPTWebLogin captures a pasted ChatGPT session cookie and saves it as
// a chatgpt-web auth record in the configured auth directory.
func DoChatGPTWebLogin(cfg *config.Config, options *LoginOptions) error {
	if options == nil {
		options = &LoginOptions{}
	}
	promptFn := options.Prompt
	if promptFn == nil {
		promptFn = secretPrompt()
	}

	manager := newAuthManager()
	authOpts := &sdkAuth.LoginOptions{
		NoBrowser: true,
		Metadata:  map[string]string{},
		Prompt:    promptFn,
	}

	_, savedPath, err := manager.Login(context.Background(), "chatgpt-web", cfg, authOpts)
	if err != nil {
		return fmt.Errorf("ChatGPT Web authentication failed: %w", err)
	}
	if savedPath != "" {
		fmt.Printf("Authentication saved to %s\n", savedPath)
	}
	fmt.Println("ChatGPT Web authentication successful!")
	return nil
}

func secretPrompt() func(string) (string, error) {
	reader := bufio.NewReader(os.Stdin)
	return func(prompt string) (string, error) {
		fd := int(os.Stdin.Fd())
		if term.IsTerminal(fd) {
			fmt.Print(prompt)
			raw, err := readSecretLine(os.Stdin)
			fmt.Println()
			if err != nil {
				return "", err
			}
			return strings.TrimSpace(raw), nil
		}
		line, errRead := reader.ReadString('\n')
		if errRead != nil && !errors.Is(errRead, io.EOF) {
			return "", errRead
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" && errors.Is(errRead, io.EOF) {
			return promptTTY(prompt)
		}
		return trimmed, nil
	}
}

// promptTTY prompts on the controlling terminal after piped stdin has been
// exhausted, so flows like `pbpaste | ... --chatgpt-web-login` can still
// answer the remaining prompts interactively.
func promptTTY(prompt string) (string, error) {
	inputPath, outputPath := "/dev/tty", "/dev/tty"
	inputFlags := os.O_RDWR
	if runtime.GOOS == "windows" {
		inputPath, outputPath = "CONIN$", "CONOUT$"
		inputFlags = os.O_RDONLY
	}
	tty, errOpen := os.OpenFile(inputPath, inputFlags, 0)
	if errOpen != nil {
		return "", fmt.Errorf("stdin exhausted and no terminal available: %w", errOpen)
	}
	defer func() {
		if errClose := tty.Close(); errClose != nil {
			log.Errorf("failed to close terminal input: %v", errClose)
		}
	}()
	output := tty
	if outputPath != inputPath {
		output, errOpen = os.OpenFile(outputPath, os.O_WRONLY, 0)
		if errOpen != nil {
			return "", fmt.Errorf("stdin exhausted and no terminal output available: %w", errOpen)
		}
		defer func() {
			if errClose := output.Close(); errClose != nil {
				log.Errorf("failed to close terminal output: %v", errClose)
			}
		}()
	}
	fmt.Fprint(output, prompt)
	raw, err := readSecretLine(tty)
	fmt.Fprintln(output)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(raw), nil
}

// readSecretLine reads one line from a terminal in raw mode without echoing
// it. Raw mode is required because canonical-mode reads (term.ReadPassword)
// are capped at 1024 bytes per line on macOS, which silently drops long
// pastes such as full Cookie headers and leaves the prompt hanging.
func readSecretLine(f *os.File) (value string, err error) {
	const (
		ctrlC     = 3
		ctrlD     = 4
		backspace = 127
		ctrlH     = 8
	)
	fd := int(f.Fd())
	state, err := term.MakeRaw(fd)
	if err != nil {
		return "", err
	}
	defer func() {
		if errRestore := term.Restore(fd, state); errRestore != nil {
			err = errors.Join(err, fmt.Errorf("restore terminal state: %w", errRestore))
		}
	}()
	var out []byte
	buf := make([]byte, 4096)
	for {
		n, errRead := f.Read(buf)
		for i := 0; i < n; i++ {
			switch c := buf[i]; c {
			case '\r', '\n':
				return string(out), nil
			case ctrlC:
				return "", errors.New("interrupted")
			case ctrlD:
				if len(out) == 0 {
					return "", io.EOF
				}
				return string(out), nil
			case backspace, ctrlH:
				if len(out) > 0 {
					out = out[:len(out)-1]
				}
			default:
				if len(out) >= 1<<20 {
					return "", errors.New("input too long")
				}
				out = append(out, c)
			}
		}
		if errRead != nil {
			if errors.Is(errRead, io.EOF) {
				return string(out), nil
			}
			return "", errRead
		}
	}
}
