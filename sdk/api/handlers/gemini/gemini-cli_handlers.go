// Package gemini provides HTTP handlers for Gemini CLI API functionality.
// This package implements handlers that process CLI-specific requests for Gemini API operations,
// including content generation and streaming content generation endpoints.
// The handlers restrict access to localhost only and manage communication with the backend service.
package gemini

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	. "github.com/router-for-me/CLIProxyAPI/v7/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// GeminiCLIAPIHandler contains the handlers for Gemini CLI API endpoints.
// It holds a pool of clients to interact with the backend service.
type GeminiCLIAPIHandler struct {
	*handlers.BaseAPIHandler
}

// NewGeminiCLIAPIHandler creates a new Gemini CLI API handlers instance.
// It takes an BaseAPIHandler instance as input and returns a GeminiCLIAPIHandler.
func NewGeminiCLIAPIHandler(apiHandlers *handlers.BaseAPIHandler) *GeminiCLIAPIHandler {
	return &GeminiCLIAPIHandler{
		BaseAPIHandler: apiHandlers,
	}
}

// HandlerType returns the type of this handler.
func (h *GeminiCLIAPIHandler) HandlerType() string {
	return GeminiCLI
}

// Models returns a list of models supported by this handler.
func (h *GeminiCLIAPIHandler) Models() []map[string]any {
	return make([]map[string]any, 0)
}

// CLIHandler handles CLI-specific requests for Gemini API operations.
// It restricts access to localhost only and routes requests to appropriate internal handlers.
func (h *GeminiCLIAPIHandler) CLIHandler(c *gin.Context) {
	if h.Cfg == nil || !h.Cfg.EnableGeminiCLIEndpoint {
		c.JSON(http.StatusForbidden, handlers.ErrorResponse{
			Error: handlers.ErrorDetail{
				Message: "Gemini CLI endpoint is disabled",
				Type:    "forbidden",
			},
		})
		return
	}

	requestHost := c.Request.Host
	requestHostname := requestHost
	if hostname, _, errSplitHostPort := net.SplitHostPort(requestHost); errSplitHostPort == nil {
		requestHostname = hostname
	}

	if !strings.HasPrefix(c.Request.RemoteAddr, "127.0.0.1:") || requestHostname != "127.0.0.1" {
		c.JSON(http.StatusForbidden, handlers.ErrorResponse{
			Error: handlers.ErrorDetail{
				Message: "CLI reply only allow local access",
				Type:    "forbidden",
			},
		})
		return
	}

	rawJSON, _ := c.GetRawData()
	requestRawURI := c.Request.URL.Path

	if requestRawURI == "/v1internal:generateContent" {
		h.handleInternalGenerateContent(c, rawJSON)
	} else if requestRawURI == "/v1internal:streamGenerateContent" {
		h.handleInternalStreamGenerateContent(c, rawJSON)
	} else {
		reqBody := bytes.NewBuffer(rawJSON)
		req, err := http.NewRequest("POST", fmt.Sprintf("https://cloudcode-pa.googleapis.com%s", c.Request.URL.RequestURI()), reqBody)
		if err != nil {
			c.JSON(http.StatusBadRequest, handlers.ErrorResponse{
				Error: handlers.ErrorDetail{
					Message: fmt.Sprintf("Invalid request: %v", err),
					Type:    "invalid_request_error",
				},
			})
			return
		}
		for key, value := range c.Request.Header {
			req.Header[key] = value
		}

		httpClient := util.SetProxy(h.Cfg, &http.Client{})

		resp, err := httpClient.Do(req)
		if err != nil {
			c.JSON(http.StatusBadRequest, handlers.ErrorResponse{
				Error: handlers.ErrorDetail{
					Message: fmt.Sprintf("Invalid request: %v", err),
					Type:    "invalid_request_error",
				},
			})
			return
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			defer func() {
				if err = resp.Body.Close(); err != nil {
					log.Printf("warn: failed to close response body: %v", err)
				}
			}()
			bodyBytes, _ := io.ReadAll(resp.Body)

			c.JSON(http.StatusBadRequest, handlers.ErrorResponse{
				Error: handlers.ErrorDetail{
					Message: string(bodyBytes),
					Type:    "invalid_request_error",
				},
			})
			return
		}

		defer func() {
			_ = resp.Body.Close()
		}()

		for key, value := range resp.Header {
			c.Header(key, value[0])
		}
		output, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("Failed to read response body: %v", err)
			return
		}
		c.Set("API_RESPONSE_TIMESTAMP", time.Now())
		_, _ = c.Writer.Write(output)
		c.Set("API_RESPONSE", output)
	}
}

// handleInternalStreamGenerateContent handles streaming content generation requests.
// It sets up a server-sent event stream and forwards the request to the backend client.
// The function continuously proxies response chunks from the backend to the client.
func (h *GeminiCLIAPIHandler) handleInternalStreamGenerateContent(c *gin.Context, rawJSON []byte) {
	alt := h.GetAlt(c)

	// Get the http.Flusher interface to manually flush the response.
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, handlers.ErrorResponse{
			Error: handlers.ErrorDetail{
				Message: "Streaming not supported",
				Type:    "server_error",
			},
		})
		return
	}

	modelResult := gjson.GetBytes(rawJSON, "model")
	modelName := modelResult.String()

	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	dataChan, upstreamHeaders, streamMeta, errChan := h.ExecuteStreamWithAuthManagerMeta(cliCtx, h.HandlerType(), modelName, rawJSON, alt)
	setSSEHeaders := func() {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("Access-Control-Allow-Origin", "*")
	}
	forwardCommitted := func(streamErrs <-chan *interfaces.ErrorMessage) {
		setSSEHeaders()
		handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
		flusher.Flush()
		h.forwardCLIStream(c, flusher, alt, func(err error) { cliCancel(err) }, dataChan, streamErrs, streamMeta.KeepAliveInterval())
	}
	writeInitialError := func(errMsg *interfaces.ErrorMessage) {
		h.WriteErrorResponse(c, errMsg)
		if errMsg != nil {
			cliCancel(errMsg.Error)
		} else {
			cliCancel(nil)
		}
	}
	commitSignal := streamMeta.BootstrapCommittedSignal()
	if alt != "" {
		commitSignal = nil
	}
	if alt == "" && streamMeta.BootstrapCommitted() {
		forwardCommitted(errChan)
		return
	}

	for {
		select {
		case <-c.Request.Context().Done():
			cliCancel(c.Request.Context().Err())
			return
		case <-commitSignal:
			forwardCommitted(errChan)
			return
		case errMsg, ok := <-errChan:
			if !ok {
				errChan = nil
				continue
			}
			if alt == "" && streamMeta.BootstrapCommitted() {
				forwardCommitted(handlers.StreamErrorChannel(errMsg))
				return
			}
			writeInitialError(errMsg)
			return
		case chunk, ok := <-dataChan:
			if !ok {
				if errMsg, pending := handlers.PendingStreamError(errChan); pending {
					if alt == "" && streamMeta.BootstrapCommitted() {
						forwardCommitted(handlers.StreamErrorChannel(errMsg))
					} else {
						writeInitialError(errMsg)
					}
					return
				}
				if alt == "" {
					setSSEHeaders()
				}
				handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
				flusher.Flush()
				cliCancel(nil)
				return
			}

			if alt == "" {
				setSSEHeaders()
			}
			handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
			writeCLIStreamChunk(c, alt, chunk)
			flusher.Flush()
			h.forwardCLIStream(c, flusher, alt, func(err error) { cliCancel(err) }, dataChan, errChan, streamMeta.KeepAliveInterval())
			return
		}
	}
}

// handleInternalGenerateContent handles non-streaming content generation requests.
// It sends a request to the backend client and proxies the entire response back to the client at once.
func (h *GeminiCLIAPIHandler) handleInternalGenerateContent(c *gin.Context, rawJSON []byte) {
	c.Header("Content-Type", "application/json")
	modelResult := gjson.GetBytes(rawJSON, "model")
	modelName := modelResult.String()

	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	resp, upstreamHeaders, errMsg := h.ExecuteWithAuthManager(cliCtx, h.HandlerType(), modelName, rawJSON, "")
	if errMsg != nil {
		h.WriteErrorResponse(c, errMsg)
		cliCancel(errMsg.Error)
		return
	}
	handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
	_, _ = c.Writer.Write(resp)
	cliCancel()
}

func writeCLIStreamChunk(c *gin.Context, alt string, chunk []byte) {
	if alt != "" {
		_, _ = c.Writer.Write(chunk)
		return
	}
	if len(chunk) == 0 {
		return
	}
	_, _ = c.Writer.Write([]byte("data: "))
	_, _ = c.Writer.Write(chunk)
	_, _ = c.Writer.Write([]byte("\n\n"))
}

func (h *GeminiCLIAPIHandler) forwardCLIStream(c *gin.Context, flusher http.Flusher, alt string, cancel func(error), data <-chan []byte, errs <-chan *interfaces.ErrorMessage, keepAliveInterval *time.Duration) {
	if alt != "" {
		keepAliveInterval = new(time.Duration(0))
	}

	h.ForwardStream(c, flusher, cancel, data, errs, handlers.StreamForwardOptions{
		KeepAliveInterval: keepAliveInterval,
		WriteChunk: func(chunk []byte) {
			writeCLIStreamChunk(c, alt, chunk)
		},
		WriteTerminalError: func(errMsg *interfaces.ErrorMessage) {
			if errMsg == nil {
				return
			}
			status := http.StatusInternalServerError
			if errMsg.StatusCode > 0 {
				status = errMsg.StatusCode
			}
			errText := http.StatusText(status)
			if errMsg.Error != nil && errMsg.Error.Error() != "" {
				errText = errMsg.Error.Error()
			}
			body := handlers.BuildErrorResponseBody(status, errText)
			if alt == "" {
				_, _ = fmt.Fprintf(c.Writer, "event: error\ndata: %s\n\n", string(body))
			} else {
				_, _ = c.Writer.Write(body)
			}
		},
	})
}
