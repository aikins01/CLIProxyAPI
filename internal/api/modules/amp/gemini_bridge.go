package amp

import (
	"strings"

	"github.com/gin-gonic/gin"
)

// createGeminiBridgeHandler creates a handler that bridges AMP CLI's non-standard Gemini paths
// to our standard Gemini handler by rewriting the request context.
//
// AMP CLI format: /publishers/google/models/gemini-3-pro-preview:streamGenerateContent
// Standard format: /models/gemini-3-pro-preview:streamGenerateContent
//
// This extracts the model+method from the AMP path and sets it as the :action parameter
// so the standard Gemini handler can process it.
//
// The handler parameter should be a Gemini-compatible handler that expects the :action param.
func createGeminiBridgeHandler(handler gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get the full path from the catch-all parameter
		path := c.Param("path")

		// Extract model:method from AMP CLI path format
		// Example: /publishers/google/models/gemini-3-pro-preview:streamGenerateContent
		const modelsPrefix = "/models/"
		if idx := strings.Index(path, modelsPrefix); idx >= 0 {
			// Extract everything after modelsPrefix
			actionPart := path[idx+len(modelsPrefix):]

			actionPart = mappedGeminiAction(actionPart, c)

			// Set this as the :action parameter that the Gemini handler expects
			setGeminiActionParam(c, actionPart)

			// Call the handler
			handler(c)
			return
		}

		// If we can't parse the path, return Google-style error envelope so
		// Amp's Gemini parser surfaces a structured error instead of a raw
		// string.
		c.JSON(400, gin.H{
			"error": gin.H{
				"code":    400,
				"message": "Invalid Gemini API path format",
				"status":  "INVALID_ARGUMENT",
			},
		})
	}
}

func withMappedGeminiAction(handler gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		if action := c.Param("action"); action != "" {
			setGeminiActionParam(c, mappedGeminiAction(action, c))
		}
		handler(c)
	}
}

func mappedGeminiAction(action string, c *gin.Context) string {
	action = strings.TrimPrefix(action, "/")
	mappedModel, exists := c.Get(MappedModelContextKey)
	if !exists {
		return action
	}
	strModel, ok := mappedModel.(string)
	if !ok || strModel == "" {
		return action
	}
	if colonIdx := strings.Index(action, ":"); colonIdx > 0 {
		return strModel + action[colonIdx:]
	}
	return action
}

func setGeminiActionParam(c *gin.Context, action string) {
	for i := range c.Params {
		if c.Params[i].Key == "action" {
			c.Params[i].Value = action
			return
		}
	}
	c.Params = append(c.Params, gin.Param{Key: "action", Value: action})
}
