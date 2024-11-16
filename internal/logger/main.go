package logger

import (
	"html"
	"log"
	"net/http"

	"github.com/yusuf-musleh/mmar/internal/utils"
)

// Wrapping ResponseWriter to capture response status code and content length
type WrappedResponseWriter struct {
	http.ResponseWriter
	statusCode    int
	contentLength int64
}

// Capture the response status code then call the actual ResponseWriter's WriteHeader
func (wrw *WrappedResponseWriter) WriteHeader(statusCode int) {
	wrw.statusCode = statusCode
	wrw.ResponseWriter.WriteHeader(statusCode)
}

// Capture the response content length then call the actual ResponseWriter's Write
func (wrw *WrappedResponseWriter) Write(data []byte) (int, error) {
	wrw.contentLength = int64(len(data))
	return wrw.ResponseWriter.Write(data)
}

// Log HTTP requests including their response's status code and response data length
func LogHTTP(req *http.Request, statusCode int, contentLength int64, includeSubdomain bool) {
	hasQueryParams := ""
	if req.URL.RawQuery != "" {
		hasQueryParams = "?"
	}

	subdomainInfo := ""
	if includeSubdomain {
		subdomainInfo = "[" + utils.ExtractSubdomain(req.Host) + "] "
	}

	log.Printf(
		"%s\"%s %s%s%s %s\" %d %d",
		subdomainInfo,
		req.Method,
		html.EscapeString(req.URL.Path),
		hasQueryParams,
		req.URL.RawQuery,
		req.Proto,
		statusCode,
		contentLength,
	)
}

// Logger middle to log all HTTP requests handled
func LoggerMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Initializing WrappedResponseWrapper with default values
		wrw := WrappedResponseWriter{ResponseWriter: w, statusCode: http.StatusOK, contentLength: 0}
		h.ServeHTTP(&wrw, r)
		LogHTTP(r, wrw.statusCode, wrw.contentLength, true)
	})
}
