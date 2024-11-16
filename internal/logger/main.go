package logger

import (
	"html"
	"log"
	"net/http"
	"strconv"

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
func LogHTTP(req *http.Request, statusCode int, contentLength int64, includeSubdomain bool, colored bool) {
	hasQueryParams := ""
	if req.URL.RawQuery != "" {
		hasQueryParams = "?"
	}

	subdomainInfo := ""
	if includeSubdomain {
		subdomainInfo = "[" + utils.ExtractSubdomain(req.Host) + "] "
	}

	if !colored {
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
		return
	}

	// TODO: Need to refactor this code to be cleaner.

	var strStatusCode string
	switch statusCode / 100 {
	case 2:
		strStatusCode = "\033[32m" + strconv.Itoa(statusCode) + "\033[0m"
	case 3:
		strStatusCode = "\033[33m" + strconv.Itoa(statusCode) + "\033[0m"
	case 4:
		strStatusCode = "\033[31m" + strconv.Itoa(statusCode) + "\033[0m"
	case 5:
		strStatusCode = "\033[31m" + strconv.Itoa(statusCode) + "\033[0m"
	default:
		strStatusCode = strconv.Itoa(statusCode)
	}

	var coloredMethod string
	switch req.Method {
	case "GET":
		coloredMethod = "\033[33m" + req.Method + "\033[0m"
	case "POST", "PATCH", "PUT":
		coloredMethod = "\033[34m" + req.Method + "\033[0m"
	case "DELETE":
		coloredMethod = "\033[31m" + req.Method + "\033[0m"
	default:
		coloredMethod = req.Method
	}

	log.Printf(
		"%s\"%s %s%s%s %s\" %s %d",
		subdomainInfo,
		coloredMethod,
		html.EscapeString(req.URL.Path),
		hasQueryParams,
		req.URL.RawQuery,
		req.Proto,
		strStatusCode,
		contentLength,
	)

}

// Logger middle to log all HTTP requests handled
func LoggerMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Initializing WrappedResponseWrapper with default values
		wrw := WrappedResponseWriter{ResponseWriter: w, statusCode: http.StatusOK, contentLength: 0}
		h.ServeHTTP(&wrw, r)
		LogHTTP(r, wrw.statusCode, wrw.contentLength, true, false)
	})
}
