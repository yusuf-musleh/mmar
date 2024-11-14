package logger

import (
	"html"
	"log"
	"net/http"
)

type WrappedResponseWriter struct {
	http.ResponseWriter
	statusCode    int
	contentLength int64
}

func (wrw *WrappedResponseWriter) WriteHeader(statusCode int) {
	wrw.statusCode = statusCode
	wrw.ResponseWriter.WriteHeader(statusCode)
}

func (wrw *WrappedResponseWriter) Write(data []byte) (int, error) {
	wrw.contentLength = int64(len(data))
	return wrw.ResponseWriter.Write(data)
}

func LogHTTP(req *http.Request, statusCode int, contentLength int64) {
	hasQueryParams := ""
	if req.URL.RawQuery != "" {
		hasQueryParams = "?"
	}

	log.Printf(
		"\"%s %s%s%s %s\" %d %d",
		req.Method,
		html.EscapeString(req.URL.Path),
		hasQueryParams,
		req.URL.RawQuery,
		req.Proto,
		statusCode,
		contentLength,
	)
}

func LoggerMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wrw := WrappedResponseWriter{ResponseWriter: w, statusCode: http.StatusOK, contentLength: 0}
		h.ServeHTTP(&wrw, r)
		LogHTTP(r, wrw.statusCode, wrw.contentLength)
	})
}
