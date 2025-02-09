package logger

import (
	"fmt"
	"html"
	"log"
	"net/http"
	"strconv"

	"github.com/yusuf-musleh/mmar/constants"
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

func ColorLogStr(color string, logstr string) string {
	return color + logstr + constants.RESET
}

func Log(color string, logstr string) {
	if color == constants.DEFAULT_COLOR {
		log.Println(logstr)
		return
	}
	log.Println(ColorLogStr(color, logstr))
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

	// Color HTTP status code
	var strStatusCode string
	switch statusCode / 100 {
	case 2:
		strStatusCode = ColorLogStr(constants.GREEN, strconv.Itoa(statusCode))
	case 3:
		strStatusCode = ColorLogStr(constants.YELLOW, strconv.Itoa(statusCode))
	case 4:
		strStatusCode = ColorLogStr(constants.RED, strconv.Itoa(statusCode))
	case 5:
		strStatusCode = ColorLogStr(constants.RED, strconv.Itoa(statusCode))
	default:
		strStatusCode = strconv.Itoa(statusCode)
	}

	// Color HTTP method
	var coloredMethod string
	switch req.Method {
	case "GET":
		coloredMethod = ColorLogStr(constants.YELLOW, req.Method)
	case "POST", "PATCH", "PUT":
		coloredMethod = ColorLogStr(constants.BLUE, req.Method)
	case "DELETE":
		coloredMethod = ColorLogStr(constants.RED, req.Method)
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

func LogStartMmarServer(tcpPort string, httpPort string) {
	logStr := `Starting mmar server...
  Starting HTTP Server on port: %s
  Starting TCP Sever on port: %s

`
	log.Printf(
		logStr,
		httpPort,
		tcpPort,
	)

}

func LogStartMmarClient(tunnelHost string, tunnelTcpPort string, tunnelHttpPort string, localPort string) {
	logStr := `Starting %s...
  Creating tunnel:
    Tunnel Host: %s%s%s
    Local Port: %s

`

	tunnelTcpPortStr := ""
	if tunnelTcpPort != constants.SERVER_TCP_PORT {
		tunnelTcpPortStr = fmt.Sprintf("\n    Tunnel TCP Port: %s", ColorLogStr(constants.BLUE, tunnelTcpPort))
	}

	tunnelHttpPortStr := ""
	if tunnelHttpPort != constants.TUNNEL_HTTP_PORT {
		tunnelHttpPortStr = fmt.Sprintf("\n    Tunnel HTTP Port: %s", ColorLogStr(constants.BLUE, tunnelHttpPort))
	}

	log.Printf(
		logStr,
		ColorLogStr(constants.BLUE, "mmar client"),
		ColorLogStr(constants.BLUE, tunnelHost),
		tunnelTcpPortStr,
		tunnelHttpPortStr,
		ColorLogStr(constants.BLUE, localPort),
	)
}

func LogTunnelCreated(subdomain string, tunnelHost string, tunnelHttpPort string, localPort string) {
	logStr := `%s

A mmar tunnel is now open on:

>>>  %s://%s.%s%s %s http://localhost:%s

`
	httpProtocol := "https"
	tunnelHttpPortStr := ""
	if tunnelHost == "localhost" {
		httpProtocol = "http"
		if tunnelHttpPort == constants.TUNNEL_HTTP_PORT {
			tunnelHttpPort = constants.SERVER_HTTP_PORT
		}
	}

	if tunnelHttpPort != constants.TUNNEL_HTTP_PORT {
		tunnelHttpPortStr = ":" + tunnelHttpPort
	}

	log.Printf(
		logStr,
		ColorLogStr(constants.GREEN, "Tunnel created successfully!"),
		httpProtocol,
		subdomain,
		tunnelHost,
		tunnelHttpPortStr,
		ColorLogStr(constants.GREEN, "->"),
		localPort,
	)
}
