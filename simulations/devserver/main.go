package devserver

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"
)

const (
	GET_SUCCESS_URL  = "/get"
	GET_FAILURE_URL  = "/get-fail"
	POST_SUCCESS_URL = "/post"
	POST_FAILURE_URL = "/post-fail"
	REDIRECT_URL     = "/redirect"
	BAD_RESPONSE_URL = "/bad-resp"
	LONG_RUNNING_URL = "/long-running"
	CRASH_URL        = "/crash"
)

type DevServer struct {
	*httptest.Server
}

func NewDevServer() *DevServer {
	mux := setupMux()

	return &DevServer{
		httptest.NewServer(mux),
	}
}

func (ds *DevServer) Port() string {
	urlSplit := strings.Split(ds.URL, ":")
	devServerPort := urlSplit[len(urlSplit)-1]
	return devServerPort
}

func setupMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle(GET_SUCCESS_URL, http.HandlerFunc(handleGet))
	mux.Handle(GET_FAILURE_URL, http.HandlerFunc(handleGetFail))
	mux.Handle(POST_SUCCESS_URL, http.HandlerFunc(handlePost))
	mux.Handle(POST_FAILURE_URL, http.HandlerFunc(handlePostFail))
	mux.Handle(REDIRECT_URL, http.HandlerFunc(handleRedirect))
	mux.Handle(BAD_RESPONSE_URL, http.HandlerFunc(handleBadResp))
	mux.Handle(LONG_RUNNING_URL, http.HandlerFunc(handleLongRunningReq))
	mux.Handle(CRASH_URL, http.HandlerFunc(handleCrashingReq))

	return mux
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	// Include echo of request headers in response to confirm they were received
	respBody, err := json.Marshal(map[string]interface{}{
		"success": true,
		"data":    "some data",
		"echo": map[string]interface{}{
			"reqHeaders": r.Header,
		},
	})

	if err != nil {
		log.Fatalf("Failed to marshal response for GET: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	// Add custom header to response to confirm to confirm that they
	// propograte when going through mmar
	w.Header().Set("Simulation-Header", "devserver-handle-get")
	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
}

func handleGetFail(w http.ResponseWriter, r *http.Request) {
	// Include echo of request headers in response to confirm they were received
	respBody, err := json.Marshal(map[string]interface{}{
		"success": false,
		"error":   "Sent bad GET request",
		"echo": map[string]interface{}{
			"reqHeaders": r.Header,
		},
	})

	if err != nil {
		log.Fatalf("Failed to marshal response for GET: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	// Add custom header to response to confirm to confirm that they
	// propograte when going through mmar
	w.Header().Set("Simulation-Header", "devserver-handle-get-fail")
	w.WriteHeader(http.StatusBadRequest)
	w.Write(respBody)
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	// Include echo of request headers/body in response to confirm they were received
	var reqBody interface{}
	jsonDecoder := json.NewDecoder(r.Body)
	err := jsonDecoder.Decode(&reqBody)
	if err != nil {
		log.Fatal("Failed to decode request body to json", err)
	}

	respBody, err := json.Marshal(map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"posted": "data",
		},
		"echo": map[string]interface{}{
			"reqHeaders": r.Header,
			"reqBody":    reqBody,
		},
	})

	if err != nil {
		log.Fatalf("Failed to marshal response for POST: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(respBody)))
	// Add custom header to response to confirm to confirm that they
	// propograte when going through mmar
	w.Header().Set("Simulation-Header", "devserver-handle-post-success")
	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
}

func handlePostFail(w http.ResponseWriter, r *http.Request) {
	// Include echo of request headers/body in response to confirm they were received
	var reqBody interface{}
	jsonDecoder := json.NewDecoder(r.Body)
	err := jsonDecoder.Decode(&reqBody)
	if err != nil {
		log.Fatal("Failed to decode request body to json", err)
	}

	respBody, err := json.Marshal(map[string]interface{}{
		"success": false,
		"error":   "Sent bad POST request",
		"echo": map[string]interface{}{
			"reqHeaders": r.Header,
			"reqBody":    reqBody,
		},
	})

	if err != nil {
		log.Fatalf("Failed to marshal response for GET: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	// Add custom header to response to confirm to confirm that they
	// propograte when going through mmar
	w.Header().Set("Simulation-Header", "devserver-handle-post-fail")
	w.WriteHeader(http.StatusBadRequest)
	w.Write(respBody)
}

// Request handler that returns a redirect
func handleRedirect(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, GET_SUCCESS_URL, http.StatusFound)
}

// Request handler that returns an invalid HTTP response
func handleBadResp(w http.ResponseWriter, r *http.Request) {
	// Return a response with Content-Length headers that do not match the actual data
	respBody, err := json.Marshal(map[string]interface{}{
		"data": "some data",
	})

	if err != nil {
		log.Fatalf("Failed to marshal response for GET: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", "123") // Content length much larger than actual content
	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
}

// Request handler that takes a long time before returning response
func handleLongRunningReq(w http.ResponseWriter, r *http.Request) {
	// Include echo of request headers in response to confirm they were received
	respBody, err := json.Marshal(map[string]interface{}{
		"success": true,
		"data":    "some data",
		"echo": map[string]interface{}{
			"reqHeaders": r.Header,
		},
	})

	if err != nil {
		log.Fatalf("Failed to marshal response for GET: %v", err)
	}

	// Sleep longer than the dest server request timeout (30s)
	time.Sleep(40 * time.Second)

	w.Header().Set("Content-Type", "application/json")
	// Add custom header to response to confirm to confirm that they
	// propograte when going through mmar
	w.Header().Set("Simulation-Header", "devserver-handle-long-running")
	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
}

// Request handler that crashes the dev server
func handleCrashingReq(w http.ResponseWriter, _ *http.Request) {
	panic("crashing devserver")
}
