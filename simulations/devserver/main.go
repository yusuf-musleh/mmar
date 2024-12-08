package devserver

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
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

	mux.Handle("/get", http.HandlerFunc(handleGet))
	mux.Handle("/get-fail", http.HandlerFunc(handleGetFail))
	mux.Handle("/post", http.HandlerFunc(handlePost))
	mux.Handle("/post-fail", http.HandlerFunc(handlePostFail))

	return mux
}

func handleGet(w http.ResponseWriter, r *http.Request) {

	// TODO: Check and confirm contents of request (r) are what we expect

	respBody, err := json.Marshal(map[string]any{
		"success": true,
		"data":    "some data",
	})

	if err != nil {
		log.Fatalf("Failed to marshal response for GET: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
}

func handleGetFail(w http.ResponseWriter, r *http.Request) {

	// TODO: Check and confirm contents of request (r) are what we expect

	respBody, err := json.Marshal(map[string]any{
		"success": false,
		"error":   "Sent bad GET request",
	})

	if err != nil {
		log.Fatalf("Failed to marshal response for GET: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	w.Write(respBody)
}

func handlePost(w http.ResponseWriter, r *http.Request) {

	// TODO: Check and confirm contents of request (r) are what we expect

	respBody, err := json.Marshal(map[string]any{
		"success": true,
		"data": map[string]any{
			"posted": "data",
		},
	})

	if err != nil {
		log.Fatalf("Failed to marshal response for POST: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
}

func handlePostFail(w http.ResponseWriter, r *http.Request) {

	// TODO: Check and confirm contents of request (r) are what we expect

	respBody, err := json.Marshal(map[string]any{
		"success": false,
		"error":   "Sent bad POST request",
	})

	if err != nil {
		log.Fatalf("Failed to marshal response for GET: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	w.Write(respBody)
}
