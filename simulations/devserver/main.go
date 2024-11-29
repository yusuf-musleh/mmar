package devserver

import (
	"bytes"
	"encoding/json"
	"io"
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

	resp := http.Response{
		Status:        "200 OK",
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Body:          io.NopCloser(bytes.NewBuffer(respBody)),
		ContentLength: int64(len(respBody)),
	}

	w.WriteHeader(resp.StatusCode)
	var buf bytes.Buffer
	resp.Write(&buf)
	w.Write(buf.Bytes())
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

	resp := http.Response{
		Status:        "400 Bad Request",
		StatusCode:    http.StatusBadRequest,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Body:          io.NopCloser(bytes.NewBuffer(respBody)),
		ContentLength: int64(len(respBody)),
	}

	w.WriteHeader(resp.StatusCode)
	var buf bytes.Buffer
	resp.Write(&buf)
	w.Write(buf.Bytes())
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

	resp := http.Response{
		Status:        "200 OK",
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Body:          io.NopCloser(bytes.NewBuffer(respBody)),
		ContentLength: int64(len(respBody)),
	}

	w.WriteHeader(resp.StatusCode)
	var buf bytes.Buffer
	resp.Write(&buf)
	w.Write(buf.Bytes())
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

	resp := http.Response{
		Status:        "400 Bad Request",
		StatusCode:    http.StatusBadRequest,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Body:          io.NopCloser(bytes.NewBuffer(respBody)),
		ContentLength: int64(len(respBody)),
	}

	w.WriteHeader(resp.StatusCode)
	var buf bytes.Buffer
	resp.Write(&buf)
	w.Write(buf.Bytes())
}
