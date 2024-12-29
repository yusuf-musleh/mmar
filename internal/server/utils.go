package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/yusuf-musleh/mmar/constants"
)

var READ_BODY_CHUNK_TIMEOUT_ERR error = errors.New(constants.READ_BODY_CHUNK_TIMEOUT_ERR_TEXT)
var CLIENT_DISCONNECTED_ERR error = errors.New(constants.CLIENT_DISCONNECT_ERR_TEXT)

func responseWith(respText string, w http.ResponseWriter, statusCode int) {
	w.Header().Set("Content-Length", strconv.Itoa(len(respText)))
	w.Header().Set("Connection", "close")
	w.WriteHeader(statusCode)
	w.Write([]byte(respText))
}

func handleCancel(cause error, w http.ResponseWriter) {
	switch cause {
	case context.Canceled:
		// Cancelled, do nothing
		return
	case READ_BODY_CHUNK_TIMEOUT_ERR:
		responseWith(cause.Error(), w, http.StatusRequestTimeout)
	case CLIENT_DISCONNECTED_ERR:
		responseWith(cause.Error(), w, http.StatusBadRequest)
	}
}

func cancelRead(r IncomingRequest) {
	if errors.Is(r.ctx.Err(), context.Canceled) {
		// If context is Already cancelled, do nothing
		return
	}

	// Cancel request
	r.cancel(READ_BODY_CHUNK_TIMEOUT_ERR)
}

// Serialize HTTP request inorder to tunnel it to mmar client
func serializeRequest(r IncomingRequest) ([]byte, error) {
	var requestBuff bytes.Buffer

	// Writing & serializing the HTTP Request Line
	requestBuff.WriteString(
		fmt.Sprintf(
			"%v %v %v\nHost: %v\n",
			r.request.Method,
			r.request.URL.Path,
			r.request.Proto,
			r.request.Host,
		),
	)

	// Initialize read buffer/counter
	bufferSize := 2048
	contentLength := 0
	buf := make([]byte, bufferSize)
	reqBodyBytes := []byte{}

	// Keep reading response until completely read
	for {
		readBufferTimout := time.AfterFunc(
			constants.REQ_BODY_READ_CHUNK_TIMEOUT*time.Second,
			func() { cancelRead(r) },
		)
		r, readErr := r.request.Body.Read(buf)
		readBufferTimout.Stop()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				contentLength += r
				reqBodyBytes = append(reqBodyBytes, buf[:r]...)
				break
			}
			return nil, readErr
		}
		reqBodyBytes = append(reqBodyBytes, buf[:r]...)
		contentLength += r
		if r < bufferSize {
			break
		}
	}

	// Set actual Content-Length header
	r.request.Header.Set("Content-Length", strconv.Itoa(contentLength))

	// Serialize headers
	r.request.Header.Clone().Write(&requestBuff)

	// Add new line
	requestBuff.WriteByte('\n')

	// Write body to buffer
	requestBuff.Write(reqBodyBytes)
	requestBuff.WriteByte('\n')

	return requestBuff.Bytes(), nil
}
