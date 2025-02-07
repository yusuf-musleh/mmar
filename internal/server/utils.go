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

var READ_BODY_CHUNK_ERR error = errors.New(constants.READ_BODY_CHUNK_ERR_TEXT)
var READ_BODY_CHUNK_TIMEOUT_ERR error = errors.New(constants.READ_BODY_CHUNK_TIMEOUT_ERR_TEXT)
var CLIENT_DISCONNECTED_ERR error = errors.New(constants.CLIENT_DISCONNECT_ERR_TEXT)
var READ_RESP_BODY_ERR error = errors.New(constants.READ_RESP_BODY_ERR_TEXT)
var MAX_REQ_BODY_SIZE_ERR error = errors.New(constants.MAX_REQ_BODY_SIZE_ERR_TEXT)
var FAILED_TO_FORWARD_TO_MMAR_CLIENT_ERR error = errors.New(constants.FAILED_TO_FORWARD_TO_MMAR_CLIENT_ERR_TEXT)
var FAILED_TO_READ_RESP_FROM_MMAR_CLIENT_ERR error = errors.New(constants.FAILED_TO_READ_RESP_FROM_MMAR_CLIENT_ERR_TEXT)

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
	case READ_BODY_CHUNK_ERR, CLIENT_DISCONNECTED_ERR:
		responseWith(cause.Error(), w, http.StatusBadRequest)
	case READ_RESP_BODY_ERR:
		responseWith(cause.Error(), w, http.StatusInternalServerError)
	case MAX_REQ_BODY_SIZE_ERR:
		responseWith(cause.Error(), w, http.StatusRequestEntityTooLarge)
	}
}

func cancelRead(ctx context.Context, cancel context.CancelCauseFunc) {
	if errors.Is(ctx.Err(), context.Canceled) {
		// If context is Already cancelled, do nothing
		return
	}

	// Cancel request
	cancel(READ_BODY_CHUNK_TIMEOUT_ERR)
}

// Serialize HTTP request inorder to tunnel it to mmar client
func serializeRequest(ctx context.Context, r *http.Request, cancel context.CancelCauseFunc, serializedRequestChannel chan []byte) {
	var requestBuff bytes.Buffer

	// Writing & serializing the HTTP Request Line
	requestBuff.WriteString(
		fmt.Sprintf(
			"%v %v %v\nHost: %v\n",
			r.Method,
			r.URL.Path,
			r.Proto,
			r.Host,
		),
	)

	// Initialize read buffer/counter
	bufferSize := 2048
	contentLength := 0
	buf := make([]byte, bufferSize)
	reqBodyBytes := []byte{}

	// Keep reading response until completely read
	for {
		// Cancel request if read buffer times out
		readBufferTimeout := time.AfterFunc(
			constants.REQ_BODY_READ_CHUNK_TIMEOUT*time.Second,
			func() { cancelRead(ctx, cancel) },
		)
		r, readErr := r.Body.Read(buf)
		readBufferTimeout.Stop()
		contentLength += r
		if contentLength > constants.MAX_REQ_BODY_SIZE {
			cancel(MAX_REQ_BODY_SIZE_ERR)
			return
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				reqBodyBytes = append(reqBodyBytes, buf[:r]...)
				break
			}
			// Cancel request if there was an error reading
			cancel(READ_BODY_CHUNK_ERR)
			return
		}
		reqBodyBytes = append(reqBodyBytes, buf[:r]...)
	}

	// Set actual Content-Length header
	r.Header.Set("Content-Length", strconv.Itoa(contentLength))

	// Serialize headers
	r.Header.Clone().Write(&requestBuff)

	// Add new line
	requestBuff.WriteByte('\n')

	// Write body to buffer
	requestBuff.Write(reqBodyBytes)
	requestBuff.WriteByte('\n')

	// Send serialized request through channel
	serializedRequestChannel <- requestBuff.Bytes()
}
