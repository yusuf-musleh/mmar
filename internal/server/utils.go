package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// Serialize HTTP request inorder to tunnel it to mmar client
func serializeRequest(r *http.Request) ([]byte, error) {
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
		r, readErr := r.Body.Read(buf)
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
	r.Header.Set("Content-Length", strconv.Itoa(contentLength))

	// Serialize headers
	r.Header.Clone().Write(&requestBuff)

	// Add new line
	requestBuff.WriteByte('\n')

	// Write body to buffer
	requestBuff.Write(reqBodyBytes)
	requestBuff.WriteByte('\n')

	return requestBuff.Bytes(), nil
}
