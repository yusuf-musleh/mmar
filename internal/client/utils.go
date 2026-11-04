package client

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Process a response reader buffer containing a "content-length" header
func processContentLengthResp(respContentLength string, connReadWriter *bufio.ReadWriter) ([]byte, int, error) {
	cl, clErr := strconv.Atoi(respContentLength)
	if clErr != nil {
		return nil, 0, clErr
	}

	bodyBuffer := make([]byte, 0, cl)
	bodyReadBuffer := make([]byte, 4096)
	for len(bodyBuffer) < cl {
		r, readErr := connReadWriter.Read(bodyReadBuffer)
		if r > 0 {
			bodyBuffer = append(bodyBuffer, bodyReadBuffer[:r]...)
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, 0, readErr
		}
	}

	return bodyBuffer, cl, nil
}

// Process a response reader buffer containing a "transfer-encoding: chunked" header
func processChunkedResponse(connReadWriter *bufio.ReadWriter) ([]byte, int, error) {
	bodyBuffer := []byte{}

	contentLengthCounter := 0
	for {
		r, readErr := connReadWriter.ReadBytes('\n')

		// Check if we reached the last chunk to exit the loop
		if bytes.Equal([]byte("0\r\n"), r) {
			break
		}

		// Check hexadecimal value
		chunkHex, _ := bytes.CutSuffix(r, []byte("\r\n"))

		chunkSize, parseErr := strconv.ParseUint(string(chunkHex), 16, 64)
		if parseErr != nil {
			return nil, 0, parseErr
		}

		contentLengthCounter += int(chunkSize)

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}

			return nil, 0, readErr
		}

		chunk := make([]byte, chunkSize+2)
		_, chunkReadErr := io.ReadFull(connReadWriter, chunk)
		if chunkReadErr != nil {
			return nil, 0, chunkReadErr
		}

		bodyBuffer = append(bodyBuffer, chunk[:chunkSize]...)
	}

	return bodyBuffer, contentLengthCounter, nil
}

// Basic checks to validate a correctly formed HTTP response status line
func validateHTTPResponseStatusline(statusLine string) error {
	proto, status, ok := strings.Cut(statusLine, " ")
	if !ok {
		return errors.New(fmt.Sprintf("malformed HTTP response: %v", statusLine))
	}
	status = strings.TrimLeft(status, " ")

	statusCode, _, _ := strings.Cut(status, " ")
	if len(statusCode) != 3 {
		return errors.New(fmt.Sprintf("malformed HTTP status code: %v", statusCode))
	}
	statusCodeInt, err := strconv.Atoi(statusCode)
	if err != nil || statusCodeInt < 0 {
		return errors.New(fmt.Sprintf("malformed HTTP status code: %v", statusCodeInt))
	}
	if _, _, ok = http.ParseHTTPVersion(proto); !ok {
		return errors.New(fmt.Sprintf("malformed HTTP version: %v", proto))
	}
	return nil
}
