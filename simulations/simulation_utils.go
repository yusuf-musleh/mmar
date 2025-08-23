package simulations

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"slices"
	"testing"

	"github.com/yusuf-musleh/mmar/simulations/dnsserver"
)

type receivedRequest struct {
	headers map[string]string
	body    map[string]interface{}
}

type expectedResponse struct {
	statusCode int
	headers    map[string]string
	jsonBody   map[string]interface{}
	textBody   string
}

func validateRequestResponse(t *testing.T, expectedResp expectedResponse, resp *http.Response, testName string) {
	// Verify expected status code returned
	if resp.StatusCode != expectedResp.statusCode {
		t.Errorf("%v: resp.statusCode = %v; want %v", testName, resp.StatusCode, expectedResp.statusCode)
	}

	// Verify contains expected headers
	for hKey, hVal := range expectedResp.headers {
		vals, ok := resp.Header[hKey]
		if !ok || slices.Index(vals, hVal) == -1 {
			t.Errorf("%v: resp.headers[%v] = %v; want %v", testName, hKey, vals, hVal)
		}
	}

	// Verify correct body returned
	var respBody interface{}
	jsonDecoder := json.NewDecoder(resp.Body)
	err := jsonDecoder.Decode(&respBody)
	if err != nil {
		jsonDecoderBuffered := jsonDecoder.Buffered()
		nonJsonBody, nonJsonBodyErr := io.ReadAll(jsonDecoderBuffered)
		if nonJsonBodyErr != nil {
			t.Error("Failed to read response body", err)
			return
		}

		// Handle case when body is not JSON
		if string(nonJsonBody) != string(expectedResp.textBody) {
			t.Errorf("%v: body = %v; want %v", testName, string(nonJsonBody), string(expectedResp.textBody))
			return
		}
	}

	// Hanlde case when body is JSON
	expectedJson, _ := json.Marshal(expectedResp.jsonBody)
	actualJson, _ := json.Marshal(respBody)
	if string(actualJson) != string(expectedJson) {
		t.Errorf("%v: body = %v; want %v", testName, string(actualJson), string(expectedJson))
	}
}

func extractTunnelURL(clientStdout string) string {
	re := regexp.MustCompile(`http:\/\/[a-zA-Z0-9\-]+\.localhost:\d+`)
	return re.FindString(clientStdout)
}

func initCustomDialer() *net.Dialer {
	// Adding custom resolver that points to our simulated DNS Server to
	// handle subdomain on localhost
	r := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return net.Dial("udp", dnsserver.LOCALHOST_DNS_SERVER)
		},
	}
	dial := &net.Dialer{
		Resolver: r,
	}
	return dial
}

func httpClient() *http.Client {
	dialer := initCustomDialer()

	tp := &http.Transport{
		DialContext:       dialer.DialContext,
		DisableKeepAlives: true,
	}
	client := &http.Client{Transport: tp}
	return client
}

// This is used when we want more control over creating HTTP requests
// mainly allowing us to create invalid ones
func manualHttpRequest(url string, rawHttpReq string) net.Conn {
	dialer := initCustomDialer()

	conn, err := dialer.Dial("tcp", url)
	if err != nil {
		log.Fatal("Failed to connect to server", err)
	}

	_, writeErr := fmt.Fprint(conn, rawHttpReq)
	if writeErr != nil {
		log.Fatal("Failed to write to connection", writeErr)
	}

	return conn
}

// This is used when reading responses of manually performed HTTP requests
func manualReadResponse(conn net.Conn) (*http.Response, error) {
	defer conn.Close()
	bufferSize := 2048
	buf := make([]byte, bufferSize)
	respBytes := []byte{}

	// Keep reading response until completely read
	for {
		r, readErr := conn.Read(buf)
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				respBytes = append(respBytes, buf[:r]...)
				break
			}
			return nil, readErr
		}
		respBytes = append(respBytes, buf[:r]...)
		if r < bufferSize {
			break
		}
	}

	// Convert response bytes to http.Response
	respBuff := bytes.NewBuffer(respBytes)
	reader := bufio.NewReader(respBuff)
	resp, respErr := http.ReadResponse(reader, nil)
	if respErr != nil {
		log.Fatal("failed to parse response", respErr)
	}

	return resp, nil
}
