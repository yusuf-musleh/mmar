package simulations

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yusuf-musleh/mmar/simulations/devserver"
	"github.com/yusuf-musleh/mmar/simulations/dnsserver"
)

func StartMmarServer(ctx context.Context) {
	cmd := exec.CommandContext(ctx, "./mmar", "server")

	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.Cancel = func() error {
		fmt.Println("cancelled, server")
		wait := time.NewTimer(4 * time.Second)
		<-wait.C
		return cmd.Process.Signal(os.Interrupt)
	}

	err := cmd.Run()
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func StartMmarClient(ctx context.Context, urlCh chan string, localDevServerPort string) {
	cmd := exec.CommandContext(
		ctx,
		"./mmar",
		"client",
		"--tunnel-host",
		"localhost",
		"--local-port",
		localDevServerPort,
	)

	// Pipe Stderr To capture logs for extracting the tunnel url
	pipe, _ := cmd.StderrPipe()

	cmd.Cancel = func() error {
		fmt.Println("cancelled, client")
		return cmd.Process.Signal(os.Interrupt)
	}

	err := cmd.Start()
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal("Failed to start", err)
	}

	// Read through the logs (stderr), print them and extract the tunnel url
	// to send back through the channel
	go func() {
		stdoutReader := bufio.NewReader(pipe)
		line, readErr := stdoutReader.ReadString('\n')
		for readErr == nil {
			fmt.Print(line)
			tunnelUrl := extractTunnelURL(line)
			if tunnelUrl != "" {
				urlCh <- tunnelUrl
				break
			}
			line, readErr = stdoutReader.ReadString('\n')
		}
		// Print extra line at the end
		fmt.Println()
	}()

	waitErr := cmd.Wait()
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		log.Fatal("Failed to wait", waitErr)
	}
}

func StartLocalDevServer() *devserver.DevServer {
	ds := devserver.NewDevServer()
	log.Printf("Started local dev server on: http://localhost:%v", ds.Port())
	return ds
}

// Test to verify successful GET request through mmar tunnel returned expected request/response
func verifyGetRequestSuccess(t *testing.T, client *http.Client, tunnelUrl string) {
	req, reqErr := http.NewRequest("GET", tunnelUrl+devserver.GET_SUCCESS_URL, nil)
	if reqErr != nil {
		log.Fatalf("Failed to create new request: %v", reqErr)
	}
	// Adding custom header to confirm that they are propogated when going through mmar
	req.Header.Set("Simulation-Test", "verify-get-request-success")

	resp, respErr := client.Do(req)
	if respErr != nil {
		log.Printf("Failed to get response: %v", respErr)
	}

	expectedReqHeaders := map[string][]string{
		"User-Agent":      {"Go-http-client/1.1"}, // Default header in golang client
		"Accept-Encoding": {"gzip"},               // Default header in golang client
		"Simulation-Test": {"verify-get-request-success"},
	}

	expectedBody := map[string]interface{}{
		"success": true,
		"data":    "some data",
		"echo": map[string]interface{}{
			"reqHeaders": expectedReqHeaders,
		},
	}
	marshaledBody, _ := json.Marshal(expectedBody)

	expectedResp := expectedResponse{
		statusCode: http.StatusOK,
		headers: map[string]string{
			"Content-Length":    strconv.Itoa(len(marshaledBody)),
			"Content-Type":      "application/json",
			"Simulation-Header": "devserver-handle-get",
		},
		body: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyGetRequestSuccess")
}

// Test to verify failed GET request through mmar tunnel returned expected request/response
func verifyGetRequestFail(t *testing.T, client *http.Client, tunnelUrl string) {
	req, reqErr := http.NewRequest("GET", tunnelUrl+devserver.GET_FAILURE_URL, nil)
	if reqErr != nil {
		log.Fatalf("Failed to create new request: %v", reqErr)
	}
	// Adding custom header to confirm that they are propogated when going through mmar
	req.Header.Set("Simulation-Test", "verify-get-request-fail")

	resp, respErr := client.Do(req)
	if respErr != nil {
		log.Printf("Failed to get response: %v", respErr)
	}

	expectedReqHeaders := map[string][]string{
		"User-Agent":      {"Go-http-client/1.1"}, // Default header in golang client
		"Accept-Encoding": {"gzip"},               // Default header in golang client
		"Simulation-Test": {"verify-get-request-fail"},
	}

	expectedBody := map[string]interface{}{
		"success": false,
		"error":   "Sent bad GET request",
		"echo": map[string]interface{}{
			"reqHeaders": expectedReqHeaders,
		},
	}
	marshaledBody, _ := json.Marshal(expectedBody)

	expectedResp := expectedResponse{
		statusCode: http.StatusBadRequest,
		headers: map[string]string{
			"Content-Length":    strconv.Itoa(len(marshaledBody)),
			"Content-Type":      "application/json",
			"Simulation-Header": "devserver-handle-get-fail",
		},
		body: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyGetRequestFail")
}

// Test to verify successful POST request through mmar tunnel returned expected request/response
func verifyPostRequestSuccess(t *testing.T, client *http.Client, tunnelUrl string) {
	reqBody := map[string]interface{}{
		"success": true,
		"payload": map[string]interface{}{
			"some":     "data",
			"moreData": 123,
		},
	}
	serializedReqBody, _ := json.Marshal(reqBody)
	req, reqErr := http.NewRequest("POST", tunnelUrl+devserver.POST_SUCCESS_URL, bytes.NewBuffer(serializedReqBody))
	if reqErr != nil {
		log.Fatalf("Failed to create new request: %v", reqErr)
	}
	// Adding custom header to confirm that they are propogated when going through mmar
	req.Header.Set("Simulation-Test", "verify-post-request-success")

	resp, respErr := client.Do(req)
	if respErr != nil {
		log.Printf("Failed to get response: %v", respErr)
	}

	expectedReqHeaders := map[string][]string{
		"User-Agent":      {"Go-http-client/1.1"}, // Default header in golang client
		"Accept-Encoding": {"gzip"},               // Default header in golang client
		"Simulation-Test": {"verify-post-request-success"},
		"Content-Length":  {strconv.Itoa(len(serializedReqBody))},
	}

	expectedBody := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"posted": "data",
		},
		"echo": map[string]interface{}{
			"reqHeaders": expectedReqHeaders,
			"reqBody":    reqBody,
		},
	}
	marshaledBody, _ := json.Marshal(expectedBody)

	expectedResp := expectedResponse{
		statusCode: http.StatusOK,
		headers: map[string]string{
			"Content-Length":    strconv.Itoa(len(marshaledBody)),
			"Content-Type":      "application/json",
			"Simulation-Header": "devserver-handle-post-success",
		},
		body: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyPostRequestSuccess")
}

// Test to verify failed POST request through mmar tunnel returned expected request/response
func verifyPostRequestFail(t *testing.T, client *http.Client, tunnelUrl string) {
	reqBody := map[string]interface{}{
		"success": false,
		"payload": map[string]interface{}{
			"some":     "data",
			"moreData": 123,
		},
	}
	serializedReqBody, _ := json.Marshal(reqBody)
	req, reqErr := http.NewRequest("POST", tunnelUrl+devserver.POST_FAILURE_URL, bytes.NewBuffer(serializedReqBody))
	if reqErr != nil {
		log.Fatalf("Failed to create new request: %v", reqErr)
	}
	// Adding custom header to confirm that they are propogated when going through mmar
	req.Header.Set("Simulation-Test", "verify-post-request-fail")

	resp, respErr := client.Do(req)
	if respErr != nil {
		log.Printf("Failed to get response: %v", respErr)
	}

	expectedReqHeaders := map[string][]string{
		"User-Agent":      {"Go-http-client/1.1"}, // Default header in golang client
		"Accept-Encoding": {"gzip"},               // Default header in golang client
		"Simulation-Test": {"verify-post-request-fail"},
		"Content-Length":  {strconv.Itoa(len(serializedReqBody))},
	}

	expectedBody := map[string]interface{}{
		"success": false,
		"error":   "Sent bad POST request",
		"echo": map[string]interface{}{
			"reqHeaders": expectedReqHeaders,
			"reqBody":    reqBody,
		},
	}
	marshaledBody, _ := json.Marshal(expectedBody)

	expectedResp := expectedResponse{
		statusCode: http.StatusBadRequest,
		headers: map[string]string{
			"Content-Length":    strconv.Itoa(len(marshaledBody)),
			"Content-Type":      "application/json",
			"Simulation-Header": "devserver-handle-post-fail",
		},
		body: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyPostRequestFail")
}

// Test to verify a HTTP request with an invalid method is handled
func verifyInvalidMethodRequestHandled(t *testing.T, client *http.Client, tunnelUrl string) {
	url, urlErr := url.Parse(tunnelUrl + devserver.GET_SUCCESS_URL)
	if urlErr != nil {
		log.Fatal("Failed to create url", urlErr)
	}

	req := &http.Request{
		Method: "INVALID_METHOD",
		URL:    url,
		Header: make(http.Header),
	}
	// Adding custom header to confirm that they are propogated when going through mmar
	req.Header.Set("Simulation-Test", "verify-invalid-method-request")

	resp, respErr := client.Do(req)
	if respErr != nil {
		t.Errorf("Failed to get response %v", respErr)
	}

	// Using the same validation for "verifyGetRequestSuccess" since we are
	// hitting the same endpoint
	expectedReqHeaders := map[string][]string{
		"User-Agent":      {"Go-http-client/1.1"}, // Default header in golang client
		"Accept-Encoding": {"gzip"},               // Default header in golang client
		"Simulation-Test": {"verify-invalid-method-request"},
	}

	expectedBody := map[string]interface{}{
		"success": true,
		"data":    "some data",
		"echo": map[string]interface{}{
			"reqHeaders": expectedReqHeaders,
		},
	}
	marshaledBody, _ := json.Marshal(expectedBody)

	expectedResp := expectedResponse{
		statusCode: http.StatusOK,
		headers: map[string]string{
			"Content-Length":    strconv.Itoa(len(marshaledBody)),
			"Content-Type":      "application/json",
			"Simulation-Header": "devserver-handle-get",
		},
		body: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyInvalidMethodRequestHandled")
}

// Test to verify a HTTP request with invalid headers is handled
func verifyInvalidHeadersRequestHandled(t *testing.T, tunnelUrl string) {
	dialUrl := strings.Replace(tunnelUrl, "http://", "", 1)

	// Write a raw HTTP request with an invalid header
	req := "GET / HTTP/1.1\r\n" +
		"Host: " + dialUrl + "\r\n" +
		":Invalid-Header: :value\r\n" + // Header that starts with a colon (invalid)
		"\r\n"

	// Manually perform request and read response
	conn := manualHttpRequest(dialUrl, req)
	resp, respErr := manualReadResponse(conn)

	if respErr != nil {
		t.Errorf("%v: Failed to get response %v", "verifyInvalidHeadersRequestHandled", respErr)
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf(
			"%v: resp.StatusCode = %v; want %v",
			"verifyInvalidHeadersRequestHandled",
			resp.StatusCode,
			http.StatusBadRequest,
		)
	}
}

// Test to verify a HTTP request with invalid protocol version is handled
func verifyInvalidHttpVersionRequestHandled(t *testing.T, tunnelUrl string) {
	dialUrl := strings.Replace(tunnelUrl, "http://", "", 1)

	// Write a raw HTTP request with an invalid HTTP version
	req := "GET / HTTP/2.0.1\r\n" + // Invalid HTTP version
		"Host: " + dialUrl + "\r\n" +
		"\r\n"

	// Manually perform request and read response
	conn := manualHttpRequest(dialUrl, req)
	resp, respErr := manualReadResponse(conn)

	if respErr != nil {
		t.Errorf("%v: Failed to get response %v", "verifyInvalidHttpVersionRequestHandled", respErr)
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf(
			"%v: resp.StatusCode = %v; want %v",
			"verifyInvalidHttpVersionRequestHandled",
			resp.StatusCode,
			http.StatusBadRequest,
		)
	}
}

// Test to verify a HTTP request with a invalid Content-Length header
func verifyInvalidContentLengthRequestHandled(t *testing.T, tunnelUrl string) {
	dialUrl := strings.Replace(tunnelUrl, "http://", "", 1)

	// Write a raw HTTP request with an invalid Content-Length header
	req := "GET / HTTP/1.1\r\n" +
		"Host: " + dialUrl + "\r\n" +
		"Content-Length: abc" + "\r\n" + // Invalid Content-Length header
		"\r\n"

	// Manually perform request and read response for invalid Content-Length
	conn := manualHttpRequest(dialUrl, req)
	resp, respErr := manualReadResponse(conn)

	if respErr != nil {
		t.Errorf("%v: Failed to get response %v", "verifyInvalidContentLengthRequestHandled", respErr)
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf(
			"%v: resp.StatusCode = %v; want %v",
			"verifyInvalidContentLengthRequestHandled",
			resp.StatusCode,
			http.StatusBadRequest,
		)
	}
}

func TestSimulation(t *testing.T) {
	simulationCtx, simulationCancel := context.WithCancel(context.Background())

	localDevServer := StartLocalDevServer()
	defer localDevServer.Close()

	go dnsserver.StartDnsServer()

	go StartMmarServer(simulationCtx)
	wait := time.NewTimer(2 * time.Second)
	<-wait.C
	clientUrlCh := make(chan string)
	go StartMmarClient(simulationCtx, clientUrlCh, localDevServer.Port())

	// Wait for tunnel url
	tunnelUrl := <-clientUrlCh

	// Initialize http client
	client := httpClient()

	// Perform simulated usage tests
	verifyGetRequestSuccess(t, client, tunnelUrl)
	verifyGetRequestFail(t, client, tunnelUrl)
	verifyPostRequestSuccess(t, client, tunnelUrl)
	verifyPostRequestFail(t, client, tunnelUrl)

	// Perform Invalid HTTP requests to test durability of mmar
	verifyInvalidMethodRequestHandled(t, client, tunnelUrl)
	verifyInvalidHeadersRequestHandled(t, tunnelUrl)
	verifyInvalidHttpVersionRequestHandled(t, tunnelUrl)
	verifyInvalidContentLengthRequestHandled(t, tunnelUrl)

	// Stop simulation tests
	simulationCancel()

	wait.Reset(6 * time.Second)
	<-wait.C
}
