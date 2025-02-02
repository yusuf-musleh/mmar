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
	"sync"
	"testing"
	"time"

	"github.com/yusuf-musleh/mmar/constants"
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
func verifyGetRequestSuccess(t *testing.T, client *http.Client, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
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
		jsonBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyGetRequestSuccess")
}

// Test to verify failed GET request through mmar tunnel returned expected request/response
func verifyGetRequestFail(t *testing.T, client *http.Client, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
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
		jsonBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyGetRequestFail")
}

// Test to verify successful POST request through mmar tunnel returned expected request/response
func verifyPostRequestSuccess(t *testing.T, client *http.Client, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
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
		jsonBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyPostRequestSuccess")
}

// Test to verify failed POST request through mmar tunnel returned expected request/response
func verifyPostRequestFail(t *testing.T, client *http.Client, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
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
		jsonBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyPostRequestFail")
}

// Test to verify a HTTP request with an invalid method is handled
func verifyInvalidMethodRequestHandled(t *testing.T, client *http.Client, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
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
		jsonBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyInvalidMethodRequestHandled")
}

// Test to verify a HTTP request with invalid headers is handled
func verifyInvalidHeadersRequestHandled(t *testing.T, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
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
func verifyInvalidHttpVersionRequestHandled(t *testing.T, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
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
func verifyInvalidContentLengthRequestHandled(t *testing.T, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
	dialUrl := strings.Replace(tunnelUrl, "http://", "", 1)

	// Write a raw HTTP request with an invalid Content-Length header
	req := "POST /post HTTP/1.1\r\n" +
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

// Test to verify a HTTP request with a mismatched Content-Length header
func verifyMismatchedContentLengthRequestHandled(t *testing.T, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
	dialUrl := strings.Replace(tunnelUrl, "http://", "", 1)

	serializedBody := "{\"message\": \"Hello\"}\r\n"

	// Write a raw HTTP request with an invalid Content-Length header
	req := "POST /post HTTP/1.1\r\n" +
		"Host: " + dialUrl + "\r\n" +
		"Content-Length: 25\r\n" + // Mismatched Content-Length header, should be 20
		"Simulation-Test: verify-mismatched-content-length-request-handled\r\n" +
		"\r\n" +
		serializedBody

	// Manually perform request and read response for mismatched content-length
	conn := manualHttpRequest(dialUrl, req)
	resp, respErr := manualReadResponse(conn)

	if respErr != nil {
		t.Errorf("%v: Failed to get response %v", "verifyMismatchedContentLengthRequestHandled", respErr)
	}

	expectedBody := constants.READ_BODY_CHUNK_TIMEOUT_ERR_TEXT

	expectedResp := expectedResponse{
		statusCode: http.StatusRequestTimeout,
		headers: map[string]string{
			"Content-Length": strconv.Itoa(len(expectedBody)),
			"Content-Type":   "text/plain; charset=utf-8",
		},
		textBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyMismatchedContentLengthRequestHandled")
}

// Test to verify a HTTP request with a Content-Length header but no body
func verifyContentLengthWithNoBodyRequestHandled(t *testing.T, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
	dialUrl := strings.Replace(tunnelUrl, "http://", "", 1)

	// Write a raw HTTP request with Content-Length header but no body
	req := "POST /post HTTP/1.1\r\n" +
		"Host: " + dialUrl + "\r\n" +
		"Content-Length: 25\r\n" + // Content-Length header provided but no body
		"Simulation-Test: verify-content-length-with-no-body-request-handled\r\n" +
		"\r\n"

	// Manually perform request and read response
	conn := manualHttpRequest(dialUrl, req)
	resp, respErr := manualReadResponse(conn)

	if respErr != nil {
		t.Errorf("%v: Failed to get response %v", "verifyContentLengthWithNoBodyRequestHandled", respErr)
	}

	expectedBody := constants.READ_BODY_CHUNK_TIMEOUT_ERR_TEXT

	expectedResp := expectedResponse{
		statusCode: http.StatusRequestTimeout,
		headers: map[string]string{
			"Content-Length": strconv.Itoa(len(expectedBody)),
			"Content-Type":   "text/plain; charset=utf-8",
		},
		textBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyContentLengthWithNoBodyRequestHandled")
}

// Test to verify a HTTP request with a large body but still within the limit
func verifyRequestWithLargeBody(t *testing.T, client *http.Client, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
	littleUnderTenMb := 999989
	reqBody := map[string]interface{}{
		"success": true,
		"payload": make([]byte, littleUnderTenMb),
	}

	serializedReqBody, _ := json.Marshal(reqBody)
	req, reqErr := http.NewRequest("POST", tunnelUrl+devserver.POST_SUCCESS_URL, bytes.NewBuffer(serializedReqBody))
	if reqErr != nil {
		log.Fatalf("Failed to create new request: %v", reqErr)
	}
	// Adding custom header to confirm that they are propogated when going through mmar
	req.Header.Set("Simulation-Test", "verify-large-post-request-success")

	resp, respErr := client.Do(req)
	if respErr != nil {
		log.Printf("Failed to get response: %v", respErr)
	}

	expectedReqHeaders := map[string][]string{
		"User-Agent":      {"Go-http-client/1.1"}, // Default header in golang client
		"Accept-Encoding": {"gzip"},               // Default header in golang client
		"Simulation-Test": {"verify-large-post-request-success"},
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
		jsonBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyRequestWithLargeBody")
}

// Test to verify a HTTP request with a very large body, over the 10mb limit
func verifyRequestWithVeryLargeBody(t *testing.T, client *http.Client, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
	hundredMb := 100000000
	reqBody := map[string]interface{}{
		"success": true,
		"payload": make([]byte, hundredMb),
	}

	serializedReqBody, _ := json.Marshal(reqBody)
	req, reqErr := http.NewRequest("POST", tunnelUrl+devserver.POST_SUCCESS_URL, bytes.NewBuffer(serializedReqBody))
	if reqErr != nil {
		log.Fatalf("Failed to create new request: %v", reqErr)
	}
	// Adding custom header to confirm that they are propogated when going through mmar
	req.Header.Set("Simulation-Test", "verify-very-large-post-request-success")

	resp, respErr := client.Do(req)
	if respErr != nil {
		log.Printf("Failed to get response: %v", respErr)
	}

	expectedBody := constants.MAX_REQ_BODY_SIZE_ERR_TEXT

	expectedResp := expectedResponse{
		statusCode: http.StatusRequestEntityTooLarge,
		headers: map[string]string{
			"Content-Length": strconv.Itoa(len(expectedBody)),
			"Content-Type":   "text/plain; charset=utf-8",
		},
		textBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyRequestWithVeryLargeBody")
}

// Test to verify that mmar handles invalid response from dev server gracefully
func verifyDevServerReturningInvalidRespHandled(t *testing.T, client *http.Client, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
	req, reqErr := http.NewRequest("GET", tunnelUrl+devserver.BAD_RESPONSE_URL, nil)
	if reqErr != nil {
		log.Fatalf("Failed to create new request: %v", reqErr)
	}

	resp, respErr := client.Do(req)
	if respErr != nil {
		log.Printf("Failed to get response: %v", respErr)
	}

	expectedBody := constants.READ_RESP_BODY_ERR_TEXT

	expectedResp := expectedResponse{
		statusCode: http.StatusInternalServerError,
		headers: map[string]string{
			"Content-Length": strconv.Itoa(len(expectedBody)),
			"Content-Type":   "text/plain; charset=utf-8",
		},
		textBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyDevServerReturningInvalidRespHandled")
}

// Test to verify that mmar timesout if devserver takes too long to respond
func verifyDevServerLongRunningReqHandledGradefully(t *testing.T, client *http.Client, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
	req, reqErr := http.NewRequest("GET", tunnelUrl+devserver.LONG_RUNNING_URL, nil)
	if reqErr != nil {
		log.Fatalf("Failed to create new request: %v", reqErr)
	}

	resp, respErr := client.Do(req)
	if respErr != nil {
		log.Printf("Failed to get response: %v", respErr)
	}

	expectedBody := constants.DEST_REQUEST_TIMEDOUT_ERR_TEXT

	expectedResp := expectedResponse{
		statusCode: http.StatusOK,
		headers: map[string]string{
			"Content-Length": strconv.Itoa(len(expectedBody)),
			"Content-Type":   "text/plain; charset=utf-8",
		},
		textBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyDevServerLongRunningReqHandledGradefully")
}

// Test to verify that mmar handles crashes in the devserver gracefully
func verifyDevServerCrashHandledGracefully(t *testing.T, client *http.Client, tunnelUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
	req, reqErr := http.NewRequest("GET", tunnelUrl+devserver.CRASH_URL, nil)
	if reqErr != nil {
		log.Fatalf("Failed to create new request: %v", reqErr)
	}

	resp, respErr := client.Do(req)
	if respErr != nil {
		log.Printf("Failed to get response: %v", respErr)
	}

	expectedBody := constants.LOCALHOST_NOT_RUNNING_ERR_TEXT

	expectedResp := expectedResponse{
		statusCode: http.StatusOK,
		headers: map[string]string{
			"Content-Length": strconv.Itoa(len(expectedBody)),
			"Content-Type":   "text/plain; charset=utf-8",
		},
		textBody: expectedBody,
	}

	validateRequestResponse(t, expectedResp, resp, "verifyDevServerCrashHandledGracefully")
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

	var wg sync.WaitGroup
	wg.Add(15)

	// Perform simulated usage tests
	go verifyGetRequestSuccess(t, client, tunnelUrl, &wg)
	go verifyGetRequestFail(t, client, tunnelUrl, &wg)
	go verifyPostRequestSuccess(t, client, tunnelUrl, &wg)
	go verifyPostRequestFail(t, client, tunnelUrl, &wg)

	// Perform Invalid HTTP requests to test durability of mmar
	go verifyInvalidMethodRequestHandled(t, client, tunnelUrl, &wg)
	go verifyInvalidHeadersRequestHandled(t, tunnelUrl, &wg)
	go verifyInvalidHttpVersionRequestHandled(t, tunnelUrl, &wg)
	go verifyInvalidContentLengthRequestHandled(t, tunnelUrl, &wg)
	go verifyMismatchedContentLengthRequestHandled(t, tunnelUrl, &wg)
	go verifyContentLengthWithNoBodyRequestHandled(t, tunnelUrl, &wg)
	go verifyRequestWithLargeBody(t, client, tunnelUrl, &wg)

	// Perform edge case usage tests
	go verifyRequestWithVeryLargeBody(t, client, tunnelUrl, &wg)
	go verifyDevServerReturningInvalidRespHandled(t, client, tunnelUrl, &wg)
	go verifyDevServerLongRunningReqHandledGradefully(t, client, tunnelUrl, &wg)
	go verifyDevServerCrashHandledGracefully(t, client, tunnelUrl, &wg)

	wg.Wait()

	// Stop simulation tests
	simulationCancel()

	wait.Reset(6 * time.Second)
	<-wait.C
}
