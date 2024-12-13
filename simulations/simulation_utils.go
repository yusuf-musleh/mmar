package simulations

import (
	"context"
	"encoding/json"
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
	body       map[string]interface{}
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
		log.Fatal("Failed to read response body", err)
	}

	expectedJson, _ := json.Marshal(expectedResp.body)
	actualJson, _ := json.Marshal(respBody)
	if string(actualJson) != string(expectedJson) {
		t.Errorf("%v = %v; want %v", testName, string(actualJson), string(expectedJson))
	}
}

func extractTunnelURL(clientStdout string) string {
	re := regexp.MustCompile(`http:\/\/[a-zA-Z0-9\-]+\.localhost:\d+`)
	return re.FindString(clientStdout)
}

func httpClient() *http.Client {
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
	tp := &http.Transport{
		DialContext: dial.DialContext,
	}
	client := &http.Client{Transport: tp}
	return client
}
