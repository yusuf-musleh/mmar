package simulations

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"reflect"
	"regexp"
	"slices"
	"testing"

	"github.com/yusuf-musleh/mmar/simulations/dnsserver"
)

type expectedResponse struct {
	statusCode int
	headers    map[string]string
	body       map[string]interface{}
}

func validateResponse(t *testing.T, expectedResp expectedResponse, resp *http.Response) {
	// Verify expected status code returned
	if resp.StatusCode != expectedResp.statusCode {
		t.Errorf("verifyGetRequestSuccess: resp.statusCode = %v; want %v", resp.StatusCode, expectedResp.statusCode)
	}

	// Verify contains expected headers
	for hKey, hVal := range expectedResp.headers {
		vals, ok := resp.Header[hKey]
		if !ok || slices.Index(vals, hVal) == -1 {
			t.Errorf("verifyGetRequestSuccess: resp.headers[%v] = %v; want %v", hKey, vals, hVal)
		}
	}

	// Verify correct body returned
	var respBody interface{}
	jsonDecoder := json.NewDecoder(resp.Body)
	err := jsonDecoder.Decode(&respBody)
	if err != nil {
		log.Fatal("Failed to read response body", err)
	}

	bodyEqual := reflect.DeepEqual(respBody, expectedResp.body)

	if !bodyEqual {
		expectedJson, _ := json.Marshal(expectedResp.body)
		actualJson, _ := json.Marshal(respBody)
		t.Errorf("verifyGetRequestSuccess = %v; want %v", string(actualJson), string(expectedJson))
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
