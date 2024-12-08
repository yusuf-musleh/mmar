package simulations

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
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
			tunnelUrl := ExtractTunnelURL(line)
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

func TestSimulation(t *testing.T) {
	simulationCtx, simulationCancel := context.WithCancel(context.Background())

	localDevServer := StartLocalDevServer()
	defer localDevServer.Close()

	go dnsserver.StartDnsServer()

	go StartMmarServer(simulationCtx)
	wait := time.NewTimer(2 * time.Second)
	<-wait.C
	wait.Reset(10 * time.Second)
	clientUrlCh := make(chan string)
	go StartMmarClient(simulationCtx, clientUrlCh, localDevServer.Port())

	// Wait for tunnel url
	tunnelUrl := <-clientUrlCh

	// Adding custom resolver that points to our DNS Server
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

	req, reqErr := http.NewRequest("GET", tunnelUrl+"/get", nil)
	if reqErr != nil {
		log.Fatalf("Failed to create new request: %v", reqErr)
	}

	resp, respErr := client.Do(req)
	if respErr != nil {
		log.Printf("Failed to get response: %v", respErr)
	}

	var respData map[string]interface{}
	allBody, _ := io.ReadAll(resp.Body)
	json.Unmarshal(allBody, &respData)
	fmt.Println(respData)

	<-wait.C
	simulationCancel()

	wait.Reset(6 * time.Second)
	<-wait.C
}
