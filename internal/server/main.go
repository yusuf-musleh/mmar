package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"time"

	"github.com/yusuf-musleh/mmar/constants"
	"github.com/yusuf-musleh/mmar/internal/protocol"
	"github.com/yusuf-musleh/mmar/internal/utils"
)

type MmarServer struct {
	clients map[string]ClientTunnel
}

type IncomingRequest struct {
	responseChannel chan OutgoingResponse
	responseWriter  http.ResponseWriter
	request         *http.Request
	cancel          context.CancelFunc
}

type OutgoingResponse struct {
	statusCode int
	body       []byte
}

// Tunnel to Client
type ClientTunnel struct {
	protocol.Tunnel
	incomingChannel chan IncomingRequest
	outgoingChannel chan protocol.TunnelMessage
}

func (ct *ClientTunnel) close() {
	log.Printf("Client disconnected: %v, closing tunnel...", ct.Conn.LocalAddr().String())
	// Close the TunneledRequests channel
	close(ct.incomingChannel)
	// Clear the TunneledRequests channel
	ct.incomingChannel = nil
	// Close the TunneledResponses channel
	close(ct.outgoingChannel)
	// Clear the TunneledResponses channel
	ct.outgoingChannel = nil

	// Wait a little for final response to complete, then close the connection
	gracefulCloseTimer := time.NewTimer(constants.GRACEFUL_SHUTDOWN_TIMEOUT)
	<-gracefulCloseTimer.C
	ct.Conn.Close()
	log.Printf("Tunnel connection closed: %v", ct.Conn.LocalAddr().String())
}

func (ms *MmarServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s - %s%s", r.Method, html.EscapeString(r.URL.Path), r.URL.RawQuery)

	// Extract subdomain to retrieve related client tunnel
	subdomain := utils.ExtractSubdomain(r.Host)
	clientTunnel, clientExists := ms.clients[subdomain]

	if !clientExists {
		// Create a response for Tunnel closed/not connected
		resp := TunnelErrStateResp(protocol.CLIENT_DISCONNECT)
		w.WriteHeader(resp.StatusCode)
		respBody, _ := io.ReadAll(resp.Body)
		w.Write(respBody)
		return
	}

	// Create response channel for tunneled request
	respChannel := make(chan OutgoingResponse)

	// Check if the tunnel was closed, if so,
	// send back HTTP response right away
	if clientTunnel.incomingChannel == nil {
		// Create a response for Tunnel closed/not connected
		resp := TunnelErrStateResp(protocol.CLIENT_DISCONNECT)
		w.WriteHeader(resp.StatusCode)
		respBody, _ := io.ReadAll(resp.Body)
		w.Write(respBody)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())

	// Tunnel the request
	clientTunnel.incomingChannel <- IncomingRequest{
		responseChannel: respChannel,
		responseWriter:  w,
		request:         r,
		cancel:          cancel,
	}

	select {
	case <-ctx.Done(): // Tunnel is closed if context is cancelled
		// Create a response for Tunnel closed/not connected
		resp := TunnelErrStateResp(protocol.CLIENT_DISCONNECT)
		w.WriteHeader(resp.StatusCode)
		respBody, _ := io.ReadAll(resp.Body)
		w.Write(respBody)
		return
	case resp, _ := <-respChannel: // Await response for tunneled request
		// Write response headers with response status code to original client
		w.WriteHeader(resp.statusCode)

		// Write the response body to original client
		w.Write(resp.body)
	}
}

func (ms *MmarServer) GenerateUniqueId() string {
	reservedIDs := []string{"", "admin", "stats"}

	var randSeed *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	generatedId := ""
	for _, exists := ms.clients[generatedId]; exists || slices.Contains(reservedIDs, generatedId); {
		b := make([]byte, constants.ID_LENGTH)
		for i := range b {
			b[i] = constants.ID_CHARSET[randSeed.Intn(len(constants.ID_CHARSET))]
		}
		generatedId = string(b)
	}

	return generatedId
}

func (ms *MmarServer) newClientTunnel(conn net.Conn) *ClientTunnel {
	// Generate unique ID for client
	uniqueId := ms.GenerateUniqueId()
	tunnel := protocol.Tunnel{
		Id:   uniqueId,
		Conn: conn,
	}

	// Create channels to tunnel requests to and recieve responses from
	incomingChannel := make(chan IncomingRequest)
	outgoingChannel := make(chan protocol.TunnelMessage)

	// Create client tunnel
	clientTunnel := ClientTunnel{
		tunnel,
		incomingChannel,
		outgoingChannel,
	}

	// Add client tunnel to clients
	ms.clients[uniqueId] = clientTunnel

	// Send unique ID to client
	reqMessage := protocol.TunnelMessage{MsgType: protocol.CLIENT_CONNECT, MsgData: []byte(uniqueId)}
	if err := clientTunnel.SendMessage(reqMessage); err != nil {
		log.Fatal(err)
	}

	return &clientTunnel
}

func (ms *MmarServer) handleTcpConnection(conn net.Conn) {
	log.Printf("TCP Conn from %s", conn.LocalAddr().String())

	clientTunnel := ms.newClientTunnel(conn)

	// Process Tunnel Messages coming from mmar client
	go ms.processTunnelMessages(clientTunnel)

	// Start goroutine to process tunneled requests
	go ms.processTunneledRequests(clientTunnel)
}

func (ms *MmarServer) closeClientTunnel(ct *ClientTunnel) {
	delete(ms.clients, ct.Id)
	ct.close()
}

func TunnelErrStateResp(errState int) http.Response {
	// TODO: Have nicer/more elaborative error messages/pages
	errStates := map[int]string{
		protocol.CLIENT_DISCONNECT:     "Tunnel is closed, cannot connect to mmar client.",
		protocol.LOCALHOST_NOT_RUNNING: "Tunneled successfully, but nothing is running on localhost.",
	}
	errBody := errStates[errState]
	resp := http.Response{
		Status:        "200 OK",
		StatusCode:    200,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Body:          io.NopCloser(bytes.NewBufferString(errBody)),
		ContentLength: int64(len(errBody)),
	}
	return resp
}

func (ms *MmarServer) processTunneledRequests(ct *ClientTunnel) {
	for {
		// Read requests coming in tunnel channel
		incomingReq, ok := <-ct.incomingChannel
		if !ok {
			// Channel closed, client disconencted, shutdown goroutine
			return
		}

		// Writing request to buffer to forward it
		var requestBuff bytes.Buffer
		incomingReq.request.Write(&requestBuff)

		// Forward the request to mmar client
		reqMessage := protocol.TunnelMessage{MsgType: protocol.REQUEST, MsgData: requestBuff.Bytes()}
		if err := ct.SendMessage(reqMessage); err != nil {
			log.Fatal(err)
		}

		// Wait for response for this request to come back from outgoing channel
		respTunnelMsg, ok := <-ct.outgoingChannel
		if !ok {
			// Channel closed, client disconencted, shutdown goroutine
			return
		}

		// Read response for forwarded request
		respReader := bufio.NewReader(bytes.NewReader(respTunnelMsg.MsgData))
		resp, respErr := http.ReadResponse(respReader, incomingReq.request)

		if respErr != nil {
			if errors.Is(respErr, io.ErrUnexpectedEOF) || errors.Is(respErr, net.ErrClosed) {
				incomingReq.cancel()
				ms.closeClientTunnel(ct)
				return
			}
			failedReq := fmt.Sprintf("%s - %s%s", incomingReq.request.Method, html.EscapeString(incomingReq.request.URL.Path), incomingReq.request.URL.RawQuery)
			log.Fatalf("Failed to return response: %v\n\n for req: %v", respErr, failedReq)
		}

		respBody, respBodyErr := io.ReadAll(resp.Body)
		if respBodyErr != nil {
			log.Fatalf("Failed to parse response body: %v\n\n", respBodyErr)
			os.Exit(1)
		}

		// Set headers for response
		for hKey, hVal := range resp.Header {
			incomingReq.responseWriter.Header().Set(hKey, hVal[0])
			// Add remaining values for header if more than than one exists
			for i := 1; i < len(hVal); i++ {
				incomingReq.responseWriter.Header().Add(hKey, hVal[i])
			}
		}

		// Send response back to goroutine handling the request
		incomingReq.responseChannel <- OutgoingResponse{statusCode: resp.StatusCode, body: respBody}

		// Close response body
		resp.Body.Close()
	}
}

func (ms *MmarServer) processTunnelMessages(ct *ClientTunnel) {
	for {
		tunnelMsg, err := ct.ReceiveMessage()
		if err != nil {
			log.Fatalf("Failed to receive message from client tunnel: %v", err)
		}

		switch tunnelMsg.MsgType {
		case protocol.HEARTBEAT:
			// TODO: Need to implement some heartbeat logic here
			log.Printf("Got HEARTBEAT TUNNEL MESSAGE\n")
			continue
		case protocol.RESPONSE:
			log.Printf("Got RESPONSE TUNNEL MESSAGE\n")
			ct.outgoingChannel <- tunnelMsg
		case protocol.LOCALHOST_NOT_RUNNING:
			// Create a response for Tunnel connected but localhost not running
			resp := TunnelErrStateResp(protocol.LOCALHOST_NOT_RUNNING)
			// Writing response to buffer to tunnel it back
			var responseBuff bytes.Buffer
			resp.Write(&responseBuff)
			notRunningMsg := protocol.TunnelMessage{MsgType: protocol.RESPONSE, MsgData: responseBuff.Bytes()}
			ct.outgoingChannel <- notRunningMsg
		case protocol.CLIENT_DISCONNECT:
			log.Printf("Got CLIENT_DISCONNECT TUNNEL MESSAGE\n")
			ms.closeClientTunnel(ct)
			// ct.close()
			return
		}
	}
}

func Run(tcpPort string, httpPort string) {
	// Channel handler for interrupt signal
	sigInt := make(chan os.Signal, 1)
	signal.Notify(sigInt, os.Interrupt)

	mux := http.NewServeMux()

	// Initialize Mmar Server
	mmarServer := MmarServer{clients: map[string]ClientTunnel{}}
	mux.Handle("/", &mmarServer)

	go func() {
		log.Print("Listening for TCP Requests...")
		ln, err := net.Listen("tcp", fmt.Sprintf(":%s", tcpPort))
		if err != nil {
			log.Fatalf("Failed to start TCP server: %v", err)
			return
		}
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Fatalf("Failed to accept TCP connection: %v", err)
			}
			go mmarServer.handleTcpConnection(conn)
		}
	}()

	go func() {
		log.Print("Listening for HTTP Requests...")
		if err := http.ListenAndServe(fmt.Sprintf(":%s", httpPort), mux); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "Error listening and serving: %s\n", err)
		}
	}()

	// Wait for an interrupt signal, if received, terminate gracefully
	<-sigInt
	log.Printf("Gracefully shutting down server...")
}
