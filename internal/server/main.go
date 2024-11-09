package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
	"sync"
	"time"

	"github.com/yusuf-musleh/mmar/constants"
	"github.com/yusuf-musleh/mmar/internal/protocol"
	"github.com/yusuf-musleh/mmar/internal/utils"
)

var CLIENT_MAX_TUNNELS_REACHED = errors.New("Client reached max tunnels limit")

type MmarServer struct {
	mu           sync.Mutex
	clients      map[string]ClientTunnel
	tunnelsPerIP map[string][]string
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

func (ct *ClientTunnel) close(graceful bool) {
	log.Printf("Client disconnected: %v, closing tunnel...", ct.Conn.RemoteAddr().String())
	// Close the TunneledRequests channel
	close(ct.incomingChannel)
	// Clear the TunneledRequests channel
	ct.incomingChannel = nil
	// Close the TunneledResponses channel
	close(ct.outgoingChannel)
	// Clear the TunneledResponses channel
	ct.outgoingChannel = nil

	if graceful {
		// Wait a little for final response to complete, then close the connection
		gracefulCloseTimer := time.NewTimer(constants.GRACEFUL_SHUTDOWN_TIMEOUT)
		<-gracefulCloseTimer.C
	}

	ct.Conn.Close()
	log.Printf("Tunnel connection closed: %v", ct.Conn.RemoteAddr().String())
}

func (ms *MmarServer) handleServerStats(w http.ResponseWriter) {
	stats := map[string]any{}

	// Add total connected clients count
	stats["connectedClientsCount"] = len(ms.clients)

	// Add list of connected clients, including only relevant fields
	clientStats := []map[string]string{}
	for _, val := range ms.clients {
		client := map[string]string{
			"id":        val.Id,
			"createdOn": val.CreatedOn.Format(time.RFC3339),
		}
		clientStats = append(clientStats, client)
	}
	stats["connectedClients"] = clientStats

	// Marshal the result
	marshalledStats, err := json.Marshal(stats)

	if err != nil {
		log.Fatalf("Failed to marshal server stats: %v", err)
	}
	w.WriteHeader(200)
	w.Write(marshalledStats)
}

func (ms *MmarServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s - %s%s", r.Method, html.EscapeString(r.URL.Path), r.URL.RawQuery)

	// Extract subdomain to retrieve related client tunnel
	subdomain := utils.ExtractSubdomain(r.Host)

	// Handle stats subdomain
	if subdomain == "stats" {
		ms.handleServerStats(w)
		return
	}

	clientTunnel, clientExists := ms.clients[subdomain]

	if !clientExists {
		// Create a response for Tunnel closed/not connected
		resp := protocol.TunnelErrStateResp(protocol.CLIENT_DISCONNECT)
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
		resp := protocol.TunnelErrStateResp(protocol.CLIENT_DISCONNECT)
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
		resp := protocol.TunnelErrStateResp(protocol.CLIENT_DISCONNECT)
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

	generatedId := ""
	for _, exists := ms.clients[generatedId]; exists || slices.Contains(reservedIDs, generatedId); {
		var randSeed *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
		b := make([]byte, constants.ID_LENGTH)
		for i := range b {
			b[i] = constants.ID_CHARSET[randSeed.Intn(len(constants.ID_CHARSET))]
		}
		generatedId = string(b)
	}

	return generatedId
}

func (ms *MmarServer) TunnelLimitedIP(ip string) bool {
	tunnels, tunnelsExist := ms.tunnelsPerIP[ip]

	// Initialize tunnels list for IP
	if !tunnelsExist {
		ms.tunnelsPerIP[ip] = []string{}
	}

	return len(tunnels) >= constants.MAX_TUNNELS_PER_IP
}

func (ms *MmarServer) newClientTunnel(conn net.Conn) (*ClientTunnel, error) {
	// Acquire lock to create new client tunnel data
	ms.mu.Lock()

	// Generate unique ID for client
	uniqueId := ms.GenerateUniqueId()
	tunnel := protocol.Tunnel{
		Id:        uniqueId,
		Conn:      conn,
		CreatedOn: time.Now(),
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

	// Check if IP reached max tunnel limit
	clientIP := utils.ExtractIP(conn.RemoteAddr().String())
	limitedIP := ms.TunnelLimitedIP(clientIP)
	// If so, send limit message to client and close client tunnel
	if limitedIP {
		limitMessage := protocol.TunnelMessage{MsgType: protocol.CLIENT_TUNNEL_LIMIT}
		if err := clientTunnel.SendMessage(limitMessage); err != nil {
			log.Fatal(err)
		}
		clientTunnel.close(false)
		// Release lock once errored
		ms.mu.Unlock()
		return nil, CLIENT_MAX_TUNNELS_REACHED
	}

	// Add client tunnel to clients
	ms.clients[uniqueId] = clientTunnel

	// Associate tunnel with client IP
	ms.tunnelsPerIP[clientIP] = append(ms.tunnelsPerIP[clientIP], uniqueId)

	// Release lock once created
	ms.mu.Unlock()

	// Send unique ID to client
	reqMessage := protocol.TunnelMessage{MsgType: protocol.CLIENT_CONNECT, MsgData: []byte(uniqueId)}
	if err := clientTunnel.SendMessage(reqMessage); err != nil {
		log.Fatal(err)
	}

	return &clientTunnel, nil
}

func (ms *MmarServer) handleTcpConnection(conn net.Conn) {
	log.Printf("TCP Conn from %s", conn.RemoteAddr().String())

	clientTunnel, err := ms.newClientTunnel(conn)

	if err != nil {
		if errors.Is(err, CLIENT_MAX_TUNNELS_REACHED) {
			// Close the connection when client max tunnels limit reached
			conn.Close()
			return
		}
		log.Fatalf("Failed to create ClientTunnel: %v", err)
	}

	// Process Tunnel Messages coming from mmar client
	go ms.processTunnelMessages(clientTunnel)

	// Start goroutine to process tunneled requests
	go ms.processTunneledRequests(clientTunnel)
}

func (ms *MmarServer) closeClientTunnel(ct *ClientTunnel) {
	// Remove Client Tunnel from clients
	delete(ms.clients, ct.Id)

	// Remove Client Tunnel from client IP
	clientIP := utils.ExtractIP(ct.Conn.RemoteAddr().String())
	tunnels := ms.tunnelsPerIP[clientIP]
	index := slices.Index(tunnels, ct.Id)
	if index != -1 {
		tunnels = slices.Delete(tunnels, index, index+1)
		ms.tunnelsPerIP[clientIP] = tunnels
	}

	// Gracefully close the Client Tunnel
	ct.close(true)
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
		case protocol.RESPONSE:
			log.Printf("Got RESPONSE TUNNEL MESSAGE\n")
			ct.outgoingChannel <- tunnelMsg
		case protocol.LOCALHOST_NOT_RUNNING:
			// Create a response for Tunnel connected but localhost not running
			resp := protocol.TunnelErrStateResp(protocol.LOCALHOST_NOT_RUNNING)
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
	mmarServer := MmarServer{
		clients:      map[string]ClientTunnel{},
		tunnelsPerIP: map[string][]string{},
	}
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
