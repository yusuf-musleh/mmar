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
	"github.com/yusuf-musleh/mmar/internal/logger"
	"github.com/yusuf-musleh/mmar/internal/protocol"
	"github.com/yusuf-musleh/mmar/internal/utils"
)

var CLIENT_MAX_TUNNELS_REACHED = errors.New("Client reached max tunnels limit")

type ConfigOptions struct {
	HttpPort string
	TcpPort  string
}

type MmarServer struct {
	mu           sync.Mutex
	clients      map[string]ClientTunnel
	tunnelsPerIP map[string][]string
}

type IncomingRequest struct {
	responseChannel chan OutgoingResponse
	responseWriter  http.ResponseWriter
	request         *http.Request
	cancel          context.CancelCauseFunc
	serializedReq   []byte
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

func (ct *ClientTunnel) drainChannels() {
	// Drain all incoming requests to tunnel and cancel them
incomingDrainerLoop:
	for {
		select {
		case incoming, _ := <-ct.incomingChannel:
			// Cancel incoming requests
			incoming.cancel(CLIENT_DISCONNECTED_ERR)
		default:
			// Close the TunneledRequests channel
			close(ct.incomingChannel)
			break incomingDrainerLoop
		}
	}

	// Draining all outgoing requests from tunnel
OutgoingDrainerLoop:
	for {
		select {
		case <-ct.outgoingChannel:
			// Just draining, do nothing
		default:
			// Close the TunneledResponses channel
			close(ct.outgoingChannel)
			break OutgoingDrainerLoop
		}
	}
}

func (ct *ClientTunnel) close(graceful bool) {
	logger.Log(
		constants.DEFAULT_COLOR,
		fmt.Sprintf(
			"[%s] Client disconnected: %v, closing tunnel...",
			ct.Tunnel.Id,
			ct.Conn.RemoteAddr().String(),
		),
	)

	// Drain channels before closing them to prevent panics if there are blocked writes
	ct.drainChannels()

	if graceful {
		// Wait a little for final response to complete, then close the connection
		gracefulCloseTimer := time.NewTimer(constants.GRACEFUL_SHUTDOWN_TIMEOUT * time.Second)
		<-gracefulCloseTimer.C
	}

	ct.Conn.Close()
	logger.Log(
		constants.DEFAULT_COLOR,
		fmt.Sprintf(
			"[%s] Tunnel connection closed: %v",
			ct.Tunnel.Id,
			ct.Conn.RemoteAddr().String(),
		),
	)
}

// Serves simple stats for mmar server behind Basic Authentication
func (ms *MmarServer) handleServerStats(w http.ResponseWriter, r *http.Request) {
	// Check Basic Authentication
	username, password, ok := r.BasicAuth()
	if !ok || !utils.ValidCredentials(username, password) {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"stats\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

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
	w.WriteHeader(http.StatusOK)
	w.Write(marshalledStats)
}

func (ms *MmarServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Extract subdomain to retrieve related client tunnel
	subdomain := utils.ExtractSubdomain(r.Host)

	// Handle stats subdomain
	if subdomain == "stats" {
		ms.handleServerStats(w, r)
		return
	}

	clientTunnel, clientExists := ms.clients[subdomain]

	if !clientExists {
		protocol.RespondTunnelErr(protocol.CLIENT_DISCONNECT, w)
		return
	}

	// Create channel to receive serialized request
	serializedReqChannel := make(chan []byte)

	ctx, cancel := context.WithCancelCause(r.Context())

	// Writing request to buffer to forward it
	go serializeRequest(ctx, r, cancel, serializedReqChannel)

	select {
	case <-ctx.Done():
		// We could not serialize request, so we cancelled it
		handleCancel(context.Cause(ctx), w)
		return
	case serializedRequest, _ := <-serializedReqChannel:
		// Request serialized, we can proceed to tunnel it

		// Create response channel to receive response for tunneled request
		respChannel := make(chan OutgoingResponse)

		// Tunnel the request
		clientTunnel.incomingChannel <- IncomingRequest{
			responseChannel: respChannel,
			responseWriter:  w,
			request:         r,
			cancel:          cancel,
			serializedReq:   serializedRequest,
		}

		select {
		case <-ctx.Done(): // Request is canceled or Tunnel is closed if context is canceled
			handleCancel(context.Cause(ctx), w)
			return
		case resp, _ := <-respChannel: // Await response for tunneled request
			// Add header to close the connection
			w.Header().Set("Connection", "close")

			// Write response headers with response status code to original client
			w.WriteHeader(resp.statusCode)

			// Write the response body to original client
			w.Write(resp.body)
		}
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

	clientTunnel, err := ms.newClientTunnel(conn)

	if err != nil {
		if errors.Is(err, CLIENT_MAX_TUNNELS_REACHED) {
			// Close the connection when client max tunnels limit reached
			conn.Close()
			return
		}
		log.Fatalf("Failed to create ClientTunnel: %v", err)
	}

	logger.Log(
		constants.DEFAULT_COLOR,
		fmt.Sprintf(
			"[%s] Tunnel created: %s",
			clientTunnel.Tunnel.Id,
			conn.RemoteAddr().String(),
		),
	)

	// Process Tunnel Messages coming from mmar client
	go ms.processTunnelMessages(clientTunnel)

	// Start goroutine to process tunneled requests
	go ms.processTunneledRequestsForClient(clientTunnel)
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

func (ms *MmarServer) processTunneledRequestsForClient(ct *ClientTunnel) {
	for {
		// Read requests coming in tunnel channel
		incomingReq, ok := <-ct.incomingChannel
		if !ok {
			// Channel closed, client disconencted, shutdown goroutine
			return
		}

		// Forward the request to mmar client
		reqMessage := protocol.TunnelMessage{MsgType: protocol.REQUEST, MsgData: incomingReq.serializedReq}
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
				incomingReq.cancel(CLIENT_DISCONNECTED_ERR)
				ms.closeClientTunnel(ct)
				return
			}
			// TODO: Needs to be cleaned up/refactored
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

		// Send response data back
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
			ct.outgoingChannel <- tunnelMsg
		case protocol.LOCALHOST_NOT_RUNNING:
			// Create a response for Tunnel connected but localhost not running
			errState := protocol.TunnelErrState(protocol.LOCALHOST_NOT_RUNNING)
			resp := http.Response{
				Status:     "200 OK",
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(errState)),
			}

			// Writing response to buffer to tunnel it back
			var responseBuff bytes.Buffer
			resp.Write(&responseBuff)
			notRunningMsg := protocol.TunnelMessage{MsgType: protocol.RESPONSE, MsgData: responseBuff.Bytes()}
			ct.outgoingChannel <- notRunningMsg
		case protocol.CLIENT_DISCONNECT:
			ms.closeClientTunnel(ct)
			return
		}
	}
}

func Run(config ConfigOptions) {
	logger.LogStartMmarServer(config.TcpPort, config.HttpPort)

	// Channel handler for interrupt signal
	sigInt := make(chan os.Signal, 1)
	signal.Notify(sigInt, os.Interrupt)

	mux := http.NewServeMux()

	// Initialize Mmar Server
	mmarServer := MmarServer{
		clients:      map[string]ClientTunnel{},
		tunnelsPerIP: map[string][]string{},
	}
	mux.Handle("/", logger.LoggerMiddleware(&mmarServer))

	go func() {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%s", config.TcpPort))
		if err != nil {
			log.Fatalf("Failed to start TCP server: %v", err)
			return
		}
		logger.Log(
			constants.DEFAULT_COLOR,
			fmt.Sprintf(
				"TCP Server started successfully!\nListening for TCP Connections on port %s...",
				config.TcpPort,
			),
		)

		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Fatalf("Failed to accept TCP connection: %v", err)
			}
			go mmarServer.handleTcpConnection(conn)
		}
	}()

	go func() {
		logger.Log(
			constants.DEFAULT_COLOR,
			fmt.Sprintf(
				"HTTP Server started successfully!\nListening for HTTP Requests on %s...",
				config.HttpPort,
			),
		)
		if err := http.ListenAndServe(fmt.Sprintf(":%s", config.HttpPort), mux); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "Error listening and serving: %s\n", err)
		}
	}()

	// Wait for an interrupt signal, if received, terminate gracefully
	<-sigInt
	log.Printf("Gracefully shutting down server...")
}
