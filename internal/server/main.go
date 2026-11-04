package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
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
	responseChannel   chan OutgoingResponse
	responseWriter    http.ResponseWriter
	outgoingWsChannel chan WebsocketData
	request           *http.Request
	cancel            context.CancelCauseFunc
	ctx               context.Context
}

type OutgoingResponse struct {
	statusCode int
	body       []byte
}

type RequestId uint32

type WebsocketData struct {
	bytes []byte
}

// Tunnel to Client
type ClientTunnel struct {
	protocol.Tunnel
	inflightRequests *sync.Map
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

// Generate unique request id for incoming request for client
func (ct *ClientTunnel) GenerateUniqueRequestID() RequestId {
	var generatedReqId RequestId

	for _, exists := ct.inflightRequests.Load(generatedReqId); exists || generatedReqId == 0; {
		generatedReqId = RequestId(GenerateRandomUint32())
	}
	return generatedReqId
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
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to marshal server stats: %v", err))
		w.WriteHeader(http.StatusInternalServerError)
		return
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

		// Create a websocket channel to receive websocket data from tunnel
		outgoingWsChannel := make(chan WebsocketData, constants.WEBSOCKET_CHANNEL_BUFFER_SZE)

		// Add request to client's inflight requests
		reqId := clientTunnel.GenerateUniqueRequestID()
		incomingReq := IncomingRequest{
			responseChannel:   respChannel,
			responseWriter:    w,
			outgoingWsChannel: outgoingWsChannel,
			request:           r,
			cancel:            cancel,
			ctx:               ctx,
		}
		// TODO: move the store to one shot LoadOrStore, possibly inside GenerateUniqueRequestID
		clientTunnel.inflightRequests.Store(reqId, incomingReq)

		// Construct Request message data
		reqIdBuff := make([]byte, constants.REQUEST_ID_BUFF_SIZE)
		binary.LittleEndian.PutUint32(reqIdBuff, uint32(reqId))
		reqMsgData := append(reqIdBuff, serializedRequest...)

		// Tunnel the request to mmar client
		reqMessage := protocol.TunnelMessage{MsgType: protocol.REQUEST, MsgData: reqMsgData}
		if err := clientTunnel.SendMessage(reqMessage); err != nil {
			logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to send Request msg to client: %v", err))
			cancel(FAILED_TO_FORWARD_TO_MMAR_CLIENT_ERR)
		}

		select {
		case <-ctx.Done(): // Request is canceled or Tunnel is closed if context is canceled
			handleCancel(context.Cause(ctx), w)
			clientTunnel.inflightRequests.Delete(reqId)
			return
		case resp, _ := <-respChannel: // Await response for tunneled request
			// Websockets/Switching protocol
			if resp.statusCode == http.StatusSwitchingProtocols {
				w.WriteHeader(resp.statusCode)

				// Write the response body to original client
				w.Write(resp.body)

				hj, ok := w.(http.Hijacker)
				if !ok {
					log.Fatalln(w, "webserver doesn't support hijacking", http.StatusInternalServerError)
					return
				}
				conn, bufrw, err := hj.Hijack()
				if err != nil {
					log.Fatalln(w, err.Error(), http.StatusInternalServerError)
					return
				}
				defer conn.Close()

				// Fire goroutine to read websocket data from external client connection
				go func() {
					bufferSize := 2048
					buf := make([]byte, bufferSize)
					for {
						r, readErr := bufrw.Read(buf)

						if ctx.Err() != nil {
							return
						}

						if r > 0 {
							websocketData := append(reqIdBuff, buf[:r]...)
							websocketMsg := protocol.TunnelMessage{
								MsgType: protocol.WEBSOCKET_MESSAGE,
								MsgData: websocketData,
							}

							if err := clientTunnel.SendMessage(websocketMsg); err != nil {
								logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to send WEBSOCKET_MESSAGE msg to client: %v", err))
								cancel(context.Cause(ctx))
								return
							}
						}

						if readErr != nil {
							if errors.Is(readErr, io.EOF) {
								return
							}
							log.Fatal(readErr)
						}
					}
				}()

				for {
					select {
					case <-ctx.Done(): // Request is canceled or Tunnel is closed if context is canceled
						handleCancel(context.Cause(ctx), w)
						clientTunnel.inflightRequests.Delete(reqId)
						return
					case outWs, _ := <-outgoingWsChannel: // Handle websocket data from tunnel (mmar client)
						bufrw.Write(outWs.bytes)
						if flushErr := bufrw.Flush(); flushErr != nil {
							log.Fatal("Failed to flush bufrw", flushErr)
						}
					}
				}

			} else {
				// Write response headers with response status code to original client
				w.WriteHeader(resp.statusCode)

				// Write the response body to original client
				w.Write(resp.body)

				// Remove from inflight requests after sending response back
				clientTunnel.inflightRequests.Delete(reqId)
			}

		}
	}
}

func (ms *MmarServer) GenerateUniqueSubdomain() string {
	reservedSubdomains := []string{"", "admin", "stats"}

	generatedSubdomain := ""
	for _, exists := ms.clients[generatedSubdomain]; exists || slices.Contains(reservedSubdomains, generatedSubdomain); {
		generatedSubdomain = GenerateRandomID()
	}

	return generatedSubdomain
}

func (ms *MmarServer) TunnelLimitedIP(ip string) bool {
	tunnels, tunnelsExist := ms.tunnelsPerIP[ip]

	// Initialize tunnels list for IP
	if !tunnelsExist {
		ms.tunnelsPerIP[ip] = []string{}
	}

	return len(tunnels) >= constants.MAX_TUNNELS_PER_IP
}

func (ms *MmarServer) newClientTunnel(tunnel protocol.Tunnel, subdomain string) (*ClientTunnel, error) {
	// Acquire lock to create new client tunnel data
	ms.mu.Lock()

	var uniqueSubdomain string
	var msgType uint8
	if subdomain != "" {
		uniqueSubdomain = subdomain
		msgType = protocol.TUNNEL_RECLAIMED
	} else {
		// Generate unique subdomain for client if not passed in
		uniqueSubdomain = ms.GenerateUniqueSubdomain()
		msgType = protocol.TUNNEL_CREATED
	}

	tunnel.Id = uniqueSubdomain

	// Initialize inflight requests map for client tunnel
	var inflightRequests sync.Map

	// Create client tunnel
	clientTunnel := ClientTunnel{
		tunnel,
		&inflightRequests,
	}

	// Check if IP reached max tunnel limit
	clientIP := utils.ExtractIP(tunnel.Conn.RemoteAddr().String())
	limitedIP := ms.TunnelLimitedIP(clientIP)
	// If so, send limit message to client and close client tunnel
	if limitedIP {
		limitMessage := protocol.TunnelMessage{MsgType: protocol.CLIENT_TUNNEL_LIMIT}
		if err := clientTunnel.SendMessage(limitMessage); err != nil {
			logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to send Tunnel Limit msg to client: %v", err))
		}
		clientTunnel.close(false)
		// Release lock once errored
		ms.mu.Unlock()
		return nil, CLIENT_MAX_TUNNELS_REACHED
	}

	// Add client tunnel to clients
	ms.clients[uniqueSubdomain] = clientTunnel

	// Associate tunnel with client IP
	ms.tunnelsPerIP[clientIP] = append(ms.tunnelsPerIP[clientIP], uniqueSubdomain)

	// Release lock once created
	ms.mu.Unlock()

	// Send unique subdomain to client
	connMessage := protocol.TunnelMessage{MsgType: msgType, MsgData: []byte(uniqueSubdomain)}
	if err := clientTunnel.SendMessage(connMessage); err != nil {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to send unique subdomain msg to client: %v", err))
		return nil, err
	}

	return &clientTunnel, nil
}

func (ms *MmarServer) handleTcpConnection(conn net.Conn) {

	tunnel := protocol.Tunnel{
		Conn:      conn,
		CreatedOn: time.Now(),
		Reader:    bufio.NewReader(conn),
	}

	// Process Tunnel Messages coming from mmar client
	go ms.processTunnelMessages(tunnel)
}

func (ms *MmarServer) closeTunnel(t *protocol.Tunnel) {
	t.Conn.Close()
}

func (ms *MmarServer) closeClientTunnel(ct *ClientTunnel) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

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

func (ms *MmarServer) closeClientTunnelOrConn(ct *ClientTunnel, t protocol.Tunnel) {

	ms.closeTunnel(&t)

	// If client has reserved subdomain, remove it from tunnels
	if ct != nil && ct.ReservedSubdomain() {
		ms.closeClientTunnel(ct)
	}
}

func (ms *MmarServer) handleResponseMessages(ct *ClientTunnel, tunnelMsg protocol.TunnelMessage) {
	respReader := bufio.NewReader(bytes.NewReader(tunnelMsg.MsgData))

	// Extract RequestId
	reqIdBuff := make([]byte, constants.REQUEST_ID_BUFF_SIZE)
	_, err := io.ReadFull(respReader, reqIdBuff)
	if err != nil {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("[%s] - Failed to parse RequestId for response: %v\n", ct.Tunnel.Id, err))
		return
	}

	// Get Inflight Request
	reqId := RequestId(binary.LittleEndian.Uint32(reqIdBuff))
	inflight, loaded := ct.inflightRequests.Load(reqId)
	if !loaded {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("[%s] Failed to identify inflight request: %v", ct.Tunnel.Id, reqId))
		return
	}

	inflightRequest, ok := inflight.(IncomingRequest)
	if !ok {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("[%s] Failed to parse inflight request: %v", ct.Tunnel.Id, reqId))
		return
	}

	// Read response for forwarded request
	resp, respErr := http.ReadResponse(respReader, inflightRequest.request)

	if respErr != nil {
		if errors.Is(respErr, io.ErrUnexpectedEOF) || errors.Is(respErr, net.ErrClosed) {
			inflightRequest.cancel(CLIENT_DISCONNECTED_ERR)
			ms.closeClientTunnel(ct)
			return
		}
		failedReq := fmt.Sprintf("%s - %s%s", inflightRequest.request.Method, html.EscapeString(inflightRequest.request.URL.Path), inflightRequest.request.URL.RawQuery)
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to return response: %v\n\n for req: %v", respErr, failedReq))
		inflightRequest.cancel(FAILED_TO_READ_RESP_FROM_MMAR_CLIENT_ERR)
		return
	}

	respBody, respBodyErr := io.ReadAll(resp.Body)
	if respBodyErr != nil {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to parse response body: %v %v\n", respBodyErr, len(respBody)))
		inflightRequest.cancel(READ_RESP_BODY_ERR)
		return
	}

	// Set headers for response
	for hKey, hVal := range resp.Header {
		inflightRequest.responseWriter.Header().Set(hKey, hVal[0])
		// Add remaining values for header if more than one exists
		for i := 1; i < len(hVal); i++ {
			inflightRequest.responseWriter.Header().Add(hKey, hVal[i])
		}
	}

	select {
	case <-inflightRequest.ctx.Done():
		// Request is canceled, do nothing
		return
	case inflightRequest.responseChannel <- OutgoingResponse{statusCode: resp.StatusCode, body: respBody}:
		// Send response data back
	}
}

func (ms *MmarServer) handleWebsocketMessages(ct *ClientTunnel, tunnelMsg protocol.TunnelMessage) {
	respReader := bufio.NewReader(bytes.NewReader(tunnelMsg.MsgData))

	// Extract RequestId
	reqIdBuff := make([]byte, constants.REQUEST_ID_BUFF_SIZE)
	_, err := io.ReadFull(respReader, reqIdBuff)
	if err != nil {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("[%s] - Failed to parse RequestId for websocket message: %v\n", ct.Tunnel.Id, err))
		return
	}

	// Get Inflight Request
	reqId := RequestId(binary.LittleEndian.Uint32(reqIdBuff))
	inflight, loaded := ct.inflightRequests.Load(reqId)
	if !loaded {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("[%s] Failed to identify inflight websocket request: %v. External client likely closed connection.", ct.Tunnel.Id, reqId))
		return
	}

	inflightRequest, ok := inflight.(IncomingRequest)
	if !ok {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("[%s] Failed to parse inflight request: %v", ct.Tunnel.Id, reqId))
		return
	}

	// Extract the websocket data
	wsBytes := tunnelMsg.MsgData[constants.REQUEST_ID_BUFF_SIZE:]

	select {
	case <-inflightRequest.ctx.Done():
		// Request is canceled, do nothing
		return
	case inflightRequest.outgoingWsChannel <- WebsocketData{bytes: wsBytes}:
		// Send websocket data back
	}

}

func (ms *MmarServer) handleWebsocketClose(ct *ClientTunnel, tunnelMsg protocol.TunnelMessage) {
	respReader := bufio.NewReader(bytes.NewReader(tunnelMsg.MsgData))

	// Extract RequestId
	reqIdBuff := make([]byte, constants.REQUEST_ID_BUFF_SIZE)
	_, err := io.ReadFull(respReader, reqIdBuff)
	if err != nil {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("[%s] - Failed to parse RequestId for websocket close: %v\n", ct.Tunnel.Id, err))
		return
	}

	// Get Inflight Request
	reqId := RequestId(binary.LittleEndian.Uint32(reqIdBuff))
	inflight, loaded := ct.inflightRequests.Load(reqId)
	if !loaded {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("[%s] Failed to identify inflight websocket request: %v. External client likely closed connection.", ct.Tunnel.Id, reqId))
		return
	}

	inflightRequest, ok := inflight.(IncomingRequest)
	if !ok {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("[%s] Failed to parse inflight request: %v", ct.Tunnel.Id, reqId))
		return
	}

	wsErr := errors.New("mmar client closed websocket connection")
	inflightRequest.cancel(wsErr)
}

func (ms *MmarServer) processTunnelMessages(t protocol.Tunnel) {
	var ct *ClientTunnel
	for {
		// Send heartbeat if nothing has been read for a while
		receiveMessageTimeout := time.AfterFunc(
			constants.HEARTBEAT_FROM_SERVER_TIMEOUT*time.Second,
			func() {
				heartbeatMsg := protocol.TunnelMessage{MsgType: protocol.HEARTBEAT_FROM_SERVER}
				if err := t.SendMessage(heartbeatMsg); err != nil {
					logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to send heartbeat: %v", err))
					ms.closeClientTunnelOrConn(ct, t)
					return
				}
				// Set a read timeout, if no response to heartbeat is received within that period,
				// that means the client has disconnected
				readDeadline := time.Now().Add((constants.READ_DEADLINE * time.Second))
				t.Conn.SetReadDeadline(readDeadline)
			},
		)

		tunnelMsg, err := t.ReceiveMessage()
		// If a message is received, stop the receiveMessageTimeout and remove the ReadTimeout
		// as we do not need to send heartbeat or check connection health in this iteration
		receiveMessageTimeout.Stop()
		t.Conn.SetReadDeadline(time.Time{})

		if err != nil {
			logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Receive Message from client tunnel errored: %v", err))
			if utils.NetworkError(err) || errors.Is(err, protocol.INVALID_MESSAGE_PROTOCOL_VERSION) {
				// If error with connection, stop processing messages
				ms.closeClientTunnelOrConn(ct, t)
				return
			}
			continue
		}

		switch tunnelMsg.MsgType {
		case protocol.CREATE_TUNNEL:
			// mmar client requesting new tunnel
			ct, err = ms.newClientTunnel(t, "")

			if err != nil {
				if errors.Is(err, CLIENT_MAX_TUNNELS_REACHED) {
					// Close the connection when client max tunnels limit reached
					t.Conn.Close()
					return
				}
				logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to create ClientTunnel: %v", err))
				return
			}

			logger.Log(
				constants.DEFAULT_COLOR,
				fmt.Sprintf(
					"[%s] Tunnel created: %s",
					ct.Tunnel.Id,
					t.Conn.RemoteAddr().String(),
				),
			)
		case protocol.RECLAIM_TUNNEL:
			// mmar client reclaiming a previously created tunnel
			existingId := string(tunnelMsg.MsgData)

			// Check if the subdomain has already been taken
			_, ok := ms.clients[existingId]
			if ok {
				// if so, close the tunnel, so the user can create a new one
				ms.closeClientTunnelOrConn(ct, t)
				return
			}

			ct, err = ms.newClientTunnel(t, existingId)
			if err != nil {
				if errors.Is(err, CLIENT_MAX_TUNNELS_REACHED) {
					// Close the connection when client max tunnels limit reached
					t.Conn.Close()
					return
				}
				logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to reclaim ClientTunnel: %v", err))
				return
			}

			logger.Log(
				constants.DEFAULT_COLOR,
				fmt.Sprintf(
					"[%s] Tunnel reclaimed: %s",
					existingId,
					ct.Conn.RemoteAddr().String(),
				),
			)
		case protocol.RESPONSE:
			go ms.handleResponseMessages(ct, tunnelMsg)
		case protocol.LOCALHOST_NOT_RUNNING:
			// Create a response for Tunnel connected but localhost not running
			errState := protocol.TunnelErrState(protocol.LOCALHOST_NOT_RUNNING)
			respStatus := http.StatusOK
			responseBuff := createSerializedServerResp(http.StatusText(respStatus), respStatus, errState)
			notRunningMsg := protocol.TunnelMessage{
				MsgType: protocol.RESPONSE,
				MsgData: append(tunnelMsg.MsgData, responseBuff.Bytes()...),
			}
			go ms.handleResponseMessages(ct, notRunningMsg)
		case protocol.DEST_REQUEST_TIMEDOUT:
			// Create a response for Tunnel connected but localhost took too long to respond
			errState := protocol.TunnelErrState(protocol.DEST_REQUEST_TIMEDOUT)
			respStatus := http.StatusOK
			responseBuff := createSerializedServerResp(http.StatusText(respStatus), respStatus, errState)
			destTimedoutMsg := protocol.TunnelMessage{
				MsgType: protocol.RESPONSE,
				MsgData: append(tunnelMsg.MsgData, responseBuff.Bytes()...),
			}
			go ms.handleResponseMessages(ct, destTimedoutMsg)
		case protocol.CLIENT_DISCONNECT:
			ms.closeClientTunnelOrConn(ct, t)
			return
		case protocol.HEARTBEAT_FROM_CLIENT:
			heartbeatAckMsg := protocol.TunnelMessage{MsgType: protocol.HEARTBEAT_ACK}
			if err := t.SendMessage(heartbeatAckMsg); err != nil {
				logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to heartbeat ack to client: %v", err))
				ms.closeClientTunnelOrConn(ct, t)
				return
			}
		case protocol.HEARTBEAT_ACK:
			// Got a heartbeat ack, that means the connection is healthy,
			// we do not need to perform any action
		case protocol.INVALID_RESP_FROM_DEST:
			// Create a response for receiving invalid response from destination server
			errState := protocol.TunnelErrState(protocol.INVALID_RESP_FROM_DEST)
			respStatus := http.StatusInternalServerError
			responseBuff := createSerializedServerResp(http.StatusText(respStatus), respStatus, errState)
			invalidRespFromDestMsg := protocol.TunnelMessage{
				MsgType: protocol.RESPONSE,
				MsgData: append(tunnelMsg.MsgData, responseBuff.Bytes()...),
			}
			go ms.handleResponseMessages(ct, invalidRespFromDestMsg)
		case protocol.WEBSOCKET_MESSAGE:
			// Dont fire a goroutine for this one to make sure we
			// handle websocket messages in order, since its possible for
			// a large websocket message to be split over multiple frames
			ms.handleWebsocketMessages(ct, tunnelMsg)
		case protocol.WEBSOCKET_CLOSE:
			go ms.handleWebsocketClose(ct, tunnelMsg)
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
				logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to accept TCP connection: %v", err))
			} else {
				go mmarServer.handleTcpConnection(conn)
			}
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
