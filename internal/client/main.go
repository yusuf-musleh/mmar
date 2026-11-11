package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/yusuf-musleh/mmar/constants"
	"github.com/yusuf-musleh/mmar/internal/logger"
	"github.com/yusuf-musleh/mmar/internal/protocol"
	"github.com/yusuf-musleh/mmar/internal/utils"
)

type ConfigOptions struct {
	LocalPort      string
	TunnelHttpPort string
	TunnelTcpPort  string
	TunnelHost     string
}

type MmarClient struct {
	// Tunnel to Server
	protocol.Tunnel
	ConfigOptions
	subdomain        string
	inflightRequests *sync.Map // used for websockets
}

// ID of request forwarded from mmar server
type RequestId uint32

type WebsocketData struct {
	bytes []byte
}

type IncomingRequest struct {
	incomingWsChannel chan WebsocketData
}

type ExchangeContext struct {
	originalReq    *http.Request
	connReadWriter *bufio.ReadWriter
	msgData        []byte
}

func (mc *MmarClient) handleWebsocketUpgradeResp(ctx context.Context, reqIdBuff []byte, respEC ExchangeContext, fwdConn net.Conn, respHeaders http.Header) {
	wsCtx, wsCtxCancel := context.WithCancel(ctx)
	defer wsCtxCancel()

	// Remove read/write deadlines since we need the connection open for websockets
	fwdConn.SetReadDeadline(time.Time{})
	fwdConn.SetWriteDeadline(time.Time{})

	// Construct Switching Protocol response
	var responseBuff bytes.Buffer
	resp := &http.Response{
		Status:     http.StatusText(http.StatusSwitchingProtocols),
		StatusCode: http.StatusSwitchingProtocols,
		Body:       io.NopCloser(bytes.NewBufferString("")),
	}

	resp.Header = respHeaders

	resp.Write(&responseBuff)
	respEC.msgData = append(respEC.msgData, responseBuff.Bytes()...)

	// Send back Switching Protocol response
	respMessage := protocol.TunnelMessage{MsgType: protocol.RESPONSE, MsgData: respEC.msgData}
	if err := mc.SendMessage(respMessage); err != nil {
		log.Fatal(err)
	}

	logger.LogHTTP(respEC.originalReq, resp.StatusCode, resp.ContentLength, false, true)

	// Fire a goroutine to read websocket data from mmar server
	// it uses the request ID to determine which channel to send to,
	// allowing multiple websocket connections to be opened
	incomingWsChannel := make(chan WebsocketData, constants.WEBSOCKET_CHANNEL_BUFFER_SZE)
	// Get Inflight Request
	reqId := RequestId(binary.LittleEndian.Uint32(reqIdBuff))
	// Construct IncomingRequest
	incomingReq := IncomingRequest{
		incomingWsChannel: incomingWsChannel,
	}
	mc.inflightRequests.Store(reqId, incomingReq)
	go func() {
		for {
			select {
			case <-wsCtx.Done(): // Gracefully terminate goroutine if ctx is cancelled
				// Send close frame to local dev server
				respEC.connReadWriter.Write(constants.WBSOCKET_CLOSE_BYTES)
				respEC.connReadWriter.Flush()

				closeWsMsg := protocol.TunnelMessage{
					MsgType: protocol.WEBSOCKET_CLOSE,
					MsgData: reqIdBuff,
				}
				if err := mc.SendMessage(closeWsMsg); err != nil {
					log.Println("Failed to send WS Close Message to mmar server.", err)
				}

				mc.inflightRequests.Delete(reqId)
				return
			case incomingWsData, ok := <-incomingWsChannel:
				if !ok {
					return
				}
				// Forward data received from mmar sever to local dev server
				_, fwdWsDataErr := respEC.connReadWriter.Write(incomingWsData.bytes)
				wsDataFlushErr := respEC.connReadWriter.Flush()
				if fwdWsDataErr != nil || wsDataFlushErr != nil {
					log.Fatal("Failed to forward websocket data to local server", fwdWsDataErr, wsDataFlushErr)
				}
			}
		}
	}()

	// Reading websocket data from local dev server connection
	bufferSize := 2048
	buf := make([]byte, bufferSize)
	for {
		r, readErr := respEC.connReadWriter.Read(buf)

		if r > 0 {

			// Ignore sending back WS server-to-client close bytes
			// that have no code and no reason [0x88 0x00]
			serverToClientClose := []byte{136, 0}
			if !bytes.Equal(buf[:r], serverToClientClose) {
				// Send back data from local dev server to mmar server
				websocketData := append(reqIdBuff, buf[:r]...)
				websocketMsg := protocol.TunnelMessage{
					MsgType: protocol.WEBSOCKET_MESSAGE,
					MsgData: websocketData,
				}

				if err := mc.SendMessage(websocketMsg); err != nil {
					logger.Log(constants.DEFAULT_COLOR, "Failed to forward WS message to mmar server, cancelling WS connection to local server...")
					wsCtxCancel()
				}
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return
			}
			log.Fatal(readErr)
		}
	}
}

func (mc *MmarClient) handleHttpResp(respEC ExchangeContext, headerLines [][]byte, httpStatusCode []byte, respHeaders http.Header) {
	var bodyBuffer []byte
	var cl int
	var respErr error

	// Check for transfer-encoding first
	if respHeaders.Get("transfer-encoding") != "" {
		bodyBuffer, cl, respErr = processChunkedResponse(respEC.connReadWriter)
		if respErr != nil {
			invalidRespFromDestMsg := protocol.TunnelMessage{MsgType: protocol.INVALID_RESP_FROM_DEST, MsgData: respEC.msgData}
			if err := mc.SendMessage(invalidRespFromDestMsg); err != nil {
				log.Fatal(err)
			}
			return
		}

		respHeaders.Del("transfer-encoding")
		respHeaders.Set("content-length", strconv.Itoa(cl))
	} else {
		respContentLength := respHeaders.Get("content-length")
		if respContentLength != "" {
			bodyBuffer, cl, respErr = processContentLengthResp(respContentLength, respEC.connReadWriter)
			if respErr != nil {
				invalidRespFromDestMsg := protocol.TunnelMessage{MsgType: protocol.INVALID_RESP_FROM_DEST, MsgData: respEC.msgData}
				if err := mc.SendMessage(invalidRespFromDestMsg); err != nil {
					log.Fatal(err)
				}
				return
			}
		}
	}

	// Append HTTP status line to msg
	respEC.msgData = append(respEC.msgData, headerLines[0]...)
	respEC.msgData = append(respEC.msgData, []byte("\r\n")...)

	// Append response headers to msg
	headersBuffer := bytes.NewBuffer(nil)
	respHeaders.Write(headersBuffer)
	respEC.msgData = append(respEC.msgData, headersBuffer.Bytes()...)
	respEC.msgData = append(respEC.msgData, []byte("\r\n")...)

	// Append response body to msg
	respEC.msgData = append(respEC.msgData, bodyBuffer...)
	// Send back response read from local dev server to mmar server
	respMessage := protocol.TunnelMessage{MsgType: protocol.RESPONSE, MsgData: respEC.msgData}
	if err := mc.SendMessage(respMessage); err != nil {
		log.Fatal(err)
	}

	statusCode, statusCodeErr := strconv.Atoi(string(httpStatusCode))
	if statusCodeErr != nil {
		// Fallback to 0 if status code cannot be converted
		statusCode = 0
	}
	logger.LogHTTP(respEC.originalReq, statusCode, int64(cl), false, true)
}

// Process requests coming from mmar server and forward them to localhost
func (mc *MmarClient) handleRequestMessage(ctx context.Context, tunnelMsg protocol.TunnelMessage) {
	// Extract RequestId
	reqIdBuff := tunnelMsg.MsgData[:constants.REQUEST_ID_BUFF_SIZE]
	// Include RequestId in tunnel back message
	msgData := []byte{}
	msgData = append(msgData, reqIdBuff...)

	// Prepare request bytes for forwarding to local dev server
	reqBytes := tunnelMsg.MsgData[constants.REQUEST_ID_BUFF_SIZE:]

	// Establish TCP connection to local dev server
	localUrl := net.JoinHostPort("localhost", mc.LocalPort)
	dialer := &net.Dialer{
		Timeout: constants.DEST_DIAL_TIMEOUT * time.Second,
	}

	fwdConn, fwdConnErr := dialer.DialContext(ctx, "tcp", localUrl)

	if fwdConnErr != nil {
		localhostNotRunningMsg := protocol.TunnelMessage{MsgType: protocol.LOCALHOST_NOT_RUNNING, MsgData: msgData}

		if err := mc.SendMessage(localhostNotRunningMsg); err != nil {
			log.Fatal(err)
		}
		return
	}
	defer fwdConn.Close()

	// Set Read Deadline
	readDeadline := time.Now().Add((constants.DEST_REQUEST_TIMEOUT * time.Second))
	fwdConn.SetReadDeadline(readDeadline)

	// Set Write Deadline
	writeDeadline := time.Now().Add((constants.DEST_REQUEST_TIMEOUT * time.Second))
	fwdConn.SetWriteDeadline(writeDeadline)

	// Initialize bufio Reader/Writer from conn
	connReader := bufio.NewReader(fwdConn)
	connWriter := bufio.NewWriter(fwdConn)
	connReadWriter := bufio.NewReadWriter(connReader, connWriter)

	// Forward request bytes to local dev server
	_, fwdErr := connReadWriter.Write(reqBytes)
	wFlushErr := connReadWriter.Flush()

	if fwdErr != nil || wFlushErr != nil {
		if errors.Is(fwdErr, syscall.ECONNREFUSED) || errors.Is(fwdErr, io.ErrUnexpectedEOF) || errors.Is(fwdErr, io.EOF) {
			localhostNotRunningMsg := protocol.TunnelMessage{MsgType: protocol.LOCALHOST_NOT_RUNNING, MsgData: msgData}
			if err := mc.SendMessage(localhostNotRunningMsg); err != nil {
				log.Fatal(err)
			}
			return
		} else if errors.Is(fwdErr, os.ErrDeadlineExceeded) {
			destServerTimedoutMsg := protocol.TunnelMessage{MsgType: protocol.DEST_REQUEST_TIMEDOUT, MsgData: msgData}
			if err := mc.SendMessage(destServerTimedoutMsg); err != nil {
				log.Fatal(err)
			}
			return
		}

		localhostNotRunningMsg := protocol.TunnelMessage{MsgType: protocol.LOCALHOST_NOT_RUNNING, MsgData: msgData}
		if err := mc.SendMessage(localhostNotRunningMsg); err != nil {
			log.Fatal(err)
		}
		return
	}

	// Create request instance from request bytes to associate with response
	reqReader := bufio.NewReader(bytes.NewReader(reqBytes))
	req, reqErr := http.ReadRequest(reqReader)
	if reqErr != nil {
		invalidReqFromSrc := protocol.TunnelMessage{MsgType: protocol.INVALID_REQ_FROM_SRC, MsgData: msgData}
		if err := mc.SendMessage(invalidReqFromSrc); err != nil {
			log.Fatal(err)
		}
		return
	}

	// Create response buffer and begin reading response bytes
	respBytes := []byte{}
	// Read the response headers, line-by-line
	for {
		line, _, lineErr := connReadWriter.ReadLine()
		respBytes = append(respBytes, line...)
		respBytes = append(respBytes, []byte("\r\n")...)
		if lineErr != nil {
			if errors.Is(lineErr, io.EOF) {
				// reached the end of the response data (no body)
				break
			} else if errors.Is(lineErr, os.ErrDeadlineExceeded) {
				destServerTimedoutMsg := protocol.TunnelMessage{MsgType: protocol.DEST_REQUEST_TIMEDOUT, MsgData: msgData}
				if err := mc.SendMessage(destServerTimedoutMsg); err != nil {
					log.Fatal(err)
				}
				return
			}

			invalidReqFromSrc := protocol.TunnelMessage{MsgType: protocol.INVALID_REQ_FROM_SRC, MsgData: msgData}
			if err := mc.SendMessage(invalidReqFromSrc); err != nil {
				log.Fatal(err)
			}
			return
		}

		// This means that we reached the end of the headers
		// but there is a body, denoted by the empty line
		if string(line) == "" {
			break
		}
	}

	// At this point respBytes holds the headers (if valid HTTP response)
	// parse them manually to determine what kind of response we are dealing with
	// also validates if we got back valid HTTP response
	upgradeToWebsocket := false
	headerLines := bytes.Split(respBytes, []byte("\r\n"))
	if len(headerLines) <= 0 {
		invalidRespFromDestMsg := protocol.TunnelMessage{MsgType: protocol.INVALID_RESP_FROM_DEST, MsgData: msgData}
		if err := mc.SendMessage(invalidRespFromDestMsg); err != nil {
			log.Fatal(err)
		}
		return
	}

	httpStatusLineErr := validateHTTPResponseStatusline(string(headerLines[0]))
	if httpStatusLineErr != nil {
		invalidRespFromDestMsg := protocol.TunnelMessage{MsgType: protocol.INVALID_RESP_FROM_DEST, MsgData: msgData}
		if err := mc.SendMessage(invalidRespFromDestMsg); err != nil {
			log.Fatal(err)
		}
		return
	}

	// Extract/Parse HTTP status line
	httpStatusLine := bytes.Split(headerLines[0], []byte{' '})
	if len(httpStatusLine) < 3 {
		invalidRespFromDestMsg := protocol.TunnelMessage{MsgType: protocol.INVALID_RESP_FROM_DEST, MsgData: msgData}
		if err := mc.SendMessage(invalidRespFromDestMsg); err != nil {
			log.Fatal(err)
		}
		return
	}

	// Check if response is normal HTTP response or websocket upgrade response
	httpStatusCode := httpStatusLine[1]
	// 101 Switching Protocols status used for upgrading to websocket connection
	if string(httpStatusCode) == "101" {
		upgradeToWebsocket = true
	}

	respHeaders := utils.DeserializeHeaders(respBytes)

	exchangeContext := ExchangeContext{
		originalReq:    req,
		connReadWriter: connReadWriter,
		msgData:        msgData,
	}

	if upgradeToWebsocket {
		mc.handleWebsocketUpgradeResp(ctx, reqIdBuff, exchangeContext, fwdConn, respHeaders)
		return
	}

	mc.handleHttpResp(exchangeContext, headerLines, httpStatusCode, respHeaders)
}

func (mc *MmarClient) handleWebsocketMessage(ctx context.Context, tunnelMsg protocol.TunnelMessage) {
	reqIdBuff := tunnelMsg.MsgData[:constants.REQUEST_ID_BUFF_SIZE]

	// Get Inflight Request
	reqId := RequestId(binary.LittleEndian.Uint32(reqIdBuff))
	inflight, loaded := mc.inflightRequests.Load(reqId)
	if !loaded {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to identify inflight request: %v", reqId))
		return
	}

	inflightRequest, ok := inflight.(IncomingRequest)
	if !ok {
		logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to parse inflight request: %v", reqId))
		return
	}

	select {
	case <-ctx.Done():
		// Request is canceled
		return
	case inflightRequest.incomingWsChannel <- WebsocketData{bytes: tunnelMsg.MsgData[constants.REQUEST_ID_BUFF_SIZE:]}:
		// Send websocket data to corresponding inflight request
	}

}

// Keep attempting to reconnect the existing tunnel until successful
func (mc *MmarClient) reconnectTunnel(ctx context.Context) {

	// Loop through all active WS connections to local WS server and close them
	mc.inflightRequests.Range(func(_ any, incomingReq any) bool {

		inflightRequest, ok := incomingReq.(IncomingRequest)
		if !ok {
			logger.Log(constants.DEFAULT_COLOR, fmt.Sprintf("Failed to parse inflight request: %v", incomingReq))
		}

		wsMsg := WebsocketData{bytes: constants.WBSOCKET_CLOSE_BYTES}

		inflightRequest.incomingWsChannel <- wsMsg

		return true
	})

	for {
		// If context is cancelled, do not reconnect
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}
		logger.Log(constants.DEFAULT_COLOR, "Attempting to reconnect...")
		conn, err := net.DialTimeout(
			"tcp",
			net.JoinHostPort(mc.ConfigOptions.TunnelHost, mc.ConfigOptions.TunnelTcpPort),
			constants.TUNNEL_CREATE_TIMEOUT*time.Second,
		)
		if err != nil {
			if errors.Is(err, protocol.INVALID_MESSAGE_PROTOCOL_VERSION) {
				logger.Log(constants.YELLOW, "The mmar message protocol has been updated, please update mmar.")
				os.Exit(0)
			}
			time.Sleep(constants.TUNNEL_RECONNECT_TIMEOUT * time.Second)
			continue
		}
		mc.Tunnel.Conn = conn
		mc.Tunnel.Reader = bufio.NewReader(conn)

		// Try to reclaim the same subdomain
		reclaimTunnelMsg := protocol.TunnelMessage{MsgType: protocol.RECLAIM_TUNNEL, MsgData: []byte(mc.subdomain)}
		if err := mc.SendMessage(reclaimTunnelMsg); err != nil {
			logger.Log(constants.DEFAULT_COLOR, "Tunnel failed to reconnect. Exiting...")
			os.Exit(0)
		}

		break
	}
}

func (mc *MmarClient) ProcessTunnelMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // Client gracefully shutdown
			return
		default:
			// Send heartbeat if nothing has been read for a while
			receiveMessageTimeout := time.AfterFunc(
				constants.HEARTBEAT_FROM_CLIENT_TIMEOUT*time.Second,
				func() {
					heartbeatMsg := protocol.TunnelMessage{MsgType: protocol.HEARTBEAT_FROM_CLIENT}
					if err := mc.SendMessage(heartbeatMsg); err != nil {
						logger.Log(constants.DEFAULT_COLOR, "Failed to send heartbeat. Exiting...")
						os.Exit(0)
					}
					// Set a read timeout, if no response to heartbeat is recieved within that period,
					// attempt to reconnect to the server
					readDeadline := time.Now().Add((constants.READ_DEADLINE * time.Second))
					mc.Tunnel.Conn.SetReadDeadline(readDeadline)
				},
			)

			tunnelMsg, err := mc.ReceiveMessage()
			// If a message is received, stop the receiveMessageTimeout and remove the ReadTimeout
			// as we do not need to send heartbeat or check connection health in this iteration
			receiveMessageTimeout.Stop()
			mc.Tunnel.Conn.SetReadDeadline(time.Time{})

			if err != nil {
				// If the context was cancelled just return
				if errors.Is(ctx.Err(), context.Canceled) {
					return
				} else if errors.Is(err, protocol.INVALID_MESSAGE_PROTOCOL_VERSION) {
					logger.Log(constants.YELLOW, "The mmar message protocol has been updated, please update mmar.")
					os.Exit(0)
				}

				logger.Log(constants.DEFAULT_COLOR, "Tunnel connection disconnected.")

				// Keep trying to reconnect
				mc.reconnectTunnel(ctx)

				continue
			}

			switch tunnelMsg.MsgType {
			case protocol.TUNNEL_CREATED, protocol.TUNNEL_RECLAIMED:
				tunnelSubdomain := string(tunnelMsg.MsgData)
				mc.subdomain = tunnelSubdomain
				logger.LogTunnelCreated(tunnelSubdomain, mc.TunnelHost, mc.TunnelHttpPort, mc.LocalPort)
			case protocol.CLIENT_TUNNEL_LIMIT:
				limit := logger.ColorLogStr(
					constants.RED,
					fmt.Sprintf("(%v/%v)", constants.MAX_TUNNELS_PER_IP, constants.MAX_TUNNELS_PER_IP),
				)
				logger.Log(
					constants.DEFAULT_COLOR,
					fmt.Sprintf(
						"Maximum limit of Tunnels created reached %v. Please shutdown existing tunnels to create new ones.",
						limit,
					))
				os.Exit(0)
			case protocol.REQUEST:
				go mc.handleRequestMessage(ctx, tunnelMsg)
			case protocol.HEARTBEAT_ACK:
				// Got a heartbeat ack, that means the connection is healthy,
				// we do not need to perform any action
			case protocol.HEARTBEAT_FROM_SERVER:
				heartbeatAckMsg := protocol.TunnelMessage{MsgType: protocol.HEARTBEAT_ACK}
				if err := mc.SendMessage(heartbeatAckMsg); err != nil {
					logger.Log(constants.DEFAULT_COLOR, "Failed to send Heartbeat Ack. Exiting...")
					os.Exit(0)
				}
			case protocol.WEBSOCKET_MESSAGE:
				// Dont fire a goroutine for this one to make sure we
				// handle websocket messages in order, since its possible for
				// a large websocket message to be split over multiple frames
				mc.handleWebsocketMessage(ctx, tunnelMsg)
			}
		}
	}
}

func Run(config ConfigOptions) {
	logger.LogStartMmarClient(config.TunnelHost, config.TunnelTcpPort, config.TunnelHttpPort, config.LocalPort)

	// Channel handler for interrupt signal
	sigInt := make(chan os.Signal, 1)
	signal.Notify(sigInt, os.Interrupt)

	conn, err := net.DialTimeout(
		"tcp",
		net.JoinHostPort(config.TunnelHost, config.TunnelTcpPort),
		constants.TUNNEL_CREATE_TIMEOUT*time.Second,
	)
	if err != nil {
		logger.Log(
			constants.DEFAULT_COLOR,
			fmt.Sprintf(
				"Could not reach mmar server on %s:%s\n %v \nExiting...",
				logger.ColorLogStr(constants.RED, config.TunnelHost),
				logger.ColorLogStr(constants.RED, config.TunnelTcpPort),
				err,
			),
		)
		os.Exit(0)
	}
	defer conn.Close()

	var inflightRequests sync.Map
	mmarClient := MmarClient{
		protocol.Tunnel{Conn: conn, Reader: bufio.NewReader(conn)},
		config,
		"",
		&inflightRequests,
	}

	// Create context to cancel running gouroutines when shutting down
	ctx, cancel := context.WithCancel(context.Background())

	// Process Tunnel Messages coming from mmar server
	go mmarClient.ProcessTunnelMessages(ctx)

	createTunnelMsg := protocol.TunnelMessage{MsgType: protocol.CREATE_TUNNEL}
	if err := mmarClient.SendMessage(createTunnelMsg); err != nil {
		logger.Log(constants.DEFAULT_COLOR, "Failed to create Tunnel. Exiting...")
		os.Exit(0)
	}

	// Wait for an interrupt signal, if received, terminate gracefully
	<-sigInt

	logger.Log(constants.YELLOW, "Gracefully shutting down client...")
	cancel()
	gracefulShutdownTimer := time.NewTimer(constants.GRACEFUL_SHUTDOWN_TIMEOUT * time.Second)
	<-gracefulShutdownTimer.C
	disconnectMsg := protocol.TunnelMessage{MsgType: protocol.CLIENT_DISCONNECT}
	mmarClient.SendMessage(disconnectMsg)
}
