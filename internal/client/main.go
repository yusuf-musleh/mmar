package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yusuf-musleh/mmar/constants"
	"github.com/yusuf-musleh/mmar/internal/logger"
	"github.com/yusuf-musleh/mmar/internal/protocol"
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
	subdomain string
}

func (mc *MmarClient) localizeRequest(request *http.Request) {
	localhost := fmt.Sprintf("http://localhost:%v%v", mc.LocalPort, request.RequestURI)
	localURL, urlErr := url.Parse(localhost)
	if urlErr != nil {
		log.Fatalf("Failed to parse URL: %v", urlErr)
	}

	// Set URL to send request to local server
	request.URL = localURL
	// Clear requestURI since it is now a client request
	request.RequestURI = ""
}

// Process requests coming from mmar server and forward them to localhost
func (mc *MmarClient) handleRequestMessage(tunnelMsg protocol.TunnelMessage) {
	fwdClient := &http.Client{
		Timeout: constants.DEST_REQUEST_TIMEOUT * time.Second,
		// Do not follow redirects, let the end-user's client handle it
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	reqReader := bufio.NewReader(bytes.NewReader(tunnelMsg.MsgData))
	req, reqErr := http.ReadRequest(reqReader)

	if reqErr != nil {
		if errors.Is(reqErr, io.EOF) {
			logger.Log(constants.DEFAULT_COLOR, "Connection to mmar server closed or disconnected. Exiting...")
			os.Exit(0)
		}

		if errors.Is(reqErr, net.ErrClosed) {
			logger.Log(constants.DEFAULT_COLOR, "Connection closed.")
			os.Exit(0)
		}
		log.Fatalf("Failed to read data from TCP conn: %v", reqErr)
	}

	// Convert request to target localhost
	mc.localizeRequest(req)

	resp, fwdErr := fwdClient.Do(req)
	if fwdErr != nil {
		if errors.Is(fwdErr, syscall.ECONNREFUSED) || errors.Is(fwdErr, io.ErrUnexpectedEOF) || errors.Is(fwdErr, io.EOF) {
			localhostNotRunningMsg := protocol.TunnelMessage{MsgType: protocol.LOCALHOST_NOT_RUNNING}
			if err := mc.SendMessage(localhostNotRunningMsg); err != nil {
				log.Fatal(err)
			}
			return
		} else if errors.Is(fwdErr, context.DeadlineExceeded) {
			destServerTimedoutMsg := protocol.TunnelMessage{MsgType: protocol.DEST_REQUEST_TIMEDOUT}
			if err := mc.SendMessage(destServerTimedoutMsg); err != nil {
				log.Fatal(err)
			}
			return
		}

		log.Fatalf("Failed to forward: %v", fwdErr)
	}

	logger.LogHTTP(req, resp.StatusCode, resp.ContentLength, false, true)

	// Writing response to buffer to tunnel it back
	var responseBuff bytes.Buffer
	resp.Write(&responseBuff)

	respMessage := protocol.TunnelMessage{MsgType: protocol.RESPONSE, MsgData: responseBuff.Bytes()}
	if err := mc.SendMessage(respMessage); err != nil {
		log.Fatal(err)
	}
}

// Keep attempting to reconnect the existing tunnel until successful
func (mc *MmarClient) reconnectTunnel(ctx context.Context) {
	for {
		// If context is cancelled, do not reconnect
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}
		logger.Log(constants.DEFAULT_COLOR, "Attempting to reconnect...")
		conn, err := net.DialTimeout(
			"tcp",
			fmt.Sprintf("%s:%s", mc.ConfigOptions.TunnelHost, mc.ConfigOptions.TunnelTcpPort),
			constants.TUNNEL_CREATE_TIMEOUT*time.Second,
		)
		if err != nil {
			time.Sleep(constants.TUNNEL_RECONNECT_TIMEOUT * time.Second)
			continue
		}
		mc.Tunnel.Conn = conn
		break
	}
}

func (mc *MmarClient) ProcessTunnelMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // Client gracefully shutdown
			return
		default:
			tunnelMsg, err := mc.ReceiveMessage()
			if err != nil {
				// If the context was cancelled just return
				if errors.Is(ctx.Err(), context.Canceled) {
					return
				} else if errors.Is(err, os.ErrDeadlineExceeded) {
					continue
				}

				logger.Log(constants.DEFAULT_COLOR, "Tunnel connection disconnected.")

				// Keep trying to reconnect
				mc.reconnectTunnel(ctx)

				continue
			}

			switch tunnelMsg.MsgType {
			case protocol.CLIENT_CONNECT:
				tunnelSubdomain := string(tunnelMsg.MsgData)
				// If there is an existing subdomain, that means we are reconnecting with an
				// existing mmar client, try to reclaim the same subdomain
				if mc.subdomain != "" {
					reconnectMsg := protocol.TunnelMessage{MsgType: protocol.CLIENT_RECLAIM_SUBDOMAIN, MsgData: []byte(tunnelSubdomain + ":" + mc.subdomain)}
					mc.subdomain = ""
					if err := mc.SendMessage(reconnectMsg); err != nil {
						logger.Log(constants.DEFAULT_COLOR, "Tunnel failed to reconnect. Exiting...")
						os.Exit(0)
					}
					continue
				} else {
					mc.subdomain = tunnelSubdomain
				}
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
				go mc.handleRequestMessage(tunnelMsg)
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
		fmt.Sprintf("%s:%s", config.TunnelHost, config.TunnelTcpPort),
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
	mmarClient := MmarClient{
		protocol.Tunnel{Conn: conn},
		config,
		"",
	}

	// Create context to cancel running gouroutines when shutting down
	ctx, cancel := context.WithCancel(context.Background())

	// Process Tunnel Messages coming from mmar server
	go mmarClient.ProcessTunnelMessages(ctx)

	// Wait for an interrupt signal, if received, terminate gracefully
	<-sigInt

	logger.Log(constants.YELLOW, "Gracefully shutting down client...")
	disconnectMsg := protocol.TunnelMessage{MsgType: protocol.CLIENT_DISCONNECT}
	mmarClient.SendMessage(disconnectMsg)
	cancel()
	gracefulShutdownTimer := time.NewTimer(constants.GRACEFUL_SHUTDOWN_TIMEOUT * time.Second)
	<-gracefulShutdownTimer.C
}
