package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"html"
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
	"github.com/yusuf-musleh/mmar/internal/protocol"
)

// Tunnel to Server
type ServerTunnel struct {
	protocol.Tunnel
}

func localizeRequest(request *http.Request) {
	localhost := fmt.Sprintf("http://localhost:%v%v", constants.CLIENT_PORT, request.RequestURI)
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
func (st *ServerTunnel) handleRequestMessage(tunnelMsg protocol.TunnelMessage) {
	fwdClient := &http.Client{}

	reqReader := bufio.NewReader(bytes.NewReader(tunnelMsg.MsgData))
	req, reqErr := http.ReadRequest(reqReader)

	if reqErr != nil {
		if errors.Is(reqErr, io.EOF) {
			log.Print("Connection to mmar server closed or disconnected. Exiting...")
			os.Exit(0)
		}

		if errors.Is(reqErr, net.ErrClosed) {
			log.Printf("Connection closed.")
			os.Exit(0)
		}
		log.Fatalf("Failed to read data from TCP conn: %v", reqErr)
	}

	// Convert request to target localhost
	localizeRequest(req)

	log.Printf("%s - %s%s", req.Method, html.EscapeString(req.URL.Path), req.URL.RawQuery)
	resp, fwdErr := fwdClient.Do(req)
	if fwdErr != nil {
		if errors.Is(fwdErr, syscall.ECONNREFUSED) {
			localhostNotRunningMsg := protocol.TunnelMessage{MsgType: protocol.LOCALHOST_NOT_RUNNING}
			if err := st.SendMessage(localhostNotRunningMsg); err != nil {
				log.Fatal(err)
			}
			return
		}

		log.Fatalf("Failed to forward: %v", fwdErr)
	}

	// Writing response to buffer to tunnel it back
	var responseBuff bytes.Buffer
	resp.Write(&responseBuff)

	respMessage := protocol.TunnelMessage{MsgType: protocol.RESPONSE, MsgData: responseBuff.Bytes()}
	if err := st.SendMessage(respMessage); err != nil {
		log.Fatal(err)
	}
}

func (st *ServerTunnel) ProcessTunnelMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // Client gracefully shutdown
			return
		default:
			tunnelMsg, err := st.ReceiveMessage()
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Print("Tunnel connection closed from Server. Exiting...")
					os.Exit(0)
				} else if errors.Is(err, net.ErrClosed) {
					log.Print("Tunnel connection disconnected from Server. Existing...")
					os.Exit(0)
				}
				log.Fatalf("Failed to receive message from server tunnel: %v", err)
			}

			switch tunnelMsg.MsgType {
			case protocol.REQUEST:
				log.Printf("Got REQUEST TUNNEL MESSAGE\n")
				go st.handleRequestMessage(tunnelMsg)
			}
		}
	}
}

func Run(serverTcpPort string, tunnelHost string) {
	// Channel handler for interrupt signal
	sigInt := make(chan os.Signal, 1)
	signal.Notify(sigInt, os.Interrupt)

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", tunnelHost, serverTcpPort))
	if err != nil {
		log.Printf("Could not reach mmar server on: %s:%s \nExiting...", tunnelHost, serverTcpPort)
		os.Exit(0)
	}
	defer conn.Close()
	serverTunnel := ServerTunnel{protocol.Tunnel{Conn: conn}}

	// Create context to cancel running gouroutines when shutting down
	ctx, cancel := context.WithCancel(context.Background())

	// Process Tunnel Messages coming from mmar server
	go serverTunnel.ProcessTunnelMessages(ctx)

	// Wait for an interrupt signal, if received, terminate gracefully
	<-sigInt

	log.Printf("Gracefully shutting down client...")
	disconnectMsg := protocol.TunnelMessage{MsgType: protocol.CLIENT_DISCONNECT}
	serverTunnel.SendMessage(disconnectMsg)
	cancel()
	gracefulShutdownTimer := time.NewTimer(constants.GRACEFUL_SHUTDOWN_TIMEOUT)
	<-gracefulShutdownTimer.C
}
