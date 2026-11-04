package utils

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/coder/websocket"
	"github.com/yusuf-musleh/mmar/simulations/dnsserver"
)

const SPAM_MESSAGES_COUNT = 20

func handleEcho(ctx context.Context, msg []byte, wsConn *websocket.Conn) {
	writeErr := wsConn.Write(ctx, websocket.MessageText, msg)
	if writeErr != nil {
		if errors.Is(writeErr, net.ErrClosed) || errors.Is(writeErr, context.Canceled) {
			return
		}
		log.Fatalln("Failed to echo back message", writeErr, string(msg))
	}
}

func handleSpam(ctx context.Context, wsConn *websocket.Conn) {
	spamMsg := []byte("SPAM")
	for range SPAM_MESSAGES_COUNT {
		writeErr := wsConn.Write(ctx, websocket.MessageText, spamMsg)
		if writeErr != nil {
			if errors.Is(writeErr, net.ErrClosed) || errors.Is(writeErr, context.Canceled) {
				return
			}
			log.Fatalln("Failed to send spam message:", writeErr)
		}
	}
}

func handleTriggerClose(ctx context.Context, wsConn *websocket.Conn) {
	closeErr := wsConn.Close(websocket.StatusNormalClosure, "Server closing WS connection")
	if closeErr != nil {
		log.Fatalln("Failed to close WS conn from server")
	}
}

type WebSocketServer struct {
	Server *httptest.Server
}

func (wss *WebSocketServer) Port() string {
	urlSplit := strings.Split(wss.Server.URL, ":")
	WsServerPort := urlSplit[len(urlSplit)-1]
	return WsServerPort
}

func CreateWebSocketServer(ctx context.Context) *WebSocketServer {
	log.Println("Creating/starting test http server that accepts websockets")

	handlerFn := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acceptOpts := websocket.AcceptOptions{
			// CompressionMode: websocket.CompressionContextTakeover,
			CompressionMode: websocket.CompressionDisabled,
		}
		wsConn, err := websocket.Accept(w, r, &acceptOpts)
		if err != nil {
			log.Fatalln("websocket.Accept() failed:", err)
		}

		// Disable read limit to test large payloads being sent/recv
		wsConn.SetReadLimit(-1)

		for {
			_, msg, readErr := wsConn.Read(ctx)
			if readErr != nil {
				if websocket.CloseStatus(readErr) == websocket.StatusNormalClosure || errors.Is(readErr, net.ErrClosed) || errors.Is(readErr, context.Canceled) {
					return
				}
				log.Fatalln("Failed to read msg from ws conn", readErr, websocket.CloseStatus(readErr))
			}

			switch string(msg) {
			case "ECHO":
				go handleEcho(ctx, msg, wsConn)
			case "SPAM_START":
				go handleSpam(ctx, wsConn)
			case "TRIGGER_CLOSE":
				go handleTriggerClose(ctx, wsConn)
			default:
				// Echo back message if not recognized
				go handleEcho(ctx, msg, wsConn)
			}
		}
	})

	ts := httptest.NewUnstartedServer(handlerFn)

	wss := WebSocketServer{
		Server: ts,
	}

	return &wss
}

func CreateWebSocketClient(url string) (*websocket.Conn, error) {
	log.Println("Creating a ws client and connecting to", url)
	ctx := context.Background()

	r := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return net.Dial("udp", dnsserver.LOCALHOST_DNS_SERVER)
		},
	}
	dialer := &net.Dialer{
		Resolver: r,
	}

	tp := &http.Transport{
		DialContext: dialer.DialContext,
	}
	client := &http.Client{Transport: tp}

	dialOpts := websocket.DialOptions{
		HTTPClient:      client,
		CompressionMode: websocket.CompressionDisabled,
	}

	wsConn, _, err := websocket.Dial(ctx, url, &dialOpts)
	if err != nil {
		return nil, err
	}

	// Disable read limit to test large payloads being sent/recv
	wsConn.SetReadLimit(-1)

	return wsConn, err
}
