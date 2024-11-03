package protocol

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
)

const (
	HEARTBEAT = iota + 1
	REQUEST
	RESPONSE
	CLIENT_CONNECT
	CLIENT_DISCONNECT
	CLIENT_TUNNEL_LIMIT
	LOCALHOST_NOT_RUNNING
)

var MESSAGE_MAPPING = map[int]string{
	HEARTBEAT:             "HEARTBEAT",
	REQUEST:               "REQUEST",
	RESPONSE:              "RESPONSE",
	CLIENT_CONNECT:        "CLIENT_CONNECT",
	CLIENT_DISCONNECT:     "CLIENT_DISCONNECT",
	CLIENT_TUNNEL_LIMIT:   "CLIENT_TUNNEL_LIMIT",
	LOCALHOST_NOT_RUNNING: "LOCALHOST_NOT_RUNNING",
}

type Tunnel struct {
	Id   string
	Conn net.Conn
}

type TunnelInterface interface {
	ProcessTunnelMessages(ctx context.Context)
}

func TunnelErrStateResp(errState int) http.Response {
	// TODO: Have nicer/more elaborative error messages/pages
	errStates := map[int]string{
		CLIENT_DISCONNECT:     "Tunnel is closed, cannot connect to mmar client.",
		LOCALHOST_NOT_RUNNING: "Tunneled successfully, but nothing is running on localhost.",
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

type TunnelMessage struct {
	MsgType int
	MsgData []byte
}

func (tm *TunnelMessage) serializeMessage() ([]byte, error) {
	serializedMsg := [][]byte{}

	// TODO: Come up with more effecient protocol for server<->client
	// Determine message type to add prefix
	msgType := MESSAGE_MAPPING[tm.MsgType]
	if msgType == "" {
		log.Fatalf("Invalid TunnelMessage type: %v:", tm.MsgType)
	}

	// Add the message type
	serializedMsg = append(serializedMsg, []byte(msgType))
	// Add message data bytes length
	serializedMsg = append(serializedMsg, []byte(strconv.Itoa(len(tm.MsgData))))
	// Add the message data
	serializedMsg = append(serializedMsg, tm.MsgData)

	// Combine all the data separated by new lines
	return bytes.Join(serializedMsg, []byte("\n")), nil
}

func (tm *TunnelMessage) readMessageData(length int, reader *bufio.Reader) []byte {
	msgData := make([]byte, length)

	if _, err := io.ReadFull(reader, msgData); err != nil {
		log.Fatalf("Failed to read all Msg Data: %v", err)
	}

	return msgData
}

func (tm *TunnelMessage) deserializeMessage(reader *bufio.Reader) error {
	msgPrefix, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	msgLengthStr, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	msgLength, err := strconv.Atoi(msgLengthStr[:len(msgLengthStr)-1])
	if err != nil {
		log.Fatalf("Could not parse message length: %v", msgLengthStr)
	}

	var msgType int
	msgData := tm.readMessageData(msgLength, reader)

	// TODO: is there a better way to do this?
	switch msgPrefix {
	case "HEARTBEAT\n":
		log.Printf("Got HEARTBEAT\n")
		msgType = HEARTBEAT
	case "REQUEST\n":
		msgType = REQUEST
	case "RESPONSE\n":
		msgType = RESPONSE
	case "CLIENT_CONNECT\n":
		msgType = CLIENT_CONNECT
	case "CLIENT_DISCONNECT\n":
		msgType = CLIENT_DISCONNECT
	case "CLIENT_TUNNEL_LIMIT\n":
		msgType = CLIENT_TUNNEL_LIMIT
	case "LOCALHOST_NOT_RUNNING\n":
		msgType = LOCALHOST_NOT_RUNNING
	default:
		// TODO: Gracefully handle non-protocol message received
		log.Fatalf("Invalid TunnelMessage prefix: %v", msgPrefix)
	}

	tm.MsgType = msgType
	tm.MsgData = msgData

	return nil
}

func (t *Tunnel) SendMessage(tunnelMsg TunnelMessage) error {
	// Serialize tunnel message data
	serializedMsg, serializeErr := tunnelMsg.serializeMessage()
	if serializeErr != nil {
		return serializeErr
	}
	_, err := t.Conn.Write(serializedMsg)
	return err
}

func (t *Tunnel) ReceiveMessage() (TunnelMessage, error) {
	msgReader := bufio.NewReader(t.Conn)

	// Read and deserialize tunnel message data
	tunnelMessage := TunnelMessage{}
	deserializeErr := tunnelMessage.deserializeMessage(msgReader)

	return tunnelMessage, deserializeErr
}
