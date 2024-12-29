package protocol

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/yusuf-musleh/mmar/constants"
)

const (
	REQUEST = uint8(iota + 1)
	RESPONSE
	CLIENT_CONNECT
	CLIENT_DISCONNECT
	CLIENT_TUNNEL_LIMIT
	LOCALHOST_NOT_RUNNING
)

var INVALID_MESSAGE_PROTOCOL_VERSION = errors.New("Invalid Message Protocol Version")
var INVALID_MESSAGE_TYPE = errors.New("Invalid Tunnel Message Type")

func isValidTunnelMessageType(mt uint8) (uint8, error) {
	// Iterate through all the message type, from first to last, checking
	// if the provided message type matches one of them
	for msgType := REQUEST; msgType <= LOCALHOST_NOT_RUNNING; msgType++ {
		if mt == msgType {
			return msgType, nil
		}
	}

	return 0, INVALID_MESSAGE_TYPE
}

func TunnelErrState(errState uint8) string {
	// TODO: Have nicer/more elaborative error messages/pages
	errStates := map[uint8]string{
		CLIENT_DISCONNECT:     constants.CLIENT_DISCONNECT_ERR_TEXT,
		LOCALHOST_NOT_RUNNING: constants.LOCALHOST_NOT_RUNNING_ERR_TEXT,
	}
	fallbackErr := "An error occured while attempting to tunnel."

	tunnelErr, ok := errStates[errState]
	if !ok {
		tunnelErr = fallbackErr
	}
	return tunnelErr
}

func RespondTunnelErr(errState uint8, w http.ResponseWriter) {
	errBody := TunnelErrState(errState)

	w.Header().Set("Content-Length", strconv.Itoa(len(errBody)))
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(errBody))
}

type Tunnel struct {
	Id        string
	Conn      net.Conn
	CreatedOn time.Time
}

type TunnelInterface interface {
	ProcessTunnelMessages(ctx context.Context)
}

type TunnelMessage struct {
	MsgType uint8
	MsgData []byte
}

// A TunnelMessage is serialized in the following format:
//
// +---------+------------+---------------------+------------+-------------------------+
// | Version | Msg Type   | Length of Msg Data  | Delimiter  | Message Data            |
// | (1 byte)| (1 byte)   | (1 or more bytes)   | (1 byte)   | (Variable Length)       |
// +---------+------------+---------------------+------------+-------------------------+
func (tm *TunnelMessage) serializeMessage() ([]byte, error) {
	serializedMsg := [][]byte{}

	// Determine and validate message type to add prefix
	msgType, err := isValidTunnelMessageType(tm.MsgType)
	if err != nil {
		// TODO: Gracefully handle non-protocol message received
		log.Fatalf("Invalid TunnelMessage type: %v:", tm.MsgType)
	}

	// Add version of TunnelMessage protocol and TunnelMessage type
	serializedMsg = append(
		serializedMsg,
		[]byte{byte(constants.TUNNEL_MESSAGE_PROTOCOL_VERSION), byte(msgType)},
	)

	// Add message data bytes length
	serializedMsg = append(serializedMsg, []byte(strconv.Itoa(len(tm.MsgData))))

	// Add delimiter to know where the data content starts in the message
	serializedMsg = append(serializedMsg, []byte{byte(constants.TUNNEL_MESSAGE_DATA_DELIMITER)})

	// Add the message data
	serializedMsg = append(serializedMsg, tm.MsgData)

	// Combine all the data with no separators
	return bytes.Join(serializedMsg, nil), nil
}

func (tm *TunnelMessage) readMessageData(length int, reader *bufio.Reader) []byte {
	msgData := make([]byte, length)

	if _, err := io.ReadFull(reader, msgData); err != nil {
		log.Fatalf("Failed to read all Msg Data: %v", err)
	}

	return msgData
}

func (tm *TunnelMessage) deserializeMessage(reader *bufio.Reader) error {
	msgProtocolVersion, err := reader.ReadByte()
	if err != nil {
		return err
	}

	// Check if the message protocol version is correct
	if uint8(msgProtocolVersion) != constants.TUNNEL_MESSAGE_PROTOCOL_VERSION {
		return INVALID_MESSAGE_PROTOCOL_VERSION
	}

	msgPrefix, err := reader.ReadByte()
	if err != nil {
		return err
	}

	msgType, err := isValidTunnelMessageType(msgPrefix)
	if err != nil {
		// TODO: Gracefully handle non-protocol message received
		log.Fatalf("Invalid TunnelMessage prefix: %v", msgPrefix)
	}

	msgLengthStr, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	// Determine the length of the data by stripping out the '\n' and convert to int
	msgLength, err := strconv.Atoi(msgLengthStr[:len(msgLengthStr)-1])
	if err != nil {
		// TODO: Gracefully handle invalid message data length
		log.Fatalf("Could not parse message length: %v", msgLengthStr)
	}

	msgData := tm.readMessageData(msgLength, reader)

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
