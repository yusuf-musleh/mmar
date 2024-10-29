package constants

import "time"

const (
	SERVER_CMD       = "server"
	CLIENT_CMD       = "client"
	CLIENT_PORT      = "8000"
	SERVER_HTTP_PORT = "3376"
	SERVER_TCP_PORT  = "6673"
	TUNNEL_HOST      = "https://mmar.dev"

	GRACEFUL_SHUTDOWN_TIMEOUT = 3 * time.Second
	ID_CHARSET                = "abcdefghijklmnopqrstuvwxyz0123456789"
	ID_LENGTH                 = 6
)
