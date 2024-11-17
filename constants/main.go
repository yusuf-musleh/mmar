package constants

import "time"

const (
	MMAR_VERSION = "0.0.0"

	VERSION_CMD       = "version"
	SERVER_CMD        = "server"
	CLIENT_CMD        = "client"
	CLIENT_LOCAL_PORT = "8000"
	SERVER_HTTP_PORT  = "3376"
	SERVER_TCP_PORT   = "6673"
	TUNNEL_HOST       = "https://mmar.dev"

	SERVER_STATS_DEFAULT_USERNAME = "admin"
	SERVER_STATS_DEFAULT_PASSWORD = "admin"

	SERVER_HTTP_PORT_HELP = "Define port where mmar will bind to and run on server for HTTP requests."
	SERVER_TCP_PORT_HELP  = "Define port where mmar will bind to and run on server for TCP connections."

	CLIENT_LOCAL_PORT_HELP = "Define the port where your local dev server is running to expose through mmar."
	CLIENT_HTTP_PORT_HELP  = "Define port of mmar HTTP server to make requests through the tunnel."
	CLIENT_TCP_PORT_HELP   = "Define port of mmar TCP server for client to connect to, creating a tunnel."
	TUNNEL_HOST_HELP       = "Define host domain of mmar server for client to connect to."

	TUNNEL_MESSAGE_PROTOCOL_VERSION = 1
	TUNNEL_MESSAGE_DATA_DELIMITER   = '\n'
	ID_CHARSET                      = "abcdefghijklmnopqrstuvwxyz0123456789"
	ID_LENGTH                       = 6

	MAX_TUNNELS_PER_IP        = 5
	GRACEFUL_SHUTDOWN_TIMEOUT = 3 * time.Second

	// TERMINAL ANSI ESCAPED COLORS
	DEFAULT_COLOR = ""
	RED           = "\033[31m"
	GREEN         = "\033[32m"
	YELLOW        = "\033[33m"
	BLUE          = "\033[34m"
	RESET         = "\033[0m"
)

var (
	MMAR_SUBCOMMANDS = [][]string{
		{"server", "Runs a mmar server. Run this on your publicly reachable server if you're self-hosting mmar."},
		{"client", "Runs a mmar client. Run this on your machine to expose your localhost on a public URL."},
		{"version", "Prints the installed version of mmar."},
	}
)
