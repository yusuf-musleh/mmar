package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/yusuf-musleh/mmar/constants"
	"github.com/yusuf-musleh/mmar/internal/client"
	"github.com/yusuf-musleh/mmar/internal/server"
	"github.com/yusuf-musleh/mmar/internal/utils"
)

func main() {
	serverCmd := flag.NewFlagSet(constants.SERVER_CMD, flag.ExitOnError)
	serverHttpPort := serverCmd.String(
		"http-port", constants.SERVER_HTTP_PORT, constants.SERVER_HTTP_PORT_HELP,
	)
	serverTcpPort := serverCmd.String(
		"tcp-port", constants.SERVER_TCP_PORT, constants.SERVER_TCP_PORT_HELP,
	)

	clientCmd := flag.NewFlagSet(constants.CLIENT_CMD, flag.ExitOnError)
	clientPort := clientCmd.String(
		"port", constants.CLIENT_PORT, constants.CLIENT_PORT_HELP,
	)
	clientTunnelHost := clientCmd.String(
		"tunnel-host", constants.TUNNEL_HOST, constants.TUNNEL_HOST_HELP,
	)

	versionCmd := flag.NewFlagSet(constants.VERSION_CMD, flag.ExitOnError)
	versionCmd.Usage = utils.MmarVersionUsage

	flag.Usage = utils.MmarUsage

	if len(os.Args) < 2 {
		utils.MmarUsage()
		os.Exit(0)
	}

	switch os.Args[1] {
	case constants.SERVER_CMD:
		serverCmd.Parse(os.Args[2:])
		fmt.Println("subcommand 'server'")
		fmt.Println("  http port:", *serverHttpPort)
		fmt.Println("  tcp port:", *serverTcpPort)
		fmt.Println("  tail:", serverCmd.Args())
		server.Run(*serverTcpPort, *serverHttpPort)
	case constants.CLIENT_CMD:
		clientCmd.Parse(os.Args[2:])
		fmt.Println("subcommand 'client'")
		fmt.Println("  port:", *clientPort)
		fmt.Println("  tunnel-host:", *clientTunnelHost)
		fmt.Println("  tail:", clientCmd.Args())
		client.Run(*serverTcpPort, *clientTunnelHost)
	case constants.VERSION_CMD:
		versionCmd.Parse(os.Args[2:])
		fmt.Println("mmar version", constants.MMAR_VERSION)
	default:
		utils.MmarUsage()
	}
}
