package main

import (
	"context"
	"fmt"
	"log"

	"github.com/yusuf-musleh/mmar/simulations/websocketserver/utils"
)

func main() {
	ctx := context.Background()
	wsServer := utils.CreateWebSocketServer(ctx)
	wsServer.Server.Start()
	log.Println("Listening on", wsServer.Server.URL, "for websocket connections")
	defer wsServer.Server.Close()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("shutting down wsserver")
			return
		default:
			// do nothing
		}
	}
}
