package simulations

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/yusuf-musleh/mmar/simulations/devserver"
)

func StartMmarServer(ctx context.Context) {
	cmd := exec.CommandContext(ctx, "./mmar", "server")

	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.Cancel = func() error {
		fmt.Println("cancelled, server")
		wait := time.NewTimer(4 * time.Second)
		<-wait.C
		return cmd.Process.Signal(os.Interrupt)
	}

	err := cmd.Run()
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func StartMmarClient(ctx context.Context, localDevServerPort string) {
	cmd := exec.CommandContext(
		ctx,
		"./mmar",
		"client",
		"--tunnel-host",
		"localhost",
		"--local-port",
		localDevServerPort,
	)

	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.Cancel = func() error {
		fmt.Println("cancelled, client")
		return cmd.Process.Signal(os.Interrupt)
	}

	err := cmd.Run()
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func StartLocalDevServer() *devserver.DevServer {
	ds := devserver.NewDevServer()
	log.Printf("Started local dev server on: http://localhost:%v", ds.Port())
	return ds
}

func TestSimulation(t *testing.T) {
	simulationCtx, simulationCancel := context.WithCancel(context.Background())

	localDevServer := StartLocalDevServer()
	defer localDevServer.Close()

	go StartMmarServer(simulationCtx)
	wait := time.NewTimer(2 * time.Second)
	<-wait.C
	wait.Reset(2 * time.Second)
	go StartMmarClient(simulationCtx, localDevServer.Port())
	<-wait.C

	simulationCancel()

	wait.Reset(6 * time.Second)
	<-wait.C
}
