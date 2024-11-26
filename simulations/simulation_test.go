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

func StartMmarClient(ctx context.Context) {
	cmd := exec.CommandContext(ctx, "./mmar", "client", "--tunnel-host", "localhost")

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

func TestSimulation(t *testing.T) {
	simulationCtx, simulationCancel := context.WithCancel(context.Background())

	go StartMmarServer(simulationCtx)
	wait := time.NewTimer(2 * time.Second)
	<-wait.C
	wait.Reset(2 * time.Second)
	go StartMmarClient(simulationCtx)
	<-wait.C

	simulationCancel()

	wait.Reset(6 * time.Second)
	<-wait.C
}
