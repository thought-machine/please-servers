// Package main implements a test runner which orchestrates starting up
// and tearing down servers in the background, and ensuring they are ready before continuing.
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"time"
)

func Main() error {
	// Build the servers first (so we don't wait for ports to open while we're actually compiling)
	plz := exec.Command("./pleasew", "buildlocal", "-p", "-v", "notice")
	plz.Stdout = os.Stdout
	plz.Stderr = os.Stderr
	if err := plz.Run(); err != nil {
		return err
	}

	// Start the servers in the background and keep them running as we go
	servers := exec.Command("./pleasew", "runlocal")
	if err := servers.Start(); err != nil {
		return err
	}
	defer servers.Process.Signal(os.Interrupt)

	if err := checkPort("7772"); err != nil {
		return err
	}

	plz = exec.Command("./pleasew", "--profile", "localremote", "test", "//tests/...", "-p", "-v", "notice")
	plz.Stdout = os.Stdout
	plz.Stderr = os.Stderr
	return plz.Run()
}

func checkPort(port string) error {
	const tries = 20
	for i := 0; i < tries; i++ {
		conn, err := net.Dial("tcp", "127.0.0.1:"+port)
		if err != nil {
			log.Printf("Waiting for port %s to open...", port)
			time.Sleep(time.Second)
			continue
		}
		conn.Close()
		return nil
	}
	return fmt.Errorf("Failed to find open port %s after %d tries", port, tries)
}

func main() {
	if err := Main(); err != nil {
		log.Fatalf("%s", err)
	}
}
