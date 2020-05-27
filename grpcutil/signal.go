package grpcutil

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

// ServeForever runs the given server until termination via signal.
func ServeForever(lis net.Listener, s *grpc.Server) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	go handleSignals(ch, s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("%s", err)
	}
	log.Fatalf("gRPC server shutdown, exiting")
}

func handleSignals(ch chan os.Signal, s *grpc.Server) {
	sig := <-ch
	log.Warning("Received signal %s, gracefully shutting down server", sig)
	go func() {	s.GracefulStop() }()
	select {
	case sig := <-ch:
		log.Warning("Received signal %s, less politely shutting down", sig)
	case <-time.After(5 * time.Minute):  // This is quite long, but most of our servers can have long-running RPCs.
		log.Warning("Graceful stop not terminated, escalating to impolite shutdown")
	}
	go func() { s.Stop() }()
	select {
	case sig := <-ch:
		log.Fatalf("Received signal %s, dying immediately", sig)
	case <-time.After(10 * time.Second):
		log.Fatalf("Abrupt stop not terminated, dying now")
	}
}
