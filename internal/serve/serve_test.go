package serve

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestUntilStopsOnCancel(t *testing.T) {
	srv := &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Until(ctx, srv) }()

	time.Sleep(50 * time.Millisecond) // let it start
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Until did not return after cancel")
	}
}

func TestUntilReportsStartFailure(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	srv := &http.Server{Addr: busy.Addr().String(), Handler: http.NotFoundHandler()}
	if err := Until(context.Background(), srv); err == nil {
		t.Fatal("want error for a port already in use")
	}
}
