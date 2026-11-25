// Package serve runs an HTTP server for the lifetime of a context, the
// pattern shared by every binary in cmd/.
package serve

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// ShutdownTimeout bounds how long in-flight requests get to finish.
const ShutdownTimeout = 5 * time.Second

// Until serves srv until ctx is canceled, then shuts it down gracefully.
// It returns an error if the server fails to start (e.g. port in use) or
// stops on its own, and nil after a clean shutdown.
func Until(ctx context.Context, srv *http.Server) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	select {
	case err := <-serveErr:
		return fmt.Errorf("http server on %s: %w", srv.Addr, err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	return nil
}
