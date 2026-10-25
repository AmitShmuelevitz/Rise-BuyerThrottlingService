// Package app holds process wiring shared by the binaries: Redis connection,
// HTTP server lifecycle and signal handling.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// SignalContext is cancelled on SIGINT or SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// ConnectRedis connects to addr, retrying until it answers PING or ctx ends,
// so services can start before Redis in docker-compose.
func ConnectRedis(ctx context.Context, addr string, logger *slog.Logger) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     32,
	})
	backoff := 200 * time.Millisecond
	for {
		err := rdb.Ping(ctx).Err()
		if err == nil {
			logger.Info("connected to redis", "addr", addr)
			return rdb, nil
		}
		logger.Warn("waiting for redis", "addr", addr, "err", err)
		select {
		case <-ctx.Done():
			_ = rdb.Close()
			return nil, fmt.Errorf("connect to redis at %s: %w", addr, ctx.Err())
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// ServeHTTP runs srv until ctx is done, then shuts it down gracefully.
func ServeHTTP(ctx context.Context, srv *http.Server, logger *slog.Logger) error {
	errc := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// NewServer returns an http.Server with sane timeouts.
func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
