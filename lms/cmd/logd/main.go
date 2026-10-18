package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mingsorg/ClickHouse_lms/lms/internal/logservice"
)

func value(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}

func main() {
	var grants map[string]logservice.Grant
	if err := json.Unmarshal([]byte(os.Getenv("LMS_TOKENS")), &grants); err != nil {
		log.Fatal("LMS_TOKENS must contain a JSON credential-to-grant map")
	}
	if os.Getenv("CLICKHOUSE_PASSWORD") == "" {
		log.Fatal("CLICKHOUSE_PASSWORD is required")
	}
	store, err := logservice.NewHTTPStore(value("CLICKHOUSE_URL", "http://127.0.0.1:8123"), value("CLICKHOUSE_USER", "lms"), os.Getenv("CLICKHOUSE_PASSWORD"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app, err := logservice.NewServer(ctx, store, grants, []byte(os.Getenv("LMS_CURSOR_KEY")))
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Addr: value("LMS_LISTEN", "127.0.0.1:8080"), Handler: app, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 25 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-stop
		shutdownCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Print("shutdown deadline exceeded")
		}
		cancel()
	}()
	log.Print("LMS logging service starting; append-only protocol subset")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	<-shutdownDone
}
