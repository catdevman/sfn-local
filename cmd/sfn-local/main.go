package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/catdevman/sfn-local/internal/api"
	"github.com/catdevman/sfn-local/internal/exec"
	"github.com/catdevman/sfn-local/internal/mock"
	"github.com/catdevman/sfn-local/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(run(os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, ctx))
}

func run(args []string, getenv func(string) (string, bool), stdout, stderr io.Writer, ctx context.Context) int {
	fmt.Fprintln(stdout, "sfn-local "+Version)
	cfg, err := parseArgs(args, getenv)
	if errors.Is(err, errHelp) {
		fmt.Fprint(stdout, helpText())
		return 0
	}
	if errors.Is(err, errVersion) {
		fmt.Fprintln(stdout, "sfn-local "+Version)
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, err.Error())
		fmt.Fprint(stderr, helpText())
		return 1
	}
	if cfg.WaitInvalid {
		fmt.Fprintf(stderr, "invalid wait time scale %q\n", cfg.WaitRaw)
	}
	if cfg.AccessKeyID == "" {
		fmt.Fprintln(stdout, "Failed to load credentials, default to dummy credentials...")
		cfg.AccessKeyID = "abcd"
		cfg.SecretAccessKey = "1234"
	}
	return serve(ctx, cfg, stdout, stderr)
}

func waitScale(v *float64) *float32 {
	if v == nil {
		return nil
	}
	s := float32(math.Abs(*v))
	return &s
}

func serve(ctx context.Context, cfg Config, stdout, stderr io.Writer) int {
	if ctx == nil {
		ctx = context.Background()
	}
	surface, err := api.LoadSurface()
	if err != nil {
		fmt.Fprintf(stderr, "surface: %v\n", err)
		return 1
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(cfg.BindAddress, fmt.Sprintf("%d", cfg.Port)))
	if err != nil {
		fmt.Fprintf(stderr, "listen: %v\n", err)
		return 1
	}
	tcp, ok := ln.Addr().(*net.TCPAddr)
	port := cfg.Port
	if ok {
		port = tcp.Port
	}
	fmt.Fprintf(stdout, "Starting server on port %d with account %s, region %s, bind address %s\n",
		port, cfg.Account, cfg.Region, cfg.BindAddress)
	st := store.New()
	go exec.Sweep(ctx, st)
	runner := &exec.Runner{
		Store:          st,
		Log:            stdout,
		WaitScale:      waitScale(cfg.WaitTimeScale),
		Mocks:          &mock.Repo{Path: cfg.MockConfig},
		LambdaEndpoint: cfg.LambdaEndpoint,
		Region:         cfg.Region,
		Account:        cfg.Account,
	}
	srv := &http.Server{
		Handler: &api.Server{
			Handlers: &api.Service{
				Store:   st,
				Runner:  runner,
				Account: cfg.Account,
				Region:  cfg.Region,
				Profile: cfg.Profile,
				Log:     stdout,
			},
			Surface: surface,
			Profile: cfg.Profile,
			Log:     stdout,
		},
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
		err = <-errCh
	case err = <-errCh:
	}
	if err != nil {
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return 1
	}
	return 0
}
