package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/mingsorg/ClickHouse_lms/lms/internal/doctor"
)

func main() { os.Exit(run()) }
func run() int {
	if len(os.Args) < 2 || os.Args[1] != "doctor" {
		fmt.Fprintln(os.Stderr, "usage: lmsctl doctor --config path.json")
		return 2
	}
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	file := flags.String("config", "", "explicit node inventory JSON file")
	if flags.Parse(os.Args[2:]) != nil || *file == "" || flags.NArg() != 0 {
		return 2
	}
	b, err := os.ReadFile(*file)
	if err != nil || len(b) > 64*1024 {
		fmt.Fprintln(os.Stderr, "cannot read bounded doctor configuration")
		return 2
	}
	c, err := doctor.Parse(b)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r := doctor.Run(ctx, client, c)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if encoder.Encode(r) != nil {
		return 2
	}
	if r.Status == "FAIL" {
		return 1
	}
	return 0
}
