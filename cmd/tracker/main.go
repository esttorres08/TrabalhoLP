// Inicia o tracker centralizado.
//
// Uso:
//
//	go run ./cmd/tracker [--host 0.0.0.0] [--port 5000]
package main

import (
	"flag"
	"fmt"
	"os"

	"trabalholp/tracker"
)

func main() {
	host := flag.String("host", "0.0.0.0", "Host de bind (default: 0.0.0.0)")
	port := flag.Int("port", 5000, "Porta TCP (default: 5000)")
	flag.Parse()

	if err := tracker.New(*host, *port).Start(); err != nil {
		fmt.Fprintf(os.Stderr, "[Tracker] erro: %v\n", err)
		os.Exit(1)
	}
}
