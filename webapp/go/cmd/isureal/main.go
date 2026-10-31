package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"

	"example.com/isureal-company/isureal"
)

// 2026-09-23

func main() {
	if addr := os.Getenv("ISUREAL_PPROF_ADDR"); addr != "" {
		go http.ListenAndServe(addr, nil)
	}
	config := isureal.ConfigFromEnv()
	server := isureal.NewServer(config)
	isureal.StartFrontFromEnv(server, config.Region) // ISUREAL_FRONT=1 のときだけ :80 (frontproxy.go)
	if err := server.Start(":8080"); err != nil {
		panic("failed to start server: " + err.Error())
	}
}
