// pseudobench: 公式ベンチに頼らない整合性チェッカー (check) と疑似負荷 (load)。
//
//	go run . check [-target URL]
//	go run . load  [-target URL] [-secs 25] [-c 20]
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: pseudobench check|load [flags]")
		os.Exit(2)
	}
	mode := os.Args[1]
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	target := fs.String("target", envOr("PSEUDOBENCH_TARGET", "https://57.183.58.128"), "router base URL")
	secs := fs.Int("secs", 25, "load duration seconds (max 30)")
	conc := fs.Int("c", 20, "load concurrency")
	scale := fs.Float64("scale", 1.0, "load: users per region multiplier (base asia18/europe7/america6)")
	noInit := fs.Bool("no-init", false, "skip POST /api/initialize")
	verbose := fs.Bool("v", false, "verbose")
	_ = fs.Parse(os.Args[2:])
	base := strings.TrimRight(*target, "/")

	switch mode {
	case "check":
		initHTTP(8)
		os.Exit(runCheck(base, !*noInit, *verbose))
	case "load":
		if *secs > 30 {
			*secs = 30
		}
		initHTTP(*conc)
		os.Exit(runLoad(base, time.Duration(*secs)*time.Second, *conc, *scale, !*noInit, *verbose))
	default:
		fmt.Fprintln(os.Stderr, "unknown mode:", mode)
		os.Exit(2)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
