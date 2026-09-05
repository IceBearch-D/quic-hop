package main

import (
    "flag"
    "fmt"
    "log"

    "net_1218/pkg/magic"
)

func main() {
    serverMode := flag.Bool("s", false, "start calibration server")
    clientMode := flag.Bool("c", false, "run client sync with server")
    addr := flag.String("addr", magic.ListenAddr, "server IP for calibration")
    flag.Parse()

    switch {
    case *serverMode:
        fmt.Printf("[Sync] Starting calibration server at %s:9999\n", *addr)
        // Override ListenAddr at runtime if flag is used
        magic.ListenAddr = *addr
        magic.StartCalibrationServer()
    case *clientMode:
        target := magic.SyncAddr
        fmt.Printf("[Sync] Calibrating with %s:9999...\n", target)
        lat := magic.SyncWithServer(target)
        fmt.Printf("[Sync] Result latency (offset+RTT/2): %v\n", lat)
    default:
        log.Fatalf("choose one mode: -s to start server, -c to run client (use -addr to set IP)")
    }
}
