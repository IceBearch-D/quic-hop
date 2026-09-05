package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	BasePort    = 30000
	HopInterval = 10 * time.Millisecond
	TimePort    = BasePort - 1
)

var TotalCycle = HopInterval.Milliseconds() * 20000

// Moving-target client: sends a UDP message to port rule 30000 + ((nowMs % 2_000_000)/200),
// i.e., 5 hops/sec cycling through 30000-39999. Prints send rate stats.

func main() {
	serverHost := flag.String("host", "192.168.10.11", "server host")
	interval := flag.Duration("interval", 20*time.Millisecond, "send interval")
	flag.Parse()

	offset, rtt, err := syncTime(*serverHost)
	if err != nil {
		log.Printf("time sync failed, using local clock only: %v", err)
	} else {
		log.Printf("time sync success: offset=%v rtt=%v", offset, rtt)
	}

	conn, err := net.ListenPacket("udp", "0.0.0.0:0")
	if err != nil {
		log.Fatalf("listen udp: %v", err)
	}
	defer conn.Close()
	log.Printf("client local %s", conn.LocalAddr())

	var sent, failed int64
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		var lastSent, lastFail int64
		for range ticker.C {
			s := atomic.LoadInt64(&sent)
			f := atomic.LoadInt64(&failed)
			log.Printf("[stats] sent=%d/s fail=%d/s", s-lastSent, f-lastFail)
			lastSent, lastFail = s, f
		}
	}()

	for {
		port := computePort(offset, rtt)
		raddr := &net.UDPAddr{IP: net.ParseIP(*serverHost), Port: port}
		msg := fmt.Sprintf("from=%s randPort=%d t=%d", conn.LocalAddr(), randomPort(), time.Now().UnixNano())
		if _, err := conn.WriteTo([]byte(msg), raddr); err != nil {
			atomic.AddInt64(&failed, 1)
			log.Printf("send error: %v", err)
		} else {
			atomic.AddInt64(&sent, 1)
		}
		time.Sleep(*interval)
	}
}

// syncTime 向时间端口询问服务器时间，返回偏移量（服务器 - 本地）和 RTT。
func syncTime(host string) (time.Duration, time.Duration, error) {
	addr := fmt.Sprintf("%s:%d", host, TimePort)
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	_ = conn.SetDeadline(deadline)

	sendTime := time.Now()
	if _, err := conn.Write([]byte("time?")); err != nil {
		return 0, 0, err
	}

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	recvTime := time.Now()
	if err != nil {
		return 0, 0, err
	}

	serverNs, err := strconv.ParseInt(string(buf[:n]), 10, 64)
	if err != nil {
		return 0, 0, err
	}

	serverTime := time.Unix(0, serverNs)
	rtt := recvTime.Sub(sendTime)
	offset := serverTime.Sub(sendTime.Add(rtt / 2))
	return offset, rtt, nil
}

func computePort(offset, rtt time.Duration) int {
	arrivalEstimate := time.Now().Add(offset + rtt/2)
	ms := arrivalEstimate.UnixMilli() % TotalCycle
	bucket := ms / HopInterval.Milliseconds()
	return BasePort + int(bucket)
}

// 随机生成客户端端口的可选帮助程序
func randomPort() int {
	return 20000 + rand.Intn(20000)
}
