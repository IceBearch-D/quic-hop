package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	addr      = "192.168.43.2:4244"
	totalSize = 100 * 1024 * 1024
	chunkSize = 64 * 1024
)

func main() {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"quic-test"},
	}

	quicConf := &quic.Config{
		MaxConnectionReceiveWindow:     128 * 1024 * 1024,
		InitialConnectionReceiveWindow: 64 * 1024 * 1024,
		DisablePathMTUDiscovery:        true,
	}

	// 1. 客户端绑定本地端口
	localAddr, _ := net.ResolveUDPAddr("udp", ":0")
	pconn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		log.Fatal(err)
	}

	// 2. 【核心】强制设置发送缓冲区 (客户端主要是发数据，WriteBuffer 很重要)
	err = pconn.SetWriteBuffer(32 * 1024 * 1024)
	if err != nil {
		log.Printf("警告: SetWriteBuffer error: %v", err)
	}
	pconn.SetReadBuffer(32 * 1024 * 1024) // 为了收 ACK

	// 3. 解析服务器地址
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("正在连接 %s (Pointer Mode)...\n", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 4. 使用 quic.Dial (传入 pconn)
	// 假设你的版本 quic.Dial 返回 *quic.Conn
	conn, err := quic.Dial(ctx, pconn, remoteAddr, tlsConf, quicConf)
	if err != nil {
		log.Fatal("连接失败: ", err)
	}

	// 5. 打开流
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatal("打开流失败: ", err)
	}

	fmt.Println("开始发送数据...")
	buf := make([]byte, chunkSize)
	remaining := totalSize
	start := time.Now()

	for remaining > 0 {
		toWrite := chunkSize
		if remaining < chunkSize {
			toWrite = remaining
		}
		// stream 为 *quic.Stream 指针
		_, err := stream.Write(buf[:toWrite])
		if err != nil {
			log.Fatal(err)
		}
		remaining -= toWrite
	}

	stream.Close()
	fmt.Printf("发送完毕! 耗时: %v\n", time.Since(start))
	time.Sleep(500 * time.Millisecond)
}
