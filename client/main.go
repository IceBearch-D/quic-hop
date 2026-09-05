package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/quic-go/quic-go"
)

// ================= 配置区域 (必须与服务端保持一致) =================
const (
	BaseListenPort = 20000                   // 监听起始端口 (L_port)
	SendPortOffset = 20000                   // 发送端口偏移量 (S_port = L_port + Offset) -> 40000起始
	HopInterval    = 3000 * time.Millisecond // 跳变间隔，单位：毫秒
	TimePort       = BaseListenPort - 1      // 固定时间校对端口
)

// TotalCycle 单位毫秒：HopInterval(毫秒) * 20000
var TotalCycle = HopInterval.Milliseconds() * 20000

func main() {
	serverIP := flag.String("server", "192.168.43.2", "Server IP address")
	localPort := flag.Int("port", 4242, "Local QUIC listen port")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	offset, rtt, err := syncTime(*serverIP)
	if err != nil {
		log.Printf("[Client] time sync failed, fallback to local clock: %v", err)
		offset, rtt = 0, 0
	} else {
		log.Printf("[Client] time sync success: offset=%v rtt=%v", offset, rtt)
	}

	// 1. 启动 QUIC 接收端 (等待服务端连回来传文件)
	// 生成临时的 TLS 证书
	tlsConf := generateTLSConfig()
	listener, err := quic.ListenAddr(fmt.Sprintf("0.0.0.0:%d", *localPort), tlsConf, &quic.Config{KeepAlivePeriod: 15 * time.Second})
	if err != nil {
		log.Fatalf("Failed to listen QUIC: %v", err)
	}
	defer listener.Close()

	log.Printf("[Client] Listening on :%d, waiting for server connection...", *localPort)

	// 2. 启动一个协程，向服务端发送 UDP 触发包
	// 因为服务端在跳变，且可能丢包，我们每隔一小段时间发一次，直到连接建立
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sendTriggerLoop(ctx, *serverIP, *localPort, offset, rtt)

	// 3. 接受连接并接收文件
	// 这里只演示接收一个连接
	conn, err := listener.Accept(context.Background())
	if err != nil {
		log.Fatalf("Accept failed: %v", err)
	}
	log.Printf("[Client] Connection accepted from %s!", conn.RemoteAddr())

	// 连接建立后，取消触发包发送
	cancel()

	stream, err := conn.AcceptStream(context.Background())
	if err != nil {
		log.Fatalf("AcceptStream failed: %v", err)
	}

	// 4. 读取文件
	if err := receiveFile(stream); err != nil {
		log.Printf("Receive file error: %v", err)
	} else {
		log.Println("File received successfully!")
	}

	// 给一点时间让 ack 发回去
	time.Sleep(1 * time.Second)
}

// 循环发送触发包，追逐服务端的跳变端口
func sendTriggerLoop(ctx context.Context, serverIP string, myPort int, offset, rtt time.Duration) {
	// 获取本机对外 IP (简单处理，仅供演示，实际环境需根据路由选择)
	myIP := getOutboundIP(serverIP)

	ticker := time.NewTicker(HopInterval / 2) // 以 2 倍频率发送，增加命中率
	defer ticker.Stop()

	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		log.Printf("UDP init failed: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("[Client] Start shouting trigger packets to server...")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 核心：基于校时后的估计计算服务端当前端口
			targetPort, _, _ := getCurrentSlotInfoWithOffset(offset, rtt)
			serverAddr := fmt.Sprintf("%s:%d", serverIP, targetPort)

			udpAddr, _ := net.ResolveUDPAddr("udp", serverAddr)
			msg := fmt.Sprintf("%s:%d;%d;%d", myIP, myPort, offset.Nanoseconds(), rtt.Nanoseconds())
			_, err := conn.WriteTo([]byte(msg), udpAddr)
			if err != nil {
				log.Printf("Send trigger failed: %v", err)
			} else {
				// log.Printf("Sent trigger to %s", serverAddr) // 调试时可打开
			}
		}
	}
}

// 核心算法：必须与服务端完全一致
func getCurrentSlotInfo() (int, time.Time, time.Time) {
	now := time.Now()
	nowMs := now.UnixMilli()
	intervalMs := HopInterval.Milliseconds()

	msInCycle := nowMs % int64(TotalCycle)
	bucketIndex := msInCycle / intervalMs
	port := BaseListenPort + int(bucketIndex)

	// 这里只需要端口，时间无所谓
	return port, time.Time{}, time.Time{}
}

// 基于校时后的到达时间估计计算端口。
func getCurrentSlotInfoWithOffset(offset, rtt time.Duration) (int, time.Time, time.Time) {
	arrival := time.Now().Add(offset + rtt/2)
	nowMs := arrival.UnixMilli()
	intervalMs := HopInterval.Milliseconds()

	msInCycle := nowMs % int64(TotalCycle)
	bucketIndex := msInCycle / intervalMs
	port := BaseListenPort + int(bucketIndex)

	return port, time.Time{}, time.Time{}
}

// syncTime 向服务器的时间端口询问时间，返回偏移量（服务器-本地）和 RTT。
func syncTime(server string) (time.Duration, time.Duration, error) {
	addr := net.JoinHostPort(server, fmt.Sprintf("%d", TimePort))
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

// 简单的文件接收协议
func receiveFile(stream io.Reader) error {
	// 1. 读文件名长度
	var nameLen uint16
	if err := binary.Read(stream, binary.BigEndian, &nameLen); err != nil {
		if ae, ok := err.(*quic.ApplicationError); ok && ae.Remote && ae.ErrorCode == 0 {
			return nil // server closed with code 0: treat as clean finish
		}
		return err
	}
	// 2. 读文件名
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(stream, nameBuf); err != nil {
		if ae, ok := err.(*quic.ApplicationError); ok && ae.Remote && ae.ErrorCode == 0 {
			return nil
		}
		return err
	}
	fileName := "recv_" + string(nameBuf)

	// 3. 读文件大小
	var fileSize uint64
	if err := binary.Read(stream, binary.BigEndian, &fileSize); err != nil {
		if ae, ok := err.(*quic.ApplicationError); ok && ae.Remote && ae.ErrorCode == 0 {
			return nil
		}
		return err
	}

	log.Printf("Receiving file: %s (Size: %.2f MB)", fileName, float64(fileSize)/1024/1024)

	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	start := time.Now()
	n, err := io.Copy(f, stream)
	if err != nil {
		if ae, ok := err.(*quic.ApplicationError); ok && ae.Remote && ae.ErrorCode == 0 {
			log.Printf("Remote closed with app code 0 after %d bytes", n)
			return nil
		}
		return err
	}
	duration := time.Since(start)

	log.Printf("Finished. %d bytes in %v (Speed: %.2f MB/s)", n, duration, (float64(n)/1024/1024)/duration.Seconds())
	return nil
}

// 获取本机出口 IP
func getOutboundIP(target string) string {
	conn, err := net.Dial("udp", target+":80")
	if err != nil {
		return "127.0.0.1" // fallback
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

// 生成自签名证书用于 QUIC
func generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"quic-migration-demo"},
	}
}
