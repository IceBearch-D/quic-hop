package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
)

// --- 核心：自定义的支持迁移的 PacketConn ---

type MigratingPacketConn struct {
	mu         sync.RWMutex
	curConn    net.PacketConn
	localAddr  net.Addr
	udpBufSize int
}

const (
	TimePortDefaultOffset = -1 // time service runs on serverPort-1
	// 默认UDP缓冲区大小（可通过flag覆盖），较大的缓冲区能在高RTT/弱网下减少拥塞与丢包影响
	DefaultUDPBufferSize = 64 * 1024 * 1024 // 64MB
	// 默认QUIC接收窗口，增大流和连接的初始接收窗口以提升弱网吞吐
	DefaultStreamRecvWindow     = 64 * 1024 * 1024  // 64MB
	DefaultConnectionRecvWindow = 256 * 1024 * 1024 // 256MB
)

// 创建一个新的迁移连接管理器
func NewMigratingPacketConn(udpBufSize int) (*MigratingPacketConn, error) {
	// 初始绑定一个端口
	c, err := net.ListenPacket("udp", "0.0.0.0:0")
	if err != nil {
		return nil, err
	}
	// 提升底层UDP缓冲区，避免高RTT导致拥塞限制
	if uc, ok := c.(*net.UDPConn); ok {
		// 尝试设置到较大的值（默认64MB）。不同平台可能会向下调整。
		if udpBufSize <= 0 {
			udpBufSize = DefaultUDPBufferSize
		}
		_ = uc.SetReadBuffer(udpBufSize)
		_ = uc.SetWriteBuffer(udpBufSize)
	}
	return &MigratingPacketConn{
		curConn:   c,
		localAddr: c.LocalAddr(),
		udpBufSize: func() int {
			if udpBufSize > 0 {
				return udpBufSize
			}
			return DefaultUDPBufferSize
		}(),
	}, nil
}

func getAddress(duration time.Duration) string {
	timestamp := time.Now().UnixMilli()
	timeSlot := timestamp / duration.Milliseconds()
	ip := (timeSlot % 252) + 2
	port := (timeSlot % 20000) + 20000
	return fmt.Sprintf("192.168.2.%d:%d", ip, port)
}

// 核心功能：切换到底层的新端口
func (m *MigratingPacketConn) RotatePort(duration time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldConn := m.curConn
	// 1. 绑定新端口
	address := getAddress(duration)
	newConn, err := net.ListenPacket("udp", address) // 0.0.0.0表示
	if err != nil {
		return err
	}
	if uc, ok := newConn.(*net.UDPConn); ok {
		_ = uc.SetReadBuffer(m.udpBufSize)
		_ = uc.SetWriteBuffer(m.udpBufSize)
	}

	fmt.Printf("[Client] Port Hopping! %s -> %s\n", oldConn.LocalAddr(), newConn.LocalAddr())

	// 2. 替换引用
	m.curConn = newConn
	m.localAddr = newConn.LocalAddr()

	// 3. 关闭旧连接 (这会导致正在阻塞的 ReadFrom 报错，需要处理)
	// 注意：这里为了演示简单，直接关闭。在极高并发下可能需要更平滑的处理，
	// 但 QUIC 本身有重传机制，丢失几个包问题不大。
	oldConn.Close()

	return nil
}

// --- 实现 net.PacketConn 接口 ---

func (m *MigratingPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	for {
		m.mu.RLock()
		conn := m.curConn
		m.mu.RUnlock()

		n, addr, err = conn.ReadFrom(p)

		// 如果错误是因为 Socket 被关闭了（我们在 RotatePort 里关的），则重试
		if err != nil && errors.Is(err, net.ErrClosed) {
			continue // 重新获取锁，读取新的 conn
		}
		return
	}
}

func (m *MigratingPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// fmt.Printf("[Client] Sending %d bytes to %s from %s\n", len(p), addr.String(), m.localAddr.String())
	return m.curConn.WriteTo(p, addr)
}

func (m *MigratingPacketConn) Close() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.curConn.Close()
}

func (m *MigratingPacketConn) LocalAddr() net.Addr {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.localAddr
}

func (m *MigratingPacketConn) SetDeadline(t time.Time) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.curConn.SetDeadline(t)
}

func (m *MigratingPacketConn) SetReadDeadline(t time.Time) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.curConn.SetReadDeadline(t)
}

func (m *MigratingPacketConn) SetWriteDeadline(t time.Time) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.curConn.SetWriteDeadline(t)
}

// --- 主程序 ---

func main() {
	serverAddr := flag.String("server", "192.168.43.2:4242", "server udp address")
	filePath := flag.String("file", "sample.bin", "path of file to send")
	autogenMB := flag.Int("autogenMB", 500, "if file missing, auto-generate with this size (MB)")
	hopInterval := flag.Duration("hop", 500*time.Millisecond, "port hop interval")
	// 新增可调节参数：UDP缓冲与QUIC窗口
	udpBuf := flag.Int("udpBufMB", DefaultUDPBufferSize/(1024*1024), "UDP read/write buffer size (MB)")
	streamWin := flag.Int("streamWinMB", DefaultStreamRecvWindow/(1024*1024), "QUIC initial stream receive window (MB)")
	connWin := flag.Int("connWinMB", DefaultConnectionRecvWindow/(1024*1024), "QUIC initial connection receive window (MB)")
	appBuf := flag.Int("appBufKB", 1024, "application write buffer size (KB)")
	flag.Parse()

	// 0. 校时：测量偏移与 RTT
	serverHost, serverPortStr, err := net.SplitHostPort(*serverAddr)
	if err != nil {
		log.Fatalf("invalid server addr: %v", err)
	}
	serverPort, err := strconv.Atoi(serverPortStr)
	if err != nil {
		log.Fatalf("invalid server port: %v", err)
	}
	timePort := serverPort + TimePortDefaultOffset
	offset, rtt, err := syncTime(serverHost, timePort)
	if err != nil {
		log.Printf("[Client] time sync failed, fallback to local clock: %v", err)
		offset, rtt = 0, 0
	} else {
		log.Printf("[Client] time sync success: offset=%v rtt=%v", offset, rtt)
	}

	// 1. 准备要发送的文件
	fileSize, err := ensureFile(*filePath, *autogenMB)
	if err != nil {
		log.Fatalf("prepare file failed: %v", err)
	}
	file, err := os.Open(*filePath)
	if err != nil {
		log.Fatalf("open file failed: %v", err)
	}
	defer file.Close()

	// 2. 初始化自定义连接并与服务器建立 QUIC 连接
	// 创建自定义PacketConn并应用UDP缓冲区设置
	mConn, err := NewMigratingPacketConn((*udpBuf) * 1024 * 1024)
	if err != nil {
		log.Fatal(err)
	}

	tlsConf := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"quic-migration-demo"}}
	remoteAddr, _ := net.ResolveUDPAddr("udp", *serverAddr)

	ctx := context.Background()
	// 配置较大的QUIC接收窗口，以适配高RTT弱网环境
	// 启用 qlog 输出到 ./qlog 目录
	_ = os.MkdirAll("qlog", 0o755)
	_ = os.Setenv("QLOGDIR", "qlog")

	quicConf := &quic.Config{
		KeepAlivePeriod:                15 * time.Second,
		InitialStreamReceiveWindow:     uint64(*streamWin) * 1024 * 1024,
		InitialConnectionReceiveWindow: uint64(*connWin) * 1024 * 1024,
		MaxIncomingStreams:             1024, // 适度提升流并发上限
		MaxIncomingUniStreams:          256,
		Tracer:                         qlog.DefaultConnectionTracer,
	}

	conn, err := quic.Dial(ctx, mConn, remoteAddr, tlsConf, quicConf)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.CloseWithError(0, "client done")

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// 3. 启动端口跳变并记录下一次跳变时间
	var nextHopDeadline int64
	atomic.StoreInt64(&nextHopDeadline, time.Now().Add(*hopInterval).UnixNano())
	go func() {
		for {
			time.Sleep(*hopInterval)
			if err := mConn.RotatePort(*hopInterval); err != nil {
				log.Printf("Rotate port failed: %v", err)
			}
			atomic.StoreInt64(&nextHopDeadline, time.Now().Add(*hopInterval).UnixNano())
		}
	}()

	// 4. 发送文件（带文件名和长度的简单头部），并在跳变前留出 2*RTT 静默窗口
	start := time.Now()
	if err := sendFile(stream, file, filepath.Base(*filePath), fileSize, rtt, hopInterval, &nextHopDeadline, *appBuf); err != nil {
		log.Fatalf("send file failed: %v", err)
	}
	if err := stream.Close(); err != nil {
		log.Printf("stream close: %v", err)
	}
	duration := time.Since(start)
	mb := float64(fileSize) / (1024 * 1024)
	log.Printf("Finished sending %.2f MB in %s (%.2f MB/s)", mb, duration, mb/duration.Seconds())
}

func sendFile(stream *quic.Stream, file *os.File, name string, size int64, rtt time.Duration, hopInterval *time.Duration, nextHopDeadline *int64, appBufKB int) error {
	// 协议：uint16 文件名长度 | 文件名 | uint64 文件字节数 | 文件内容
	nameBytes := []byte(name)
	if len(nameBytes) > 65535 {
		return fmt.Errorf("file name too long: %d", len(nameBytes))
	}
	if err := binary.Write(stream, binary.BigEndian, uint16(len(nameBytes))); err != nil {
		return err
	}
	if _, err := stream.Write(nameBytes); err != nil {
		return err
	}
	if err := binary.Write(stream, binary.BigEndian, uint64(size)); err != nil {
		return err
	}

	// 提升应用层缓冲区以降低系统调用开销，缓解高RTT下的吞吐下降
	if appBufKB <= 0 {
		appBufKB = 1024
	}
	buf := make([]byte, appBufKB*1024)
	var written int64
	for {
		if rtt > 0 {
			dl := atomic.LoadInt64(nextHopDeadline)
			if dl > 0 {
				timeLeft := time.Until(time.Unix(0, dl))
				if timeLeft > 0 && timeLeft < 2*rtt {
					time.Sleep(timeLeft)
				}
			}
		}

		n, readErr := file.Read(buf)
		if n > 0 {
			if wn, err := stream.Write(buf[:n]); err != nil {
				return err
			} else if wn != n {
				return fmt.Errorf("short write chunk: %d/%d", wn, n)
			}
			written += int64(n)
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return readErr
		}
	}

	if written != size {
		return fmt.Errorf("short write: wrote %d, expect %d", written, size)
	}
	return nil
}

func ensureFile(path string, sizeMB int) (int64, error) {
	info, err := os.Stat(path)
	if err == nil {
		return info.Size(), nil
	}
	if !os.IsNotExist(err) {
		return 0, err
	}

	if sizeMB <= 0 {
		sizeMB = 10
	}
	size := int64(sizeMB) * 1024 * 1024
	log.Printf("auto-generating file %s (%d MB)", path, sizeMB)

	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	buf := make([]byte, 64*1024)
	var written int64
	for written < size {
		chunk := size - written
		if chunk > int64(len(buf)) {
			chunk = int64(len(buf))
		}
		if _, err := f.Write(buf[:chunk]); err != nil {
			return 0, err
		}
		written += chunk
	}
	return size, nil
}

// syncTime 向服务器时间端口发送探测，返回偏移量（服务器-本地）与 RTT。
func syncTime(serverHost string, timePort int) (time.Duration, time.Duration, error) {
	addr := fmt.Sprintf("%s:%d", serverHost, timePort)
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
