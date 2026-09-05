package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"net_1218/pkg/magic"

	"github.com/quic-go/quic-go"
	qlogpkg "github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// const FileSize = 50 * 1024 * 1024 // 50MB

const (
	taskUpload   byte = 1
	taskDownload byte = 2
)
type LossStats struct {
	sent uint64
	lost uint64
}

// qlog-based Recorder: counts sent/lost 1-RTT packets, optionally forwards to default tracer
type lossTrace struct {
	stats *LossStats
	inner qlogwriter.Trace
}

type lossRecorder struct {
	stats *LossStats
	inner qlogwriter.Recorder
}

func makeLossTracer(stats *LossStats, enableQlog bool) func(ctx context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
	return func(ctx context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
		var inner qlogwriter.Trace
		if enableQlog {
			inner = qlogpkg.DefaultConnectionTracer(ctx, isClient, connID)
		}
		return &lossTrace{stats: stats, inner: inner}
	}
}

func (t *lossTrace) AddProducer() qlogwriter.Recorder {
	var innerProd qlogwriter.Recorder
	if t.inner != nil {
		innerProd = t.inner.AddProducer()
	}
	return &lossRecorder{stats: t.stats, inner: innerProd}
}

func (t *lossTrace) SupportsSchemas(schema string) bool {
	if t.inner != nil {
		return t.inner.SupportsSchemas(schema)
	}
	return true
}

func (r *lossRecorder) RecordEvent(ev qlogwriter.Event) {
	switch e := ev.(type) {
	case qlogpkg.PacketLost:
		if e.Header.PacketType == qlogpkg.PacketType1RTT {
			atomic.AddUint64(&r.stats.lost, 1)
		}
	case qlogpkg.PacketSent:
		if e.Header.PacketType == qlogpkg.PacketType1RTT {
			atomic.AddUint64(&r.stats.sent, 1)
		}
	}
	if r.inner != nil {
		r.inner.RecordEvent(ev)
	}
}

func (r *lossRecorder) Close() error {
	if r.inner != nil {
		return r.inner.Close()
	}
	return nil
}

func snapshotLoss(ls *LossStats) (sent uint64, lost uint64) {
	if ls == nil {
		return 0, 0
	}
	return atomic.LoadUint64(&ls.sent), atomic.LoadUint64(&ls.lost)
}

func lossPercentage(preSent, preLost, postSent, postLost uint64) float64 {
	deltaSent := postSent - preSent
	deltaLost := postLost - preLost
	if deltaSent == 0 {
		return 0
	}
	return float64(deltaLost) * 100 / float64(deltaSent)
}

const (
	TimePortDefaultOffset = -1 // time service runs on serverPort-1
	// 默认UDP缓冲区大小（可通过flag覆盖），较大的缓冲区能在高RTT/弱网下减少拥塞与丢包影响
	DefaultUDPBufferSize = 64 * 1024 * 1024 // 64MB
	// 默认QUIC接收窗口，增大流和连接的初始接收窗口以提升弱网吞吐
	DefaultStreamRecvWindow     = 64 * 1024 * 1024  // 64MB
	DefaultConnectionRecvWindow = 256 * 1024 * 1024 // 256MB
)

func main() {
	slotDurationMs := flag.Int("slot", 1000, "slot duration in milliseconds")
	autoSlots := flag.Bool("auto", false, "run sequential slots 100..10ms and exit after each single session")
	streamWin := flag.Int("streamWinMB", DefaultStreamRecvWindow/(1024*1024), "QUIC initial stream receive window (MB)")
	connWin := flag.Int("connWinMB", DefaultConnectionRecvWindow/(1024*1024), "QUIC initial connection receive window (MB)")
	qlogFlag := flag.Bool("qlog", false, "enable qlog tracing")
	disableDoubleSend := flag.Bool("dou", false, "disable client double-send")
	disableCIDObfuscation := flag.Bool("cid", false, "disable CID obfuscation")

	flag.Parse()
	enableQlog := *qlogFlag
	if *disableDoubleSend {
		magic.ClientDoubleSendEdgeThresholdMs = 0
	}
	if *disableCIDObfuscation {
		magic.CIDObfuscationEnabled = false
	}

	if *autoSlots {
		runAutoSlots(*streamWin, *connWin, enableQlog)
		return
	}

	if err := runClientOnce(*slotDurationMs, *streamWin, *connWin, enableQlog); err != nil {
		log.Fatal(err)
	}
}

func runAutoSlots(streamWinMB, connWinMB int, enableQlog bool) {
	slots := []int{1500, 1400, 1300, 1200, 1100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 100, 99999999}
	for _, ms := range slots {
		fmt.Printf("\n[Client][Auto] === Slot %d ms ===\n", ms)
		for {
			if err := runClientOnce(ms, streamWinMB, connWinMB, enableQlog); err != nil {
				fmt.Printf("[Client][Auto] Slot %d failed: %v. Retrying same slot after 10s...\n", ms, err)
				time.Sleep(10 * time.Second)
				continue
			}
			fmt.Printf("[Client][Auto] Slot %d completed.\n", ms)
			fmt.Println("[Client][Auto] Cooling down 3s for server init...")
			time.Sleep(3 * time.Second)
			break
		}
	}
	fmt.Println("[Client][Auto] All slots completed.")
}

func runClientOnce(slotDurationMs int, streamWinMB, connWinMB int, enableQlog bool) error {
	magic.SlotDuration = time.Duration(slotDurationMs) * time.Millisecond
	fmt.Printf("[Client] SlotDuration: %v (%d ms)\n", magic.SlotDuration, magic.SlotDuration.Milliseconds())

	if enableQlog {
		_ = os.MkdirAll("./qlogs/client", 0o755)
		_ = os.Setenv("QLOGDIR", "qlogs/client")
	}

	fmt.Println("[Client] Calibrating...")
	latency := magic.SyncWithServer(magic.SyncAddr) // 返回了单程延迟估计
	fmt.Printf("[Client] Latency: %v\n", latency)

	// 开始传输测试（10 轮，每轮上传+下载，均使用新连接）
	return performTransferTasks(latency, streamWinMB, connWinMB, enableQlog)
}

func performTransferTasks(latency time.Duration, streamWinMB, connWinMB int, enableQlog bool) error {
	rounds := magic.TaskRounds
	fmt.Printf("[Client] Starting transfer tasks (%d rounds, upload+download)...\n", rounds)
	testData := make([]byte, magic.TransferFileSizeBytes)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var uploadResults []float64
	var downloadResults []float64

	for i := 1; i <= rounds; i++ {
		fmt.Printf("[Client] Round %d: upload task (50MB)\n", i)
		latency = magic.SyncWithServer(magic.SyncAddr)
		var upload float64
		var lossPct float64
		for {
			qConn, magicConn, lossStats, err := dialOnce(latency, streamWinMB, connWinMB, enableQlog)
			if err != nil {
				fmt.Printf("[Client] Upload dial failed, retrying: %v\n", err)
				time.Sleep(2 * time.Second)
				continue
			}
			upload, lossPct, err = runUploadTask(qConn, testData, lossStats)
			qConn.CloseWithError(quic.ApplicationErrorCode(0), "task done")
			magicConn.Close()
			if err != nil {
				fmt.Printf("[Client] Upload task failed, retrying: %v\n", err)
				time.Sleep(2 * time.Second)
				continue
			}
			break
		}
		uploadResults = append(uploadResults, upload)
		fmt.Printf("[Client] Round %d upload done: %.2f MB/s, client loss %.2f%%\n", i, upload, lossPct)

		fmt.Printf("[Client] Round %d: download task (50MB)\n", i)
		latency = magic.SyncWithServer(magic.SyncAddr)
		var download float64
		for {
			qConn, magicConn, lossStats, err := dialOnce(latency, streamWinMB, connWinMB, enableQlog)
			if err != nil {
				fmt.Printf("[Client] Download dial failed, retrying: %v\n", err)
				time.Sleep(2 * time.Second)
				continue
			}
			download, lossPct, err = runDownloadTask(qConn, magic.TransferFileSizeBytes, lossStats)
			qConn.CloseWithError(quic.ApplicationErrorCode(0), "task done")
			magicConn.Close()
			if err != nil {
				fmt.Printf("[Client] Download task failed, retrying: %v\n", err)
				time.Sleep(2 * time.Second)
				continue
			}
			break
		}
		downloadResults = append(downloadResults, download)
		fmt.Printf("[Client] Round %d download done: %.2f MB/s, client loss %.2f%%\n", i, download, lossPct)
	}

	fmt.Println("[Client] All tasks completed. Summary:")
	for i := 0; i < rounds; i++ {
		fmt.Printf("  Round %d: upload %.2f MB/s, download %.2f MB/s\n", i+1, uploadResults[i], downloadResults[i])
	}
	return nil
}

func dialOnce(latency time.Duration, streamWinMB, connWinMB int, enableQlog bool) (*quic.Conn, *magic.MagicConn, *LossStats, error) {
	conn, err := magic.NewClientConn(latency)
	if err != nil {
		return nil, nil, nil, err
	}

	lossStats := &LossStats{}
	tr := &quic.Transport{
		Conn:                  conn,
		ConnectionIDGenerator: &magic.FixedBytesGenerator{Len: magic.FixedCIDLen},
	}

	ctx := context.Background()
	dummy := &net.UDPAddr{IP: net.ParseIP(magic.FakeAddrIP), Port: magic.FakeAddrPort}
	tracer := makeLossTracer(lossStats, enableQlog)
	quicConf := &quic.Config{
		KeepAlivePeriod:                15 * time.Second,
		InitialStreamReceiveWindow:     uint64(streamWinMB) * 1024 * 1024,
		InitialConnectionReceiveWindow: uint64(connWinMB) * 1024 * 1024,
		MaxIncomingStreams:             1024,
		MaxIncomingUniStreams:          256,
		Tracer:                         tracer,
	}

	qConn, err := tr.Dial(ctx, dummy, magic.GenerateTLSConfig(), quicConf)
	if err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	return qConn, conn, lossStats, nil
}

func runUploadTask(conn *quic.Conn, data []byte, lossStats *LossStats) (uploadMBps float64, lossPct float64, err error) {
	ctx := context.Background()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("open upload stream: %w", err)
	}
	defer stream.Close()

	if _, err = stream.Write([]byte{taskUpload}); err != nil {
		return 0, 0, fmt.Errorf("send task type: %w", err)
	}

	sizeBuf := make([]byte, 8)
	startUpload := time.Now()
	binary.BigEndian.PutUint64(sizeBuf, uint64(len(data)))
	if _, err = stream.Write(sizeBuf); err != nil {
		return 0, 0, fmt.Errorf("send upload size: %w", err)
	}

	ackBuf := make([]byte, 1)
	if _, err = io.ReadFull(stream, ackBuf); err != nil {
		return 0, 0, fmt.Errorf("receive ready ACK: %w", err)
	}

	preSent, preLost := snapshotLoss(lossStats)
	sent := 0
	chunkSize := 64 * 1024
	for sent < len(data) {
		end := sent + chunkSize
		if end > len(data) {
			end = len(data)
		}
		n, werr := stream.Write(data[sent:end])
		if werr != nil {
			return 0, 0, fmt.Errorf("send upload data: %w", werr)
		}
		sent += n
	}

	if _, err = io.ReadFull(stream, ackBuf); err != nil {
		return 0, 0, fmt.Errorf("receive completion ACK: %w", err)
	}

	postSent, postLost := snapshotLoss(lossStats)
	lossPct = lossPercentage(preSent, preLost, postSent, postLost)

	uploadDuration := time.Since(startUpload)
	uploadMBps = float64(len(data)) / uploadDuration.Seconds() / (1024 * 1024)

	if _, err = io.ReadFull(stream, sizeBuf); err != nil {
		return uploadMBps, lossPct, fmt.Errorf("receive server speed: %w", err)
	}

	lossBp := uint64(lossPct * 10000)
	binary.BigEndian.PutUint64(sizeBuf, lossBp)
	if _, err = stream.Write(sizeBuf); err != nil {
		return uploadMBps, lossPct, fmt.Errorf("send loss pct: %w", err)
	}
	if _, err = io.ReadFull(stream, ackBuf); err != nil {
		return uploadMBps, lossPct, fmt.Errorf("receive final ACK: %w", err)
	}

	return uploadMBps, lossPct, nil
}

func runDownloadTask(conn *quic.Conn, size int, lossStats *LossStats) (downloadMBps float64, lossPct float64, err error) {
	ctx := context.Background()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("open download stream: %w", err)
	}
	defer stream.Close()

	if _, err = stream.Write([]byte{taskDownload}); err != nil {
		return 0, 0, fmt.Errorf("send task type: %w", err)
	}

	sizeBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBuf, uint64(size))
	if _, err = stream.Write(sizeBuf); err != nil {
		return 0, 0, fmt.Errorf("send download size: %w", err)
	}

	if _, err = io.ReadFull(stream, sizeBuf); err != nil {
		return 0, 0, fmt.Errorf("receive file size: %w", err)
	}
	expectedSize := binary.BigEndian.Uint64(sizeBuf)

	preSent, preLost := snapshotLoss(lossStats)
	startDownload := time.Now()
	received := uint64(0)
	buf := make([]byte, 64*1024)
	for received < expectedSize {
		n, rerr := stream.Read(buf)
		if rerr != nil && rerr != io.EOF {
			return 0, 0, fmt.Errorf("receive data: %w", rerr)
		}
		if n == 0 {
			break
		}
		received += uint64(n)
	}

	postSent, postLost := snapshotLoss(lossStats)
	lossPct = lossPercentage(preSent, preLost, postSent, postLost)

	downloadDuration := time.Since(startDownload)
	downloadMBps = float64(received) / downloadDuration.Seconds() / (1024 * 1024)

	clientSpeed := uint64(downloadMBps * 100)
	binary.BigEndian.PutUint64(sizeBuf, clientSpeed)
	if _, err = stream.Write(sizeBuf); err != nil {
		return downloadMBps, lossPct, fmt.Errorf("send download speed: %w", err)
	}

	lossBp := uint64(lossPct * 10000)
	binary.BigEndian.PutUint64(sizeBuf, lossBp)
	if _, err = stream.Write(sizeBuf); err != nil {
		return downloadMBps, lossPct, fmt.Errorf("send loss pct: %w", err)
	}
	ackBuf := make([]byte, 1)
	if _, err = io.ReadFull(stream, ackBuf); err != nil {
		return downloadMBps, lossPct, fmt.Errorf("receive final ACK: %w", err)
	}

	return downloadMBps, lossPct, nil
}
