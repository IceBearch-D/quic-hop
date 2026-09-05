package main

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"net_1218/pkg/cpumon"
	"net_1218/pkg/magic"

	"github.com/quic-go/quic-go"
	qlogpkg "github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

const resultsCSV = "results.csv"
var tasksPerSlot = magic.TaskRounds * 2

const (
	taskUpload   byte = 1
	taskDownload byte = 2
)

var enableQlog bool
var taskSeq uint64

type LossStats struct {
	sent uint64
	lost uint64
}

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

func main() {
	slotDurationMs := flag.Int("slot", 1000, "slot duration in milliseconds")
	autoSlots := flag.Bool("auto", false, "run sequential slots 100..1000ms (step 100) and exit after each single session")
	qlogFlag := flag.Bool("qlog", false, "enable qlog tracing")
	disableOldClose := flag.Bool("old", false, "disable server old-port delay close")
	disablePrebind := flag.Bool("pre", false, "disable server prebind next port")
	disableCIDObfuscation := flag.Bool("cid", false, "disable CID obfuscation")
	flag.Parse()
	enableQlog = *qlogFlag
	if *disableOldClose {
		magic.ServerOldPortCloseDelayMs = 0
	}
	if *disablePrebind {
		magic.ServerPrebindNextPortEnabled = false
	}
	if *disableCIDObfuscation {
		magic.CIDObfuscationEnabled = false
	}

	if *autoSlots {
		go magic.StartCalibrationServer()
		runAutoSlots()
		return
	}

	magic.SlotDuration = time.Duration(*slotDurationMs) * time.Millisecond
	fmt.Printf("[Server] SlotDuration: %v (%d ms)\n", magic.SlotDuration, magic.SlotDuration.Milliseconds())

	// 启动延迟校准服务器
	go magic.StartCalibrationServer()

	// 创建可取消的上下文以管理服务器生命周期
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if enableQlog {
		_ = os.MkdirAll("qlogs/server", 0o755)
		_ = os.Setenv("QLOGDIR", "qlogs/server")
	}

	conn, err := magic.NewServerConn(ctx)
	if err != nil {
		log.Fatal(err)
	}

	lossStats := &LossStats{}
	tracer := makeLossTracer(lossStats, enableQlog)

	tr := &quic.Transport{
		Conn:                  conn,
		ConnectionIDGenerator: &magic.FixedBytesGenerator{Len: magic.FixedCIDLen},
	}

	listener, err := tr.Listen(magic.GenerateTLSConfig(), &quic.Config{
		// 允许连接迁移 (默认就是允许的，这里显式写出来为了明确)
		DisablePathMTUDiscovery: false,
		KeepAlivePeriod:         15 * time.Second, // 提升保活时长
		Tracer:                  tracer,
		// 增大接收窗口以提升高带宽网络的传输性能
		InitialStreamReceiveWindow:     100 * 1024 * 1024,  // 10 MB 初始流接收窗口
		MaxStreamReceiveWindow:         500 * 1024 * 1024,  // 50 MB 最大流接收窗口
		InitialConnectionReceiveWindow: 150 * 1024 * 1024,  // 15 MB 初始连接接收窗口
		MaxConnectionReceiveWindow:     1000 * 1024 * 1024, // 100 MB 最大连接接收窗口
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("[Server] Ready. (Hopping + Polymorphic CID)")

	for {
		c, err := listener.Accept(context.Background())
		if err != nil {
			continue
		}
		go handleConn(c, lossStats)
	}
}

func runAutoSlots() {
	if enableQlog {
		_ = os.MkdirAll("qlogs/server", 0o755)
		_ = os.Setenv("QLOGDIR", "qlogs/server")
	}

	slots := []int{1500, 1400, 1300, 1200, 1100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 100, 99999999}
	// slots := []int{100,800, 2000, 1900, 1800,1700, 1600, 1500, 1400, 1300, 1200, 1100, 9999999}
	// slots := []int{2000, 1900, 1800,1700, 1600, 1500, 1400, 1300, 1200, 1100, 1000,900,800,700,600,500,400,300,200,100,100,9999999}
	for _, ms := range slots {
		fmt.Printf("\n[Server][Auto] === Slot %d ms ===\n", ms)
		for {
			if err := serveOnce(ms); err != nil {
				fmt.Printf("[Server][Auto] Slot %d failed: %v. Retrying same slot...\n", ms, err)
				continue
			}
			fmt.Printf("[Server][Auto] Slot %d completed.\n", ms)
			break
		}
	}
	fmt.Println("[Server][Auto] All slots completed.")
}

func serveOnce(slotMs int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	magic.SlotDuration = time.Duration(slotMs) * time.Millisecond
	fmt.Printf("[Server] SlotDuration: %v (%d ms)\n", magic.SlotDuration, magic.SlotDuration.Milliseconds())

	conn, err := magic.NewServerConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	lossStats := &LossStats{}
	tracer := makeLossTracer(lossStats, enableQlog)

	tr := &quic.Transport{
		Conn:                  conn,
		ConnectionIDGenerator: &magic.FixedBytesGenerator{Len: magic.FixedCIDLen},
	}

	listener, err := tr.Listen(magic.GenerateTLSConfig(), &quic.Config{
		DisablePathMTUDiscovery:        false,
		KeepAlivePeriod:                15 * time.Second,
		Tracer:                         tracer,
		InitialStreamReceiveWindow:     100 * 1024 * 1024,
		MaxStreamReceiveWindow:         500 * 1024 * 1024,
		InitialConnectionReceiveWindow: 150 * 1024 * 1024,
		MaxConnectionReceiveWindow:     1000 * 1024 * 1024,
	})
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("[Server] Ready. Waiting for %d client tasks...\n", tasksPerSlot)

	for i := 1; i <= tasksPerSlot; i++ {
		c, err := listener.Accept(ctx)
		if err != nil {
			return err
		}
		err = handleConn(c, lossStats)
		if err != nil {
			return err
		}
		waitForPeerClose(c, 2*time.Second)
		fmt.Printf("[Server] Completed task %d/%d\n", i, tasksPerSlot)
	}

	return nil
}

func handleConn(c *quic.Conn, lossStats *LossStats) error {
	fmt.Printf("[Server] New Session: %s\n", c.RemoteAddr())

	pm, stopMon := startProcessMonitor()
	if stopMon != nil {
		defer stopMon()
	}

	stream, err := c.AcceptStream(context.Background())
	if err != nil {
		return fmt.Errorf("accept stream: %w", err)
	}
	defer stream.Close()

	result, err := handleSingleTask(stream, lossStats, pm)
	if err != nil {
		return err
	}

	if err := appendResultsCSV(int(magic.SlotDuration.Milliseconds()), result); err != nil {
		log.Printf("[Server] Failed to write CSV: %v", err)
	}
	waitForPeerClose(c, 2*time.Second)
	return nil
}

type taskResult struct {
	taskID        uint64
	taskType      string
	speedMBps     float64
	cpuAvg        float64
	memAvgMB      float64
	clientLossPct float64
	serverLossPct float64
}

func handleSingleTask(stream *quic.Stream, lossStats *LossStats, pm *cpumon.ProcessMonitor) (taskResult, error) {
	var result taskResult
	result.taskID = atomic.AddUint64(&taskSeq, 1)

	var taskType [1]byte
	if _, err := io.ReadFull(stream, taskType[:]); err != nil {
		return result, fmt.Errorf("read task type: %w", err)
	}

	switch taskType[0] {
	case taskUpload:
		result.taskType = "upload"
		speed, start, end, clientLoss, serverLoss, err := runUploadTask(stream, lossStats)
		if err != nil {
			return result, err
		}
		cpuAvg, memAvg := getAverages(pm, start, end)
		result.speedMBps = speed
		result.cpuAvg = cpuAvg
		result.memAvgMB = memAvg
		result.clientLossPct = clientLoss
		result.serverLossPct = serverLoss
	case taskDownload:
		result.taskType = "download"
		speed, start, end, clientLoss, serverLoss, err := runDownloadTask(stream, lossStats)
		if err != nil {
			return result, err
		}
		cpuAvg, memAvg := getAverages(pm, start, end)
		result.speedMBps = speed
		result.cpuAvg = cpuAvg
		result.memAvgMB = memAvg
		result.clientLossPct = clientLoss
		result.serverLossPct = serverLoss
	default:
		return result, fmt.Errorf("unknown task type: %d", taskType[0])
	}

	return result, nil
}

func runUploadTask(stream *quic.Stream, lossStats *LossStats) (speedMBps float64, start time.Time, end time.Time, clientLossPct float64, serverLossPct float64, err error) {
	sizeBuf := make([]byte, 8)
	ackBuf := []byte{0x01}

	start = time.Now()
	if _, err = io.ReadFull(stream, sizeBuf); err != nil {
		return 0, time.Time{}, time.Time{}, 0, 0, fmt.Errorf("receive upload size: %w", err)
	}
	expectedSize := binary.BigEndian.Uint64(sizeBuf)

	if _, err = stream.Write(ackBuf); err != nil {
		return 0, time.Time{}, time.Time{}, 0, 0, fmt.Errorf("send ready ACK: %w", err)
	}

	preSent, preLost := snapshotLoss(lossStats)
	var received uint64
	buf := make([]byte, 64*1024)
	for received < expectedSize {
		n, rerr := stream.Read(buf)
		if rerr != nil && rerr != io.EOF {
			return 0, time.Time{}, time.Time{}, 0, 0, fmt.Errorf("receive upload data: %w", rerr)
		}
		if n == 0 {
			break
		}
		received += uint64(n)
	}
	end = time.Now()

	if _, err = stream.Write(ackBuf); err != nil {
		return 0, time.Time{}, time.Time{}, 0, 0, fmt.Errorf("send completion ACK: %w", err)
	}

	duration := end.Sub(start)
	if duration > 0 {
		speedMBps = float64(received) / duration.Seconds() / (1024 * 1024)
	}

	binary.BigEndian.PutUint64(sizeBuf, uint64(speedMBps*100))
	if _, err = stream.Write(sizeBuf); err != nil {
		return speedMBps, start, end, 0, 0, fmt.Errorf("send upload speed: %w", err)
	}

	if _, err = io.ReadFull(stream, sizeBuf); err != nil {
		return speedMBps, start, end, 0, 0, fmt.Errorf("receive client loss: %w", err)
	}
	clientLossBp := binary.BigEndian.Uint64(sizeBuf)
	clientLossPct = float64(clientLossBp) / 10000.0

	if _, err = stream.Write(ackBuf); err != nil {
		return speedMBps, start, end, clientLossPct, 0, fmt.Errorf("send final ACK: %w", err)
	}

	postSent, postLost := snapshotLoss(lossStats)
	serverLossPct = lossPercentage(preSent, preLost, postSent, postLost)

	return speedMBps, start, end, clientLossPct, serverLossPct, nil
}

func runDownloadTask(stream *quic.Stream, lossStats *LossStats) (speedMBps float64, start time.Time, end time.Time, clientLossPct float64, serverLossPct float64, err error) {
	sizeBuf := make([]byte, 8)
	if _, err = io.ReadFull(stream, sizeBuf); err != nil {
		return 0, time.Time{}, time.Time{}, 0, 0, fmt.Errorf("receive download size: %w", err)
	}
	sendSize := binary.BigEndian.Uint64(sizeBuf)

	data := make([]byte, sendSize)
	for i := range data {
		data[i] = byte((i + 100) % 256)
	}

	binary.BigEndian.PutUint64(sizeBuf, uint64(sendSize))
	if _, err = stream.Write(sizeBuf); err != nil {
		return 0, time.Time{}, time.Time{}, 0, 0, fmt.Errorf("send download size: %w", err)
	}

	start = time.Now()
	preSent, preLost := snapshotLoss(lossStats)
	sent := 0
	chunkSize := 64 * 1024
	for sent < len(data) {
		endIdx := sent + chunkSize
		if endIdx > len(data) {
			endIdx = len(data)
		}
		n, werr := stream.Write(data[sent:endIdx])
		if werr != nil {
			return 0, time.Time{}, time.Time{}, 0, 0, fmt.Errorf("send download data: %w", werr)
		}
		sent += n
	}
	end = time.Now()

	duration := end.Sub(start)
	if duration > 0 {
		speedMBps = float64(sendSize) / duration.Seconds() / (1024 * 1024)
	}

	if _, err = io.ReadFull(stream, sizeBuf); err != nil {
		return speedMBps, start, end, 0, 0, fmt.Errorf("receive client speed: %w", err)
	}
	if _, err = io.ReadFull(stream, sizeBuf); err != nil {
		return speedMBps, start, end, 0, 0, fmt.Errorf("receive client loss: %w", err)
	}
	clientLossBp := binary.BigEndian.Uint64(sizeBuf)
	clientLossPct = float64(clientLossBp) / 10000.0

	if _, err = stream.Write([]byte{0x01}); err != nil {
		return speedMBps, start, end, clientLossPct, 0, fmt.Errorf("send final ACK: %w", err)
	}

	postSent, postLost := snapshotLoss(lossStats)
	serverLossPct = lossPercentage(preSent, preLost, postSent, postLost)

	return speedMBps, start, end, clientLossPct, serverLossPct, nil
}

func appendResultsCSV(slotMs int, result taskResult) error {
	fileExists := true
	if _, err := os.Stat(resultsCSV); err != nil {
		if os.IsNotExist(err) {
			fileExists = false
		} else {
			return err
		}
	}

	f, err := os.OpenFile(resultsCSV, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if !fileExists {
		header := []string{"SlotDuration", "TaskID", "TaskType", "SpeedMBps", "CPU", "MemMB", "ClientLossPct", "ServerLossPct"}
		if err := w.Write(header); err != nil {
			return err
		}
	}

	rec := []string{
		strconv.Itoa(slotMs),
		strconv.FormatUint(result.taskID, 10),
		result.taskType,
		fmt.Sprintf("%.2f", result.speedMBps),
		fmt.Sprintf("%.1f", result.cpuAvg),
		fmt.Sprintf("%.1f", result.memAvgMB),
		fmt.Sprintf("%.4f", result.clientLossPct),
		fmt.Sprintf("%.4f", result.serverLossPct),
	}
	if err := w.Write(rec); err != nil {
		return err
	}

	w.Flush()
	return w.Error()
}

func startProcessMonitor() (*cpumon.ProcessMonitor, context.CancelFunc) {
	if runtime.GOOS != "linux" {
		return nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	pm, err := cpumon.StartProcessMonitor(ctx, os.Getpid(), 100*time.Millisecond)
	if err != nil {
		cancel()
		log.Printf("[Server] Failed to start process monitor: %v", err)
		return nil, nil
	}
	return pm, cancel
}

func getAverages(pm *cpumon.ProcessMonitor, start, end time.Time) (cpuAvg float64, memAvg float64) {
	if pm == nil {
		return 0, 0
	}
	cpu, mem := pm.AverageBetween(start, end)
	return cpu, mem
}

func waitForPeerClose(c *quic.Conn, timeout time.Duration) {
	select {
	case <-c.Context().Done():
	case <-time.After(timeout):
	}
}
