package cpumon

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Result 监控数据结构
type Result struct {
	Timestamp time.Time
	CpuUsage  float64
	MemUsage  float64
	StealPerc float64
}

// Monitor 启动 CPU/内存监控并将数据记录到日志文件。
//
// 行为：
// - 每 interval 采样一次，将一行文本写入日志文件
// - 不向标准输出打印任何内容
// - 直到 ctx 结束才关闭文件并返回
//
// 参数：
// - logPath 为空时，自动在工作目录生成 cpu_YYYYMMDD_HHMMSS.log
// - 返回实际使用的日志文件路径（便于调用方在需要时获知）
func Monitor(ctx context.Context, logPath string, interval time.Duration) (string, error) {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}

	if logPath == "" {
		logPath = fmt.Sprintf("cpu_%s.log", time.Now().Format("20060102_150405"))
	}

	if dir := filepath.Dir(logPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", err
		}
	}

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 64*1024)
	defer w.Flush()

	cpuFile, err := os.Open("/proc/stat")
	if err != nil {
		return "", err
	}
	defer cpuFile.Close()

	memFile, err := os.Open("/proc/meminfo")
	if err != nil {
		return "", err
	}
	defer memFile.Close()

	cpuBuf := make([]byte, 2048)
	memBuf := make([]byte, 2048)

	prevTotal, prevWork, prevSteal := readCPURaw(cpuFile, cpuBuf)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			currTotal, currWork, currSteal := readCPURaw(cpuFile, cpuBuf)

			totalDelta := float64(currTotal - prevTotal)
			cpuPercent := 0.0
			stealPercent := 0.0
			if totalDelta > 0 {
				cpuPercent = (float64(currWork-prevWork) / totalDelta) * 100
				stealPercent = (float64(currSteal-prevSteal) / totalDelta) * 100
			}
			prevTotal, prevWork, prevSteal = currTotal, currWork, currSteal

			memPercent := readMemRaw(memFile, memBuf)

			// 写入一行：时间, CPU%, Steal%, Mem%
			// 格式示例：2026-01-09T12:34:56.789, 23.1, 0.0, 45.2
			// 保持轻量文本格式，便于后续处理
			line := fmt.Sprintf("%s, %.1f, %.1f, %.1f\n",
				t.Format(time.RFC3339Nano), cpuPercent, stealPercent, memPercent)
			if _, err := w.WriteString(line); err != nil {
				return logPath, err
			}

			// 周期性 flush，避免缓冲过大或丢数据
			_ = w.Flush()

		case <-ctx.Done():
			// 结束前做一次最终 flush
			_ = w.Flush()
			return logPath, nil
		}
	}
}

// readCPURaw 读取 /proc/stat 第一行，返回 total、work、steal
func readCPURaw(f *os.File, buf []byte) (uint64, uint64, uint64) {
	f.Seek(0, 0)
	n, _ := f.Read(buf)
	fields := bytes.Fields(buf[:n])

	parse := func(i int) uint64 {
		if i >= len(fields) {
			return 0
		}
		// 跳过字段名 "cpu"，其后依次为 user nice system idle iowait irq softirq steal ...
		val, _ := strconv.ParseUint(string(fields[i]), 10, 64)
		return val
	}

	user := parse(1)
	nice := parse(2)
	system := parse(3)
	idle := parse(4)
	iowait := parse(5)
	irq := parse(6)
	softirq := parse(7)
	steal := parse(8)

	total := user + nice + system + idle + iowait + irq + softirq + steal
	work := total - idle - iowait
	return total, work, steal
}

// readMemRaw 读取 /proc/meminfo 的 MemTotal 与 MemAvailable 计算使用率
func readMemRaw(f *os.File, buf []byte) float64 {
	f.Seek(0, 0)
	n, _ := f.Read(buf)
	content := buf[:n]

	var total, avail float64
	lines := bytes.Split(content, []byte("\n"))
	for _, line := range lines {
		if bytes.HasPrefix(line, []byte("MemTotal:")) {
			fields := bytes.Fields(line)
			total, _ = strconv.ParseFloat(string(fields[1]), 64)
		} else if bytes.HasPrefix(line, []byte("MemAvailable:")) {
			fields := bytes.Fields(line)
			avail, _ = strconv.ParseFloat(string(fields[1]), 64)
		}
		if total > 0 && avail > 0 {
			break
		}
	}
	if total == 0 {
		return 0
	}
	return ((total - avail) / total) * 100
}

// --- Process-scoped monitoring (PID level) ---

// ProcessMonitor 记录某个进程的 CPU/内存采样
type ProcessMonitor struct {
	pid       int
	interval  time.Duration
	mu        sync.RWMutex
	samples   []Result
	lastTotal uint64
	lastProc  uint64
}

// StartProcessMonitor 启动针对指定 PID 的采样，不写日志，仅保存在内存以便计算区间均值。
func StartProcessMonitor(ctx context.Context, pid int, interval time.Duration) (*ProcessMonitor, error) {
	if pid <= 0 {
		return nil, fmt.Errorf("invalid pid: %d", pid)
	}
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}

	pm := &ProcessMonitor{pid: pid, interval: interval}

	// 初始化基线
	total, proc, _, err := readProcAndTotal(pm.pid)
	if err != nil {
		return nil, err
	}
	pm.lastTotal = total
	pm.lastProc = proc

	go pm.loop(ctx)
	return pm, nil
}

// AverageBetween 计算 [start, end] 区间内的平均 CPU% 与内存(MB)。
func (pm *ProcessMonitor) AverageBetween(start, end time.Time) (cpuAvg float64, memAvg float64) {
	if end.Before(start) {
		return 0, 0
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	count := 0
	for _, s := range pm.samples {
		if (s.Timestamp.Equal(start) || s.Timestamp.After(start)) && (s.Timestamp.Before(end) || s.Timestamp.Equal(end)) {
			cpuAvg += s.CpuUsage
			memAvg += s.MemUsage
			count++
		}
	}
	if count == 0 {
		return 0, 0
	}
	return cpuAvg / float64(count), memAvg / float64(count)
}

func (pm *ProcessMonitor) loop(ctx context.Context) {
	ticker := time.NewTicker(pm.interval)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			total, proc, rssBytes, err := readProcAndTotal(pm.pid)
			if err != nil {
				return
			}

			deltaTotal := float64(total - pm.lastTotal)
			deltaProc := float64(proc - pm.lastProc)
			pm.lastTotal = total
			pm.lastProc = proc

			cpuPercent := 0.0
			if deltaTotal > 0 {
				// /proc/stat 的总jiffies已包含所有CPU核的累加，这里直接计算比例即可，避免再乘以核数导致>100%
				cpuPercent = (deltaProc / deltaTotal) * 100
			}

			memMB := float64(rssBytes) / (1024 * 1024)

			pm.mu.Lock()
			pm.samples = append(pm.samples, Result{Timestamp: t, CpuUsage: cpuPercent, MemUsage: memMB})
			pm.mu.Unlock()

		case <-ctx.Done():
			return
		}
	}
}

func readProcAndTotal(pid int) (totalJiffies uint64, procJiffies uint64, rssBytes uint64, err error) {
	totalJiffies, err = readTotalJiffies()
	if err != nil {
		return
	}

	procJiffies, rssBytes, err = readProcStat(pid)
	return
}

func readTotalJiffies() (uint64, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, err
	}
	fields := bytes.Fields(data)
	if len(fields) < 8 {
		return 0, fmt.Errorf("unexpected /proc/stat format")
	}

	parse := func(i int) uint64 {
		if i >= len(fields) {
			return 0
		}
		val, _ := strconv.ParseUint(string(fields[i]), 10, 64)
		return val
	}

	user := parse(1)
	nice := parse(2)
	system := parse(3)
	idle := parse(4)
	iowait := parse(5)
	irq := parse(6)
	softirq := parse(7)
	steal := parse(8)

	total := user + nice + system + idle + iowait + irq + softirq + steal
	return total, nil
}

func readProcStat(pid int) (procJiffies uint64, rssBytes uint64, err error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}

	// 处理 comm 中可能包含空格的情况：从最后一个 ')' 之后开始切割
	endComm := bytes.LastIndexByte(data, ')')
	if endComm == -1 || endComm+2 >= len(data) {
		return 0, 0, fmt.Errorf("invalid stat format")
	}

	parts := bytes.Fields(data[endComm+2:])
	// utime(14) 和 stime(15) 位于去掉前 2 项后的 index 11,12
	if len(parts) < 22 {
		return 0, 0, fmt.Errorf("unexpected stat fields count")
	}

	utime, _ := strconv.ParseUint(string(parts[11]), 10, 64)
	stime, _ := strconv.ParseUint(string(parts[12]), 10, 64)
	rssPages, _ := strconv.ParseUint(string(parts[21]), 10, 64)

	procJiffies = utime + stime
	rssBytes = rssPages * uint64(os.Getpagesize())
	return
}
