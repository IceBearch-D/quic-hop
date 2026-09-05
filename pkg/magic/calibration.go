package magic

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"syscall"
	"time"
)

func StartCalibrationServer() { // 启动延迟校准服务器
	lc := net.ListenConfig{Control: func(network, address string, c syscall.RawConn) error {
		var ctrlErr error
		if err := c.Control(func(fd uintptr) {
			// Enable address reuse to avoid bind failures when restarting quickly
			ctrlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		}); err != nil {
			return err
		}
		return ctrlErr
	}}

	pc, err := lc.ListenPacket(context.Background(), "udp", (&net.UDPAddr{IP: net.ParseIP(SyncAddr), Port: 9999}).String())
	if err != nil {
		log.Printf("Failed to start calibration server: %v", err)
		return
	}
	defer pc.Close()

	udpConn, ok := pc.(*net.UDPConn)
	if !ok {
		log.Printf("Calibration server: unexpected packet conn type")
		return
	}

	buf := make([]byte, 1024)
	for {
		n, a, err := udpConn.ReadFromUDP(buf) // a 是客户端地址
		if err != nil {
			log.Printf("ReadFromUDP error: %v", err)
			continue
		}

		// 收到数据包的瞬时服务器时钟
		nowNs := time.Now().UnixNano()

		var clientTsNs int64 = -1
		if n >= 8 {
			clientTsNs = int64(binary.BigEndian.Uint64(buf[:8]))
		}

		// 判定数据包中的时间戳与本地时间是否一致：一致则回复0，不一致则回复收到时的服务器时钟
		var replyNs uint64
		if clientTsNs == nowNs {
			replyNs = 0
		} else {
			replyNs = uint64(nowNs)
		}

		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, replyNs)
		if _, err := udpConn.WriteToUDP(out, a); err != nil {
			log.Printf("WriteToUDP error: %v", err)
		}
	}
}

// SyncWithServer 与服务器进行时间同步，返回两端机器时差与 RTT/2 的和（用于到达时间预测）
func SyncWithServer(ip string) time.Duration {
	raddr, _ := net.ResolveUDPAddr("udp", ip+":9999")
	c, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return 0
	}
	defer c.Close()
	// 发送并接收一次，返回服务器给出的时间戳（0 表示对齐）、RTT 以及发送/接收时间
	sendAndRecv := func(predictedServerNs int64) (serverReplyNs int64, rtt time.Duration, tSend time.Time, tRecv time.Time, ok bool) {
		payload := make([]byte, 8)
		// 如果 predictedServerNs < 0，则发送本机当前时间；否则发送预测的服务器时间戳
		var toSendNs int64
		if predictedServerNs < 0 {
			toSendNs = time.Now().UnixNano()
		} else {
			toSendNs = predictedServerNs
		}
		binary.BigEndian.PutUint64(payload, uint64(toSendNs))

		tSend = time.Now()
		if _, err := c.Write(payload); err != nil {
			return 0, 0, tSend, tSend, false
		}
		c.SetReadDeadline(time.Now().Add(time.Second))

		if _, err := c.Read(payload); err != nil {
			return 0, 0, tSend, time.Now(), false
		}
		tRecv = time.Now()
		serverReplyNs = int64(binary.BigEndian.Uint64(payload))
		rtt = tRecv.Sub(tSend)
		return serverReplyNs, rtt, tSend, tRecv, true
	}
	const maxAttempts = 3
	var predictedServerNs int64 = -1 // 第一次发送本机时间
	var lastDelta time.Duration      // 上一次测得的机器时差
	var lastRtt time.Duration        // 上一次测得的 RTT
	var sumDelta time.Duration       // 用于均值：三次机器时差求和
	var sumRtt time.Duration         // 用于均值：三次 RTT 求和
	var count int                    // 已成功测量（未对齐）的次数

	for i := 0; i < maxAttempts; i++ {
		srvNs, rtt, tSend, _, ok := sendAndRecv(predictedServerNs)
		if !ok {
			// 超时或错误：如果已有一次成功测量，则返回最近估计；否则返回保守值
			if count > 0 {
				return lastDelta + lastRtt/2
			}
			return 10 * time.Millisecond
		}

		// 如果服务器返回 0，表示预测时间与服务器本地时钟一致，结束校验
		if srvNs == 0 {
			if i == 0 {
				// 第一次即对齐：按要求直接返回 0（可能因为 机器时差 == RTT/2）
				return 0
			}
			// 其它轮次对齐：返回上一次测得的 “机器时差 + RTT/2”
			return lastDelta + lastRtt/2
		}

		// 计算两端时差：delta = server_time_at_recv - (client_send_time + RTT/2)
		deltaNs := srvNs - (tSend.UnixNano() + rtt.Nanoseconds()/2)
		delta := time.Duration(deltaNs)
		lastDelta = delta
		lastRtt = rtt
		sumDelta += delta
		sumRtt += rtt
		count++

		// 下一次发送预测：本机当前时间 + 机器时差 + RTT/2
		predictedServerNs = time.Now().UnixNano() + delta.Nanoseconds() + rtt.Nanoseconds()/2
	}

	// 三次都没有对上：取均值
	if count > 0 {
		avgDelta := sumDelta / time.Duration(count)
		avgRtt := sumRtt / time.Duration(count)
		// 等价于三次 “机器时差与 RTT/2 的和” 的平均
		return avgDelta + avgRtt/2
	}
	// 不太可能到达这里，但兜底返回保守值
	return 10 * time.Millisecond
}
