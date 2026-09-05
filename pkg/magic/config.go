package magic

import (
	"fmt"
	"time"
)

// SharedSecret 双方共享的安全密钥
var SharedSecret = []byte("EXTREME_POLYMORPHIC_SECRET_2025")

// SlotDuration 地址跳变的时间窗口（可通过运行参数覆盖）
var SlotDuration = 1000 * time.Millisecond

// ClientDoubleSendEdgeThresholdMs 跳变边缘双发阈值（毫秒）。设为 0 可关闭双发。
var ClientDoubleSendEdgeThresholdMs int64 = 50

// ServerOldPortCloseDelayMs 旧端口延迟关闭时长（毫秒）。设为 0 可立即关闭。
var ServerOldPortCloseDelayMs int64 = 50

// ServerPrebindNextPortEnabled 是否预开启未来监听端口。默认开启。
var ServerPrebindNextPortEnabled = true

// CIDObfuscationEnabled 是否启用 CID 加密传输。为 false 时始终使用真实 CID。
var CIDObfuscationEnabled = true

// CID 长度 (必须固定为 16 字节以支持我们的掩码算法)
const FixedCIDLen = 16

// LoopbackPrefix 是本地环回网段前三段，用于生成 127.0.0.x 形式地址
const LoopbackPrefix = "192.168.2"

// ListenAddr is the IP used by the calibration server; made mutable for runtime override in tools like sync.go
var ListenAddr = "192.168.1.3"

var SyncAddr = "192.168.1.2"

// 虚假地址
var FakeAddrIP = "192.0.2.1"
var FakeAddrPort = 54321

// TaskRounds 每个 slot 的任务轮数（每轮包含上传+下载）。
var TaskRounds = 10

// TransferFileSizeBytes 单次传输大小（字节），默认 50MB。
var TransferFileSizeBytes = 100 * 1024 * 1024

// LoopbackIP 拼出不带端口的环回地址
func LoopbackIP(lastOctet int) string {
	return fmt.Sprintf("%s.%d", LoopbackPrefix, lastOctet)
}

// LoopbackAddr 拼出带端口的环回地址
func LoopbackAddr(lastOctet int, port int) string {
	return fmt.Sprintf("%s.%d:%d", LoopbackPrefix, lastOctet, port)
}

func LoopbackAddr2(port int) string {
	return fmt.Sprintf("%s:%d", ListenAddr, port)
}

const SeedBits = 16
const SeedBytes = SeedBits / 8 // 2
const PayloadBits = 56
const PayloadBytes = PayloadBits / 8         // 7
const EncodedBits = PayloadBits * 2          // 112
const EncodedBytes = EncodedBits / 8         // 14
const OutputBytes = SeedBytes + EncodedBytes // 16 总输出
