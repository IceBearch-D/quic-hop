package magic

import (
	"context"
	"errors"
	"fmt"
	mrand "math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

var decodeBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, FixedCIDLen)
	},
}

var fakeAddr = &net.UDPAddr{IP: net.ParseIP(FakeAddrIP), Port: FakeAddrPort}

// packet 是内部传递的数据包结构
type packet struct {
	data []byte
	addr net.Addr
	err  error
}

type MagicConn struct {
	isServer    bool
	latency     time.Duration
	latencyMu   sync.RWMutex
	currentConn net.PacketConn // 仅用于 WriteTo (当前的活动发送端口)
	nextConn    net.PacketConn // 预绑定的下一跳监听端口
	connMu      sync.RWMutex
	localConn   net.PacketConn // 客户端固定使用的本地端口

	// 核心：多端口接收队列
	recvQueue chan packet
	ctx       context.Context
	cancel    context.CancelFunc

	// 客户端专用：本地已知的 CID 缓存
	clientCache *CIDCache

	// 发送计数 (Key: CID string, Val: Count)
	txCounts sync.Map // map[string]int，无需额外锁

	// 连接级 CID 混淆种子发生器
	obfRNG *mrand.Rand
}

func NewServerConn(parentCtx context.Context) (*MagicConn, error) {
	// 使用 context 管理生命周期，防止泄露
	ctx, cancel := context.WithCancel(parentCtx)
	c := &MagicConn{
		isServer:  true,
		obfRNG:    NewCIDSeedRNG(),
		recvQueue: make(chan packet, 2048),
		ctx:       ctx,
		cancel:    cancel,
	}

	now := time.Now()
	addr := GetAddressByTime(now)
	l, err := net.ListenPacket("udp", addr.String())
	if err != nil {
		cancel()
		return nil, err
	}
	c.currentConn = l
	fmt.Printf("[Server] Initial Listen: %s\n", addr)

	// 启动接收泵
	go c.packetPump(l)

	// 预绑定下一跳监听端口（下一时隙）
	if ServerPrebindNextPortEnabled {
		nextAddr := GetAddressByTime(now.Add(GetNextHopDelay() + 1*time.Millisecond))
		if nextL, err := net.ListenPacket("udp", nextAddr.String()); err == nil {
			c.nextConn = nextL
			go c.packetPump(nextL)
			// fmt.Printf("[Server] Prebound Next: %s\n", nextAddr)
		} else {
			// 预绑定失败不致命，等待下一轮 hop 再尝试
		}
	}

	// 启动跳变循环
	go c.serverHoppingLoop(ctx)

	return c, nil
}

func NewClientConn(latency time.Duration) (*MagicConn, error) {
	l, err := net.ListenPacket("udp", LoopbackAddr2(0))
	if err != nil {
		return nil, err
	}

	// --- 修复点：客户端也需要初始化 context ---
	ctx, cancel := context.WithCancel(context.Background())

	c := &MagicConn{
		isServer:    false,
		localConn:   l,
		latency:     latency,
		clientCache: NewCIDCache(),
		obfRNG:      NewCIDSeedRNG(),
		recvQueue:   make(chan packet, 2048),
		ctx:         ctx,
		cancel:      cancel,
	}

	go c.packetPump(l)
	return c, nil
}

// packetPump 持续从给定的 conn 读取数据并放入统一队列
func (c *MagicConn) packetPump(conn net.PacketConn) {
	for {
		buf := make([]byte, 4096)
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			// 如果连接关闭，优雅退出
			if strings.Contains(err.Error(), "closed") {
				return
			}
			// 避免在 context 已经关闭的情况下发送错误
			select {
			case <-c.ctx.Done():
				return
			case c.recvQueue <- packet{err: err}:
			}
			return
		}

		select {
		case c.recvQueue <- packet{data: buf[:n], addr: addr}:
		case <-c.ctx.Done():
			return
		default:
			// 缓冲区满则丢弃，防止阻塞影响主逻辑
		}
	}
}

// --- Hopping ---
func (c *MagicConn) serverHoppingLoop(ctx context.Context) {
	for {
		wait := GetNextHopDelay()
		select {
		case <-time.After(wait):
			c.hop()
		case <-ctx.Done():
			return
		}
	}
}

func (c *MagicConn) hop() {
	// 在时隙边界：提升已预绑定的 nextConn 为当前监听，并尝试预绑定下一跳
	var prevConn net.PacketConn
	var newCurrent net.PacketConn

	c.connMu.Lock()
	prevConn = c.currentConn
	if c.nextConn != nil {
		newCurrent = c.nextConn
		c.currentConn = c.nextConn
		c.nextConn = nil // 即将重新预绑定
	} else {
		newCurrent = c.currentConn // 无预绑定则继续沿用
	}
	c.connMu.Unlock()

	// 预绑定下一时隙监听端口（下一次跳变使用）
	if ServerPrebindNextPortEnabled {
		nextAddr := GetAddressByTime(time.Now().Add(GetNextHopDelay() + 1*time.Millisecond))
		if nextL, err := net.ListenPacket("udp", nextAddr.String()); err == nil {
			c.connMu.Lock()
			c.nextConn = nextL
			c.connMu.Unlock()
			go c.packetPump(nextL)
			// fmt.Printf("[Server] Prebound Next: %s\n", nextAddr)
		}
	}

	// 在短暂的重叠窗口内保留 prevConn，形成三端口同时监听（prev/current/next）
	if prevConn != nil && prevConn != newCurrent {
		go func(l net.PacketConn) {
			time.Sleep(time.Duration(ServerOldPortCloseDelayMs) * time.Millisecond)
			l.Close()
		}(prevConn)
	}
}

// --- ReadFrom ---
func (c *MagicConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	var pkt packet
	select {
	case pkt = <-c.recvQueue:
		if pkt.err != nil {
			return 0, nil, pkt.err
		}
	case <-c.ctx.Done():
		return 0, nil, errors.New("connection closed")
	}

	data := pkt.data
	addr = pkt.addr

	// 客户端伪造固定地址（复用静态对象）
	if !c.isServer {
		addr = fakeAddr
	}

	// 1. Long Header
	if len(data) > 0 && data[0]&0x80 > 0 {
		n = copy(p, data)
		return n, addr, nil
	}

	// 2. Short Header
	cidOffset := 1
	if len(data) < cidOffset+FixedCIDLen {
		n = copy(p, data)
		return n, addr, nil
	}

	wireCID := data[cidOffset : cidOffset+FixedCIDLen]

	var cache *CIDCache

	if c.isServer {
		if addr == nil { // 防御性检查
			return 0, nil, errors.New("nil source address")
		}
		cache = GlobalServerSessions.GetCache(addr.String())
	} else {
		cache = c.clientCache
	}

	// A. 先尝试明文匹配（命中则无需修改，直接返回原包）
	if _, ok := cache.FindByReal(wireCID); ok {
		n = copy(p, data)
		return n, addr, nil
	}

	// B. 解密尝试通过前 56 bits 匹配真实 CID
	decoded := decodeBufPool.Get().([]byte)
	defer decodeBufPool.Put(decoded)
	copy(decoded, wireCID)
	Deobfuscate(decoded)
	if real, ok := cache.FindByDecoded56(decoded); ok {
		// 需要回填真实 CID，复制并修改包
		n = copy(p, data)
		copy(p[cidOffset:], real)
		return n, addr, nil
	}

	// C. 未知则认为 wireCID 为新的真实 CID（首 10 包场景）
	cache.AddReal(wireCID)
	n = copy(p, data)
	return n, addr, nil
}

func (c *MagicConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	// 发送端记录真实 CID（用于接收端首包建立映射）

	if len(p) > 0 && p[0]&0x80 == 0 { // Short Header
		cidOffset := 1
		if len(p) >= cidOffset+FixedCIDLen {
			// 使用固定长度数组作为 key，避免 string 转换
			var cidKey [FixedCIDLen]byte
			copy(cidKey[:], p[cidOffset:cidOffset+FixedCIDLen])

			// 获取当前计数
			val, _ := c.txCounts.Load(cidKey)
			count := 0
			if val != nil {
				count = val.(int)
			}
			count++
			c.txCounts.Store(cidKey, count)

			// 前 10 包不加密；之后每包单独加密（in-place，直接修改 p）
			if CIDObfuscationEnabled && count > 10 {
				seed := uint16(c.obfRNG.Uint32())
				Obfuscate(p[cidOffset:cidOffset+FixedCIDLen], seed)
			}
		}
	}

	if c.isServer {
		c.connMu.RLock()
		l := c.currentConn
		c.connMu.RUnlock()
		if l == nil {
			return 0, net.ErrClosed
		}
		return l.WriteTo(p, addr)
	} else {
		return c.clientWrite(p)
	}
}

func (c *MagicConn) clientWrite(p []byte) (n int, err error) {
	now := time.Now()
	c.latencyMu.RLock()
	lat := c.latency
	c.latencyMu.RUnlock()
	arrival := now.Add(lat)
	target1 := GetAddressByTime(arrival)
	n, err = c.localConn.WriteTo(p, target1)
	if err != nil {
		return
	}

	ms := arrival.UnixMilli() % SlotDuration.Milliseconds()
	if SlotDuration.Milliseconds()-ms < ClientDoubleSendEdgeThresholdMs {
		target2 := GetAddressByTime(arrival.Add(SlotDuration)) // 下一个时隙
		if target2.String() != target1.String() {
			c.localConn.WriteTo(p, target2)
		}
	}
	return
}

func (c *MagicConn) LocalAddr() net.Addr {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	if c.isServer && c.currentConn != nil {
		return c.currentConn.LocalAddr()
	}
	if c.localConn != nil {
		return c.localConn.LocalAddr()
	}
	return &net.UDPAddr{IP: net.IPv4zero, Port: 0}
}

func (c *MagicConn) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()

	var err error
	if c.isServer && c.currentConn != nil {
		err = c.currentConn.Close()
	}
	if c.isServer && c.nextConn != nil {
		_ = c.nextConn.Close()
	}
	if c.localConn != nil {
		err = c.localConn.Close()
	}
	return err
}

func (c *MagicConn) SetDeadline(t time.Time) error      { return nil }
func (c *MagicConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *MagicConn) SetWriteDeadline(t time.Time) error { return nil }

func BytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// --- Periodic Calibration ---
func (c *MagicConn) StartPeriodicCalibration(serverIP string, interval time.Duration) {
	if c.isServer {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				newLat := SyncWithServer(serverIP)
				c.latencyMu.Lock()
				c.latency = newLat
				c.latencyMu.Unlock()
			case <-c.ctx.Done():
				return
			}
		}
	}()
}
