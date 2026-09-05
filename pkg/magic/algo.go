package magic

import (
	"encoding/binary"
	"hash/fnv"
	mrand "math/rand"
	"net"
	"sync"
	"time"
)

// const SlotDuration = 5 * time.Second
// 必须与 Session 中 GenerateMask 调用保持一致
// var SharedSecret = []byte("EXTREME_POLYMORPHIC_SECRET_2025")

func GetAddressByTime(t time.Time) *net.UDPAddr {
	timestamp := t.UnixMilli()
	timeSlot := timestamp / SlotDuration.Milliseconds()

	h := fnv.New64a()
	h.Write(SharedSecret)
	binary.Write(h, binary.BigEndian, timeSlot)
	sum := h.Sum64()
	ip := (sum % 253) + 2
	port := (sum % 20000) + 20000
	addrStr := LoopbackAddr(int(ip), int(port))
	addr, _ := net.ResolveUDPAddr("udp", addrStr)
	return addr
}

func GetNextHopDelay() time.Duration {
	now := time.Now()
	ms := SlotDuration.Milliseconds()
	elapsed := now.UnixMilli() % ms
	return time.Duration(ms-elapsed) * time.Millisecond
}

type CIDMask [16]byte
type SimpleRNG struct{ state uint64 }

var maskCache sync.Map
var sharedSecretHash = func() int64 {
	h := fnv.New64a()
	h.Write(SharedSecret)
	return int64(h.Sum64())
}()

var indicesTable [1 << SeedBits][EncodedBits]byte
var indicesOnce sync.Once

func NewSimpleRNG(seed int64) *SimpleRNG { return &SimpleRNG{state: uint64(seed)} }
func (r *SimpleRNG) Next() uint64 {
	r.state = r.state*6364136223846793005 + 1442695040888963407
	return r.state
}
func (r *SimpleRNG) Intn(n int) int { return int(r.Next() % uint64(n)) }

func precomputeIndices() { // 预计算所有可能种子的索引表，该表仅运算一次
	indicesOnce.Do(func() {
		for s := 0; s < (1 << SeedBits); s++ {
			rng := mrand.New(mrand.NewSource(int64(uint16(s)) ^ sharedSecretHash))
			rng.Uint64() // consume once to mirror noise generation

			var arr [EncodedBits]byte
			idx := make([]int, EncodedBits)
			for i := 0; i < EncodedBits; i++ {
				idx[i] = i
			}
			for i := EncodedBits - 1; i > 0; i-- {
				j := rng.Intn(i + 1)
				idx[i], idx[j] = idx[j], idx[i]
			}
			for i := 0; i < EncodedBits; i++ {
				arr[i] = byte(idx[i])
			}
			indicesTable[s] = arr
		}
	})
}

func GenerateMask(addr net.Addr) CIDMask {
	key := addr.String()
	if cached, ok := maskCache.Load(key); ok {
		return cached.(CIDMask)
	}

	h := fnv.New64a()
	h.Write(SharedSecret)
	h.Write([]byte(key))
	seed := int64(h.Sum64())

	rng := NewSimpleRNG(seed)
	indices := make([]int, 128)
	for i := 0; i < 128; i++ {
		indices[i] = i
	}
	for i := 127; i > 0; i-- {
		j := rng.Intn(i + 1)
		indices[i], indices[j] = indices[j], indices[i]
	}

	var mask CIDMask
	for i := 0; i < 64; i++ {
		idx := indices[i]
		mask[idx/8] |= (1 << (idx % 8))
	}

	maskCache.Store(key, mask)
	return mask
}

// NewCIDSeedRNG returns a connection-scoped RNG for CID obfuscation seeds.
func NewCIDSeedRNG() *mrand.Rand {
	seed := time.Now().UnixNano() ^ sharedSecretHash
	return mrand.New(mrand.NewSource(seed))
}

func shuffledIndices(rngSeed uint16) []byte {
	precomputeIndices()
	buf := indicesTable[rngSeed]
	return buf[:]
}

// Obfuscate 使用新算法（自带 16bit 种子 + 56bit 载荷编码）。
func Obfuscate(data []byte, rngSeed uint16) {
	if len(data) != OutputBytes {
		panic("CID length must be exactly 16 bytes")
	}

	// 0. 拷贝载荷（只取前 56bit）
	var payload [PayloadBytes]byte
	copy(payload[:], data[:PayloadBytes])

	// 1. 生成噪声 r（56bit）- 使用固定算法避免 PRNG 对象创建
	state := uint64(rngSeed) ^ uint64(sharedSecretHash)
	state = state*6364136223846793005 + 1442695040888963407
	var noise [PayloadBytes]byte
	for i := 0; i < PayloadBytes; i++ {
		noise[i] = byte(state >> (8 * (i % 8)))
		if i%8 == 7 {
			state = state*6364136223846793005 + 1442695040888963407
		}
	}

	// 2. 获取位置映射 indices[0..111]
	indices := shuffledIndices(rngSeed)

	// 3. 差分编码到临时 output（14 字节）
	var output [EncodedBytes]byte
	for i := 0; i < PayloadBits; i++ {
		byteIdx := i >> 3
		bitOff := i & 7
		m := (payload[byteIdx] >> bitOff) & 1
		r := (noise[byteIdx] >> bitOff) & 1

		realPos := int(indices[i])
		noisePos := int(indices[i+PayloadBits])

		if r == 1 {
			output[noisePos>>3] |= (1 << (noisePos & 7))
		}
		if (r ^ m) == 1 {
			output[realPos>>3] |= (1 << (realPos & 7))
		}
	}

	// 4. 写回：前 16bit 存种子，后 112bit 存编码结果
	binary.BigEndian.PutUint16(data[:SeedBytes], rngSeed)
	copy(data[SeedBytes:], output[:])
}

// Matches 解码后比对前 56bit（与测试脚本一致）。
func Matches(wireCIDDecoded, realCID []byte) bool {
	if len(wireCIDDecoded) < PayloadBytes || len(realCID) < PayloadBytes {
		return false
	}
	return BytesEqual(wireCIDDecoded[:PayloadBytes], realCID[:PayloadBytes])
}

func Deobfuscate(data []byte) {
	if len(data) != OutputBytes {
		panic("Data length must be exactly 16 bytes")
	}

	rngSeed := binary.BigEndian.Uint16(data[:SeedBytes])
	indices := shuffledIndices(rngSeed)

	cipher := data[SeedBytes:]
	var recovered [PayloadBytes]byte

	for i := 0; i < PayloadBits; i++ {
		realPos := int(indices[i])
		noisePos := int(indices[i+PayloadBits])

		valReal := (cipher[realPos>>3] >> (realPos & 7)) & 1
		valNoise := (cipher[noisePos>>3] >> (noisePos & 7)) & 1

		m := valReal ^ valNoise
		if m == 1 {
			recovered[i>>3] |= (1 << (i & 7))
		}
	}

	// 写回 56bit，剩余 72bit 归零
	for i := range data {
		data[i] = 0
	}
	copy(data[:PayloadBytes], recovered[:])
}
