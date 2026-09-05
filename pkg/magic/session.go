package magic

import "sync"

type cidEntry struct {
	real         []byte
	prefix56Hash uint64 // 前 56bits 的 hash，用于快速比较
}

// CIDCache 管理一组已知的 Real CID
type CIDCache struct {
	sync.RWMutex
	entries       map[string]*cidEntry // key: realCID string
	prefix56Index map[uint64]*cidEntry // key: 前 56bits hash
}

func NewCIDCache() *CIDCache {
	return &CIDCache{
		entries:       make(map[string]*cidEntry),
		prefix56Index: make(map[uint64]*cidEntry),
	}
}

// AddReal 确保记录真实 CID
func (c *CIDCache) AddReal(real []byte) {
	c.Lock()
	defer c.Unlock()
	key := string(real)
	if e, ok := c.entries[key]; ok {
		e.real = cloneBytes(real)
		return
	}
	var prefix56Hash uint64
	if len(real) >= PayloadBytes {
		prefix56Hash = hashPrefix56(real[:PayloadBytes])
	}
	entry := &cidEntry{real: cloneBytes(real), prefix56Hash: prefix56Hash}
	c.entries[key] = entry
	if prefix56Hash != 0 {
		c.prefix56Index[prefix56Hash] = entry
	}
}

// FindByReal 精确匹配真实 CID
func (c *CIDCache) FindByReal(real []byte) (found []byte, ok bool) {
	c.RLock()
	defer c.RUnlock()
	if e, ok := c.entries[string(real)]; ok {
		return e.real, true
	}
	return nil, false
}

// GetReals 返回所有真实 CID
func (c *CIDCache) GetReals() [][]byte {
	c.RLock()
	defer c.RUnlock()
	res := make([][]byte, 0, len(c.entries))
	for _, e := range c.entries {
		res = append(res, cloneBytes(e.real))
	}
	return res
}

// FindByDecoded56 通过解码后的前 56 bits 匹配真实 CID (O(1) hash 查找)
func (c *CIDCache) FindByDecoded56(decoded []byte) (real []byte, ok bool) {
	if len(decoded) < PayloadBytes {
		return nil, false
	}
	hash := hashPrefix56(decoded[:PayloadBytes])
	c.RLock()
	defer c.RUnlock()
	if e, ok := c.prefix56Index[hash]; ok {
		return e.real, true
	}
	return nil, false
}

// hashPrefix56 快速 hash 前 7 字节（避免 string 转换）
func hashPrefix56(b []byte) uint64 {
	// 简单快速的 hash：直接读取前 7 字节作为 uint64
	if len(b) < 7 {
		return 0
	}
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	cp := make([]byte, len(src))
	copy(cp, src)
	return cp
}

// ServerSessionMap 服务端专用，管理多个客户端
type ServerSessionMap struct {
	sync.RWMutex
	m map[string]*CIDCache
}

var GlobalServerSessions = &ServerSessionMap{m: make(map[string]*CIDCache)}

func (s *ServerSessionMap) GetCache(addrStr string) *CIDCache {
	s.Lock()
	defer s.Unlock()
	if cache, ok := s.m[addrStr]; ok {
		return cache
	}
	cache := NewCIDCache()
	s.m[addrStr] = cache
	return cache
}
