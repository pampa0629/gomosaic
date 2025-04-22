package main

import (
	// "os"
	// "fmt"
	"sync"
)

// 缓存的key
type Key struct {
	 Z, X, Y int
}

type SafeMap struct {
	mu sync.Mutex
	data map[Key][]byte
}

// 缓存的map
var g_cacheMap SafeMap

// 初始化缓存
func initCache() {
	g_cacheMap = SafeMap{data: make(map[Key][]byte)}
}

// 设置缓存
func setTile(z,x,y int, data []byte) {
	g_cacheMap.mu.Lock()
	defer g_cacheMap.mu.Unlock()
	g_cacheMap.data[Key{z,x,y}] = data
}

// 获取缓存
func getTile(z,x,y int) []byte {
	g_cacheMap.mu.Lock()
	defer g_cacheMap.mu.Unlock()
	return g_cacheMap.data[Key{z,x,y}]
}