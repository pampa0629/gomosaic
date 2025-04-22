// gdal dataset的内存池，避免重复打开同一个tiff文件造成的时间消耗
package main

import (
	// "os"
	"fmt"
	"sync"
	"github.com/lukeroth/gdal"
)

type GdalPoolItem struct {
	pool map[gdal.Dataset] bool // true表示空闲，false表示正在使用 
	mutex sync.Mutex
}

// 
func (item *GdalPoolItem) LeaseExist(name string) (bool, gdal.Dataset) {
	item.mutex.Lock()
	defer item.mutex.Unlock()
	for dt, free := range item.pool {
		if free {
			item.pool[dt] = false
			return true,dt
		}
	}
	var dt gdal.Dataset
	return false,dt
}

// 租借一个dataset
func (item *GdalPoolItem) Lease(name string) (gdal.Dataset,error) {
	isExist,dt := item.LeaseExist(name)
	if isExist {
		return dt,nil
	} else {
		dt, err := gdal.Open(name, gdal.ReadOnly)
		item.mutex.Lock()
		item.pool[dt] = false
		item.mutex.Unlock()
		return dt,err
	}
}

// 归还一个dataset
func (item *GdalPoolItem) Return(dt gdal.Dataset) {
	item.mutex.Lock()
	defer item.mutex.Unlock()
	item.pool[dt] = true	
	// 看一下map的数量，如果太多，就释放一个
	if len(item.pool) > 10 {
		fmt.Println("GdalPoolItem.Return: too many dataset, close one")
		for dt, free := range item.pool {
			if free {
				dt.Close()
				delete(item.pool, dt)
				break
			}
		}
	}
}

// 全局变量，gdal dataset的内存池
var g_gdalPool GdalPool

// 定义gdal dataset的内存池
type GdalPool struct {
	items map[string]*GdalPoolItem
	mutex sync.Mutex
}

// 初始化gdal dataset的内存池
func (p *GdalPool) Init() {
	p.items = make(map[string]*GdalPoolItem)
}

// 打开一个dataset
func (p *GdalPool) OpenDataset(name string) *GdalPoolItem {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// 查找name是否已经在池中
	item, ok := p.items[name]
	// 如果在，直接用item.Lease()租借一个dataset
	if ok {
		return item
	} else {
		// 如果不在，创建一个新的item
		item = &GdalPoolItem{pool: make(map[gdal.Dataset] bool)}
		p.items[name] = item
		return item
	}
}

// 关闭一个dataset
func (p *GdalPool) CloseDataset(name string ,dt gdal.Dataset) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	// 知道名字，先找到item，再close
	item, ok := p.items[name]
	if ok {
		item.Return(dt)
	}	
}