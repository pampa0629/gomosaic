package main

import (
	"fmt"
	"time"
	"net/http"
	"strconv"
	// "io/ioutil"
	"io"
	"strings"
	
	"github.com/gin-gonic/gin"
)

// 镶嵌数据服务发布入口

const TILE_SIZE = 256
// 定义全局的mosiac对象
var mosaic Mosaic

// 服务入口，参数为配置文件路径
// service(*input, *oss, *ak, *sk, *endpoint, *bucket)
func service(input string, oss bool, ak, sk, endpoint, bucket string) {

	fmt.Println("Starting service...")
	// 关闭 Gin 的调试模式
	gin.SetMode(gin.ReleaseMode)
	gin.DefaultWriter = io.Discard

	mosaic.Open(input, oss, ak, sk, endpoint, bucket)
	// 初始化缓存
	initCache()
	// 初始化gdal dataset的内存池
	g_gdalPool.Init()
	
	r := gin.Default()
	r.GET("/:z/:x/:y", func(c *gin.Context) {
		z, err := strconv.Atoi(c.Param("z"))
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid zoom level")
			return
		}

		x, err := strconv.Atoi(c.Param("x"))
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid X coordinate")
			return
		}

		yStr := strings.TrimSuffix(c.Param("y"), ".png")
		y, err := strconv.Atoi(yStr)
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid Y coordinate")
			return
		}

		// 计时器
		start := time.Now()
		// 读取缓存
		if imageData := getTile(z, x, y); imageData != nil {
			c.Data(http.StatusOK, "image/png", imageData)
		} else if imageData,_ := mosaic.ReadTile(z, x, y); imageData != nil{
			// 设置缓存
			setTile(z, x, y, imageData)
			c.Data(http.StatusOK, "image/png", imageData)
		} else {
			c.String(http.StatusInternalServerError, "Error generating image")
		}
		elapsed := time.Since(start)
		ms := elapsed / time.Millisecond
		if ms > 1000 {
			fmt.Println(z,x,y," Time use: ", elapsed)
		}
	})
	
	r.Run(":8080")	
}
