package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	
	"github.com/gin-gonic/gin"
)

// 镶嵌数据服务发布入口

const TILE_SIZE = 256
// 定义全局的mosiac对象
var mosaic Mosaic

// 服务入口，参数为配置文件路径
func service(jsonPath string) {
	fmt.Println("Starting service...")
	// 关闭 Gin 的调试模式
	gin.SetMode(gin.ReleaseMode)

	mosaic.Open(jsonPath)
	defer mosaic.Close()

	r := gin.Default()
	r.GET("/:z/:x/:y", func(c *gin.Context) {
		fmt.Println("URL Path:", c.Request.URL.Path)
		
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
		
		imageData,_ := mosaic.ReadTile(z, x, y)
		if imageData != nil {
			c.Data(http.StatusOK, "image/png", imageData)
		} else {
			c.String(http.StatusInternalServerError, "Error generating image")
		}
	})

	r.Run(":8080")	
}
