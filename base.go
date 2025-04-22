package main

import (
	"math"
	// "fmt"
	// "path/filepath"
)

// 提供一些公共的定义和方法

// 定义地理范围
type BBox struct {
	XMin float64 `json:"xmin"`
	XMax float64 `json:"xmax"`
	YMin float64 `json:"ymin"`
	YMax float64 `json:"ymax"`
}

// 根据瓦片级别、行列号计算瓦片的地理范围
func CalcBBox(z int, x int, y int) (bbox BBox) {
	xDis := 360.0 / (math.Pow(2, float64(z)))
	yDis := 180.0 / (math.Pow(2, float64(z)))
	bbox.XMin = float64(x)*xDis - 180.0
	bbox.XMax = bbox.XMin + xDis

	bbox.YMax = 90 - float64(y)*yDis
	bbox.YMin = bbox.YMax - yDis	
	return
}

// 根据地理范围和z值，计算出x和y的最大最小值
func CalcTileRange(bbox BBox, z int) (xMin, xMax, yMin, yMax int) {
	xDis := 360.0 / (math.Pow(2, float64(z)))
	yDis := 180.0 / (math.Pow(2, float64(z)))
	xMin = int((bbox.XMin + 180.0) / xDis)
	xMax = int((bbox.XMax + 180.0) / xDis)
	yMax = int((90.0 - bbox.YMin) / yDis)
	yMin = int((90.0 - bbox.YMax) / yDis)
	return
}

// 计算两个地理范围的交集，在第二个box上的像素范围；width和height为第二个box的像素大小
// 例如：bbox1为要绘制的地理范围，bbox2为tiff文件的地理范围，width和height为tiff文件的像素大小；
// 		返回值为要绘制的地理范围在tiff文件上的像素范围
// 注意：地理方向是Y朝上，像素方向是Y朝下
func CalcPixelRange(bbox, bbox2 BBox, width, height int) (xMinPixel, yMinPixel, xMaxPixel, yMaxPixel int) {
	// 先计算地理范围和tiff文件范围交叉的大小
	intersection := GetIntersection(bbox, bbox2)
	xScale := float64(width) / (bbox2.XMax - bbox2.XMin)
	yScale := float64(height) / (bbox2.YMax - bbox2.YMin)
	// 计算交叉范围的X方向
	xMinPixel = int((intersection.XMin - bbox2.XMin) * xScale)
	xMaxPixel = int((intersection.XMax - bbox2.XMin) * xScale)
	// 计算交叉范围的Y方向
	yMinPixel = int((bbox2.YMax - intersection.YMax) * yScale)
	yMaxPixel = int((bbox2.YMax - intersection.YMin) * yScale)
	return
}

// 得到交叉部分
func GetIntersection(bbox1, bbox2 BBox) (bbox BBox) {
	bbox.XMin = math.Max(bbox1.XMin, bbox2.XMin)
	bbox.XMax = math.Min(bbox1.XMax, bbox2.XMax)
	bbox.YMin = math.Max(bbox1.YMin, bbox2.YMin)
	bbox.YMax = math.Min(bbox1.YMax, bbox2.YMax)
	return
}

