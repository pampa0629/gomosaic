package main

import (
	"math"
	// "fmt"
	"path/filepath"
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
	// fmt.Println("dis:", xDis, yDis)
	// fmt.Println("bbox:", bbox)
	return
}

// 根据要绘制的地理范围，和tiff文件本身的地理范围，计算要绘制的像素范围
func CalcPixelRange(bbox, tifBbox BBox, width, height int) (xMinPixel, yMinPixel, xMaxPixel, yMaxPixel int) {
	// 先计算地理范围和tiff文件范围交叉的大小
	intersection := GetIntersection(bbox, tifBbox)
	// 计算交叉范围的左上角像素坐标
	xMinPixel = int((intersection.XMin - tifBbox.XMin) / (tifBbox.XMax - tifBbox.XMin) * float64(width))
	yMinPixel = int((tifBbox.YMax - intersection.YMax) / (tifBbox.YMax - tifBbox.YMin) * float64(height))
	// 计算交叉范围的右下角像素坐标
	xMaxPixel = int((intersection.XMax - tifBbox.XMin) / (tifBbox.XMax - tifBbox.XMin) * float64(width))
	yMaxPixel = int((tifBbox.YMax - intersection.YMin) / (tifBbox.YMax - tifBbox.YMin) * float64(height))
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

// 得到相对路径
func GetRelativePath(bathpath, targetpath string) string {
	bathpath = filepath.Dir(bathpath) // 先得到路径
	relpath, err := filepath.Rel(bathpath, targetpath)
	if err != nil { // 有错误，就直接返回
		return targetpath
	}
	return relpath
}

// 通过绝对路径+相对路径，得到绝对路径
// 例如：c:/temp/a.b + ./c.d --> c:/temp/c.d
func GetAbsolutePath(basepath, relpath string) string {
	basepath = filepath.Dir(basepath)
	abspath := filepath.Clean(filepath.Join(basepath, relpath))
	abspath = filepath.ToSlash(abspath)
	return abspath
}