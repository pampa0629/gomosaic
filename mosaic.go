package main
import (
	"os"
	"fmt"
	// "time"
	"math"
	// "sync"
	// "strings"
	"bytes"	
	"image/color"
	"image"
	"image/png"
	"encoding/json"
	// "io/ioutil"
	// "path/filepath"
	"github.com/lukeroth/gdal"
	"image/draw"
	xdraw "golang.org/x/image/draw" 
	// "github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// 镶嵌数据集的定义和方法实现

// COG tiff的定义
type COG struct {
	// 镶嵌数据集的名称
	Name string `json:"name"`
	// 镶嵌数据集的地理范围
	Bbox BBox `json:"bbox"`
}

// 配置文件内容，json格式
type MosaicJson struct {
	// 鸟瞰图
	AerialView struct {
		Name string `json:"name"`
		// 鸟瞰图的地理范围
		Bbox BBox `json:"bbox"`
		// 记录鸟瞰图的像素大小
		Width int `json:"width"`
		Height int `json:"height"`
	} `json:"aerialView"`
	// 镶嵌数据集的所有cog tiff
	Cogs []COG `json:"cogs"` 
}

type Mosaic struct {
	mosaicJson MosaicJson
	// avDataset gdal.Dataset
	// width ,height int
	cogDatasets []gdal.Dataset
}

// 打开并读取配置文件
func (m *Mosaic) Open(mosaicJsonPath string) error {
	// 读取配置文件
	jsonBytes, err := os.ReadFile(mosaicJsonPath)
	if err != nil {
		return err
	}
	// 解析配置文件
	err = json.Unmarshal(jsonBytes, &m.mosaicJson)
	if err != nil {
		return err
	}

	// 转换为绝对路径
	m.mosaicJson.AerialView.Name = GetAbsolutePath(mosaicJsonPath, m.mosaicJson.AerialView.Name)
	for i := 0; i < len(m.mosaicJson.Cogs); i++ {
		m.mosaicJson.Cogs[i].Name = GetAbsolutePath(mosaicJsonPath, m.mosaicJson.Cogs[i].Name)
	}
	// fmt.Println("mosaicJson:", m.mosaicJson)

	// 打开镶嵌数据集
	for _, cog := range m.mosaicJson.Cogs {
		dataset, err := gdal.Open(cog.Name, gdal.ReadOnly)
		if err != nil {
			return err
		}
		m.cogDatasets = append(m.cogDatasets, dataset)
	}
	return nil
}

// 关闭所有数据集
func (m *Mosaic) Close() {
	// m.avDataset.Close()
	for _, dataset := range m.cogDatasets {
		dataset.Close()
	}
}

// 根据xyz读取tile数据
func (m *Mosaic) ReadTile(z, x, y int) ([]byte, error) {
	// 根据xyz计算瓦片的地理范围
	geoBbox := CalcBBox(z, x, y)
	// geoBbox = BBox{XMin: 105.0, XMax: 120.0, YMin: 0.0, YMax: 10.0}
	// fmt.Println("geoBbox:", geoBbox)

	avBbox := m.mosaicJson.AerialView.Bbox
	// 如果瓦片的地理范围不在鸟瞰图的地理范围内，则返回空
	if geoBbox.XMax < avBbox.XMin || geoBbox.XMin > avBbox.XMax || geoBbox.YMax < avBbox.YMin || geoBbox.YMin > avBbox.YMax {
		// fmt.Println("geoBbox is not in avBbox", geoBbox, avBbox)
		return nil, nil
	}

	// 判断一下是否应该使用鸟瞰图，原则是：要绘制的像素长或宽超过256像素，则使用鸟瞰图
	return m.ReadTileFromDataset(geoBbox)
	// width := m.avDataset.RasterXSize()
	// height := m.avDataset.RasterYSize()
	xMinPixel, yMinPixel, xMaxPixel, yMaxPixel := CalcPixelRange(geoBbox, avBbox, m.mosaicJson.AerialView.Width, m.mosaicJson.AerialView.Height)
	if xMaxPixel - xMinPixel >= TILE_SIZE || yMaxPixel - yMinPixel >= TILE_SIZE {
		return nil, nil
	} else {
		// return m.ReadTileFromCOG(geoBbox) 
		return nil, nil
	}
}

// 从指定的tiff dataset中读取tile数据
func (m *Mosaic) ReadTileFromDataset(geoBbox BBox) ([]byte, error) {
	fmt.Println("ReadTileFromDataset")
	// 打开鸟瞰图
	dataset, err := gdal.Open(m.mosaicJson.AerialView.Name, gdal.ReadOnly)
	if err != nil {
		return nil, err
	}

	// 根据瓦片的地理范围，计算要绘制的像素范围
	width := dataset.RasterXSize()
	height := dataset.RasterYSize()
	dtBbox := getDatasetBounds(dataset)
	fmt.Println("")
	fmt.Println("geoBbox:", geoBbox)
	fmt.Println("dtBbox:", dtBbox)
	fmt.Println("width:", width)
	fmt.Println("height:", height)
	fmt.Println("")

	xMinPixel, yMinPixel, xMaxPixel, yMaxPixel := CalcPixelRange(geoBbox, dtBbox, width, height)
	xSize:= xMaxPixel - xMinPixel
	ySize:= yMaxPixel - yMinPixel
	fmt.Println("xMinPixel:", xMinPixel)
	fmt.Println("yMinPixel:", yMinPixel)
	fmt.Println("xMaxPixel:", xMaxPixel)
	fmt.Println("yMaxPixel:", yMaxPixel)
	fmt.Println("xSize:", xSize)
	fmt.Println("ySize:", ySize)
		
	bandCount := 3 // ...
	// 读取tile数据
	tile := make([]byte, xSize*ySize*bandCount)
	err = dataset.IO(gdal.Read, xMinPixel, yMinPixel, xSize, ySize, tile, 
					xSize, ySize, bandCount, []int{1, 2, 3}, bandCount, xSize*3, 1)
	if err != nil {
		return nil, err
	}
	// 生成 image
	maxSize := int(math.Max(float64(xSize), float64(ySize)))
	img := image.NewRGBA(image.Rect(0, 0, maxSize, maxSize))
	for i := 0; i < xSize; i++ {
		for j := 0; j < ySize; j++ {
			img.Set(i, j, color.RGBA{tile[(j*xSize+i)*bandCount+0], 
									 tile[(j*xSize+i)*bandCount+1], 
									 tile[(j*xSize+i)*bandCount+2], 255})
		}
	}

	outImage := image.NewRGBA(image.Rect(0, 0, TILE_SIZE, TILE_SIZE))
	xdraw.NearestNeighbor.Scale(outImage, outImage.Bounds(), img, img.Bounds(), draw.Over, nil)
	
	buf := new(bytes.Buffer)
	err = png.Encode(buf, outImage)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}