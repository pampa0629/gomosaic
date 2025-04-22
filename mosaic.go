package main
import (
	// "os"
	"fmt"
	"time"
	"math"
	"sync"
	// "strings"
	"bytes"	
	"image/color"
	"image"
	"image/png"
	"encoding/json"
	// "io/ioutil"
	// "path/filepath"
	"github.com/lukeroth/gdal"
	// "image/draw"
	xdraw "golang.org/x/image/draw" 
	// "github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// 镶嵌数据集的定义和方法实现

// COG tiff的定义
type COG struct {
	// tiff文件名，存储时为相对路径，打开后要还原为绝对路劲
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

// 镶嵌数据集的定义
type Mosaic struct {
	mosaicJson MosaicJson
}

// 打开并读取配置文件
func (m *Mosaic) Open(input string, ossfile bool, ak, sk, endpoint, bucket string) error {
	// 设置gdal的缓存大小
	gdal.SetCacheMax(1000*1000*1000)

	// 读取配置文件
	jsonBytes,err := readMosicJson(input, ossfile, ak, sk, endpoint, bucket)
	if err != nil {
		return err
	}
	
	// 解析配置文件
	err = json.Unmarshal(jsonBytes, &m.mosaicJson)
	if err != nil {
		return err
	}

	fmt.Println("mosaicJson:", m.mosaicJson)

	if ossfile {
		input = buildGdalOssPath(bucket, input)
	} 
	// 转换为绝对路径
	m.mosaicJson.AerialView.Name = GetAbsolutePath(input, m.mosaicJson.AerialView.Name)
	for i := 0; i < len(m.mosaicJson.Cogs); i++ {
		m.mosaicJson.Cogs[i].Name = GetAbsolutePath(input, m.mosaicJson.Cogs[i].Name)
	}
	return nil
}

// 根据xyz读取tile数据，返回png格式内容
func (m *Mosaic) ReadTile(z, x, y int) ([]byte, error) {
	// 根据xyz计算瓦片的地理范围
	geoBbox := CalcBBox(z, x, y)
	
	avBbox := m.mosaicJson.AerialView.Bbox
	// 如果瓦片的地理范围不在鸟瞰图的地理范围内，则返回空
	if geoBbox.XMax < avBbox.XMin || geoBbox.XMin > avBbox.XMax || geoBbox.YMax < avBbox.YMin || geoBbox.YMin > avBbox.YMax {
		return nil, nil
	}

	var outImage *image.RGBA
	// 判断一下是否应该使用鸟瞰图，原则是：要绘制的像素长或宽超过TILE_SIZE，则使用鸟瞰图
	xMinPixel, yMinPixel, xMaxPixel, yMaxPixel := CalcPixelRange(geoBbox, avBbox, m.mosaicJson.AerialView.Width, m.mosaicJson.AerialView.Height)
	if xMaxPixel - xMinPixel >= TILE_SIZE || yMaxPixel - yMinPixel >= TILE_SIZE {
		outImage,_ = m.DrawTileFromAerialView(geoBbox)
	} else {
		outImage,_ = m.DrawTileFromDatasets(geoBbox) 
	}
	// 编码为png格式
	buf := new(bytes.Buffer)
	err := png.Encode(buf, outImage)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// 从鸟瞰图中绘制tile数据
func (m *Mosaic) DrawTileFromAerialView(geoBbox BBox) (*image.RGBA, error) {
	// 打开鸟瞰图
	// dataset, err := gdal.Open(m.mosaicJson.AerialView.Name, gdal.ReadOnly)
	dataset,err := g_gdalPool.OpenDataset(m.mosaicJson.AerialView.Name).Lease(m.mosaicJson.AerialView.Name)
	if err != nil {
		return nil, err
	}
	// defer dataset.Close()
	defer g_gdalPool.CloseDataset(m.mosaicJson.AerialView.Name, dataset)

	outImage := image.NewRGBA(image.Rect(0, 0, TILE_SIZE, TILE_SIZE))
	err = DrawTileFromDataset(dataset, geoBbox, outImage, m.mosaicJson.AerialView.Name)
	if err != nil {
		return nil, err
	}
		
	// drawOutImage(outImage) // 用红色绘制tile的边框
	return outImage, nil
}

// 从具体的tiff文件中绘制tile
func (m *Mosaic) DrawTileFromDatasets(geoBbox BBox) (*image.RGBA, error) {
	// 计时器
	start := time.Now()

	outImage := image.NewRGBA(image.Rect(0, 0, TILE_SIZE, TILE_SIZE))
	names := make([]string, 0)
	// 先看看这个bbox和那些tiff文件有交集，tiff的bbox在m中有
	for _, cog := range m.mosaicJson.Cogs {
		if cog.Bbox.XMax < geoBbox.XMin || cog.Bbox.XMin > geoBbox.XMax || cog.Bbox.YMax < geoBbox.YMin || cog.Bbox.YMin > geoBbox.YMax {
			continue
		}
		names = append(names, cog.Name)
		if len(names) >= 4 { // 最多只能4个tiff，否则说明鸟瞰图没有发挥作用
			break
		}
	}

	// go 协程并发绘制
	var wg sync.WaitGroup	
	// 打开这些tiff文件
	for _, name := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			// 这里open dataset
			// 开始计时
			t0:= time.Now() 
			// dataset, _ := gdal.Open(name, gdal.ReadOnly)
			dataset,_ := g_gdalPool.OpenDataset(name).Lease(name)
			t1:= time.Now()
			DrawTileFromDataset(dataset, geoBbox, outImage, name)		
			t2:= time.Now()
			fmt.Println(name," open:", t1.Sub(t0).Nanoseconds()/1000000, " draw: ", t2.Sub(t1).Nanoseconds()/1000000)
			// defer dataset.Close()
			defer g_gdalPool.CloseDataset(name, dataset)
		} (name)
	}
	wg.Wait()

	end := time.Now()
	ms := end.Sub(start).Nanoseconds()/1000000
	if ms > 100 {
		fmt.Println("DrawTileFromDatasets time:", ms, names, geoBbox)
	}
	
	// drawOutImage(outImage) // 用红色绘制tile的边框
	return outImage, nil
}

// 提取dataset中指定地理范围的数据，绘制到outImage中
func DrawTileFromDataset(dataset gdal.Dataset, geoBbox BBox, outImage *image.RGBA, name string) (err error)  {
	// 计时器
	t0 := time.Now()

	// 根据瓦片的地理范围，计算要绘制的像素范围
	width := dataset.RasterXSize()
	height := dataset.RasterYSize()
	dtBbox := getDatasetBounds(dataset)
	
	xMinPixel, yMinPixel, xMaxPixel, yMaxPixel := CalcPixelRange(geoBbox, dtBbox, width, height)
	xSize:= xMaxPixel - xMinPixel
	ySize:= yMaxPixel - yMinPixel
		
	bandCount := 4 // 先默认为RGBA
	// 优化：如果要读取的范围大于TILE_SIZE，则缩小一下；scale为缩小的倍数。
	// 不然读取的数据太多，会很慢，最后还是要缩放到TILE_SIZE，没有用
	scale := math.Max(float64(xSize)/float64(TILE_SIZE), float64(ySize)/float64(TILE_SIZE))
	scale = math.Max(scale, 1) // 不能比1小
	xPixSize := int(math.Ceil(float64(xSize)/scale))
	yPixSize := int(math.Ceil(float64(ySize)/scale))

	t1 := time.Now()
	// 读取影像像素
	data := make([]byte, xPixSize*yPixSize*bandCount)
	err = dataset.IO(gdal.Read, xMinPixel, yMinPixel, xSize, ySize, 
				data, xPixSize, yPixSize, 3, []int{1, 2, 3}, bandCount, xPixSize*bandCount, 1)
	if err != nil {
		return err
	}

	t2 := time.Now()

	for i := 0; i < xPixSize*yPixSize; i++ {
		value := int(data[4*i+0]) + int(data[4*i+1]) + int(data[4*i+2])
		if value == 0 {
			data[4*i+3] = 0 // 无值的地方，alpha设置为0
		} else {
			data[4*i+3] = 255
		}
	}
	
	// 直接填充data，提高性能
	img := &image.RGBA{Pix: data, Stride: 4 * xPixSize, Rect: image.Rect(0, 0, xPixSize, yPixSize)}
	
	// 这里要计算一下，要绘制的图像，在tile中的位置
	outMinX, outMinY, outMaxX, outMaxY := CalcPixelRange(dtBbox, geoBbox, TILE_SIZE, TILE_SIZE)
	outRect := image.Rect(outMinX, outMinY, outMaxX, outMaxY)
	xdraw.CatmullRom.Scale(outImage, outRect, img, img.Bounds(), xdraw.Over, nil)

	t3 := time.Now()
	fmt.Println("DrawTileFromDataset:", t1.Sub(t0).Nanoseconds()/1000000, t2.Sub(t1).Nanoseconds()/1000000, t3.Sub(t2).Nanoseconds()/1000000)
	
	return nil
}

// 把image的边框描红
func drawOutImage(outImage *image.RGBA) {
	red := color.RGBA{255, 0, 0, 255}
	for i := 0; i < outImage.Bounds().Max.X; i++ {
		outImage.Set(i, 0, red)
		outImage.Set(i, outImage.Bounds().Max.Y-1, red)
	}
	for j := 0; j < outImage.Bounds().Max.Y; j++ {
		outImage.Set(0, j, red)
		outImage.Set(outImage.Bounds().Max.X-1, j, red)
	}
}