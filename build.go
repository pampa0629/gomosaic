package main
import (
	"os"
	"fmt"
	"time"
	"math"
	"sync"
	"strings"
	"encoding/json"
	// "io/ioutil"
	"path/filepath"
	"github.com/lukeroth/gdal"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// 镶嵌数据集的创建


// 根据输入目录，创建镶嵌数据集，具体包括：
// 1. 遍历目录，把所有tif文件生成cog tiff
// 2. 创建一个大tiff文件，把每个tif文件最上层的金字塔数据复制过来
// 3. 生成json配置文件，供后续发布服务使用
func build() {
	startTime := time.Now() // 计时器

	// 根据输入目录，得到所有要处理的tiff文件, 以及生成的目标tiff文件
	sources,dests,_ := getTiffFilesInDirectory()
	fmt.Println("sources:", sources)

	// 用goroutine并发，cog_one 处理每一个tiff文件
	// 记得换个并行的写法
	var wg sync.WaitGroup
	for i,source := range sources {
		wg.Add(1)
		go func(source, dest string) { // 将循环变量传递给局部变量
			defer wg.Done()
			cog_one(source, dest)
		}(source, dests[i]) // 传递循环变量的值
	}
	wg.Wait()

	buildAerialView(dests)
	
	elapsedTime := time.Since(startTime)
	fmt.Printf("build took %s\n", elapsedTime)
}

// 生成json配置文件，供后续发布服务使用
func buildJson(dests []string, datasets[]gdal.Dataset, avTiff string, avDataset gdal.Dataset) {
	// 得到dest的目录
	destDir := filepath.Dir(avTiff)
	output := destDir+"/mosaic.json"	

	// 构造json对象
	data := MosaicJson{}
	data.AerialView.Name = GetRelativePath(output, avTiff) 
	bbox := getDatasetBounds(avDataset)
	data.AerialView.Bbox = bbox
	data.AerialView.Width = avDataset.RasterXSize()
	data.AerialView.Height = avDataset.RasterYSize()
	for i, dest := range dests {
		var cog COG
		cog.Name = GetRelativePath(output, dest)
		cog.Bbox = getDatasetBounds(datasets[i])
		data.Cogs = append(data.Cogs, cog)		
	}
	
	// 将 JSON 对象转换为 JSON 字符串
	jsonData, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}
	
	err = os.WriteFile(output, jsonData, 0644)
	if err != nil {
		fmt.Println("Error writing JSON to file:", err)
		return
	}

	fmt.Println("JSON data written to memory.json successfully")

}

// 得到指定目录下的所有tiff文件
func getTiffFilesInDirectory() (sources, dests []string ,err error) {
	suffixs := []string{".tif", ".tiff"}
	// 是否使用oss存储tiff文件，后续要做成参数输入
	ossfile := false
	if ossfile {
		endpoint := "oss-cn-beijing.aliyuncs.com"       
		accessKeyID := "LTabdddeZ"   
		accessKeySecret := "uYdfNCdegmvDIg"
		bucketName := "pampa-bj"       
		directory := "testimages/source"
		setOssOptions(accessKeyID, accessKeySecret, endpoint)
	
		client, err := oss.New(endpoint, accessKeyID, accessKeySecret)
		if err != nil {
			fmt.Println("Error creating OSS client:", err)
			return nil, nil, err
		}
	
		sources, _ = findOssFilesWithSuffixs(client, bucketName, directory, suffixs)		
	} else {
		// input := "/Users/pampa/Documents/data/images/source"
		input := "/Users/pampa/Documents/data/test/source"
		
		// 查找input下面所有后缀名为tif或者tiff的文件
		sources, err = findLocalFilesWithSuffixs(input, suffixs)
	}	

	// 循环tiffFiles，构造dests；这个后续要改为外部参数输入
	for _,tiff := range sources {
		dest := strings.Replace(tiff, "source", "dest", 1)
		dests = append(dests, dest)
	}	
	return
}

// 辅助函数，检查文件的后缀名是否在指定的数组中
func hasExtension(filename string, extensions []string) bool {
	lowercaseFilename := strings.ToLower(filename)
	for _, ext := range extensions {
		if strings.HasSuffix(lowercaseFilename, ext) {
			return true
		}
	}
	return false
}

// 查找本地指定目录下的所有后缀名为suffix的文件
func findLocalFilesWithSuffixs(directory string, suffixs[] string) ([]string, error) {
	var result []string

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 判断是否为文件且后缀名匹配
		if !info.IsDir() && hasExtension(path, suffixs) {
			result = append(result, path)
		}
		return nil
	})

	return result, err
}

// 在指定oss目录下查找指定后缀的文件
func findOssFilesWithSuffixs(client *oss.Client, bucketName, directory string, suffixs []string) ([]string, error) {
	var tiffFiles []string

	bucket,_ := client.Bucket(bucketName)
	lsRes, err := bucket.ListObjects(oss.Prefix(directory))
	if err != nil {
		return nil, err
	}

	for _, object := range lsRes.Objects {
		// 判断是否是以 ".tiff" 或 ".tif" 结尾的文件
		if hasExtension(object.Key, suffixs) {
			tiff :=	"/vsioss/" + bucketName + "/" + object.Key 
			tiffFiles = append(tiffFiles, tiff)
		}
	}

	return tiffFiles, nil
}

// 给gdal设置oss的ak sk和Endpoint
func setOssOptions(accessKeyID, accessKeySecret, endpoint string) {
	// 设置 GDAL 支持阿里云 OSS
	gdal.CPLSetConfigOption("OSS_ACCESS_KEY_ID", "LTAI5tBkeddgeh97UrRd4Z")
	gdal.CPLSetConfigOption("OSS_SECRET_ACCESS_KEY", "uYtPIizxkgegegeggpqAmv8u5iqoDIclg")
	gdal.CPLSetConfigOption("OSS_ENDPOINT", "oss-cn-beijing.aliyuncs.com")
	// CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE
	gdal.CPLSetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES")	
}

// 得到gdal.DataType的size
func getDataTypeSize(dataType gdal.DataType) int {
	switch dataType {
	case gdal.Byte:
		return 1
	case gdal.UInt16:
	case gdal.Int16:
		return 2
	case gdal.UInt32:
	case gdal.Int32:
	case gdal.Float32:
		return 4
	case gdal.Float64:
		return 8
	default:
		fmt.Println("Error: Unknown data type:", dataType)
		return 0
	}
	return 0
}

// 构造鸟瞰图，其实也是一个cog tiff
func buildAerialView(tiffs []string) string {
	// 得到dest的目录
	destDir := filepath.Dir(tiffs[0])
	output := destDir+"/AerialView.tif"

	// 打开所有tiff文件
	// 准备好n个dataset的数组
	datasets := make([]gdal.Dataset, len(tiffs))
	for i,tiff := range tiffs {
		var err error
		datasets[i],err = gdal.Open(tiff, gdal.ReadOnly)
		if err != nil {
			fmt.Println("Error opening TIFF file from OSS:", err)
			return ""
		}
		defer datasets[i].Close()
	}

	// 得到总的范围，计算大tiff的像素size
	bbox, xSize, ySize := calcInfos(datasets)
	// xMin, yMin, xMax, yMax, xSize, ySize := calcInfos(datasets)
	
	// 计算每个像素代表的地理范围
	xRes := (bbox.XMax - bbox.XMin) / float64(xSize)
	yRes := (bbox.YMax - bbox.YMin) / float64(ySize)
		
	bandCount := datasets[0].RasterCount()
	datatype := datasets[0].RasterBand(1).RasterDataType()
	driver:=datasets[0].Driver()
	outDataset := driver.Create(output, xSize, ySize, bandCount, datatype, []string{"TILED=YES", "COMPRESS=DEFLATE", "COPY_SRC_OVERVIEWS=YES"})
	defer outDataset.Close()
	// 写入各类元数据
	setMetadata(outDataset, datasets[0], bbox, xSize, ySize)

	// 循环写入每一个tiff文件的像素data
	for _,dataset := range datasets {
		writeData(outDataset, dataset, xRes, yRes, bbox.XMin, bbox.YMax)
	}

	// 创建金字塔
	buildOverviews(&outDataset)
	buildJson(tiffs, datasets, output, outDataset)
	return output
}

// 写入一个tiff文件的最上层金字塔的数据
func writeData(outDataset, dataset gdal.Dataset, xRes, yRes, xMin, yMax float64) {
	// 根据dataset的minx和miny，计算在总tiff中的像素位置
	dtBbox := getDatasetBounds(dataset)
	xOff := int(math.Round((dtBbox.XMin - xMin) / xRes))
	yOff := int(math.Round((yMax - dtBbox.YMax) / yRes))

	datatype := dataset.RasterBand(1).RasterDataType()
	// 循环波段
	bandCount := dataset.RasterCount()
	for i:=1;i<=bandCount;i++{
		band := dataset.RasterBand(i)
		// 得到最粗糙层overviews的像素大小
		overview := band.Overview(band.OverviewCount()-1)
		xOvSize := overview.XSize()
		yOvSize := overview.YSize()
		data := make([]uint8, xOvSize*yOvSize*getDataTypeSize(datatype))
		// 读取数据
		overview.IO(gdal.Read, 0,0,xOvSize, yOvSize, data, xOvSize, yOvSize, 0, 0)
		// fmt.Println("xOvSize:", xOvSize)
		// fmt.Println("yOvSize:", yOvSize)
				
		// 写入数据
		outBand := outDataset.RasterBand(i)
		outBand.IO(gdal.Write, xOff, yOff, xOvSize, yOvSize, data, xOvSize, yOvSize, 0, 0)
	}
}

// 设置各类元数据
func setMetadata(dataset gdal.Dataset, srcDataset gdal.Dataset, bbox BBox, xSize, ySize int) {
	// 根据地理范围和像素size，计算Transform，并设置给dataset
	geoTransform := [6]float64{bbox.XMin, (bbox.XMax - bbox.XMin) / float64(xSize), 0, 
								bbox.YMax, 0, (bbox.YMin - bbox.YMax) / float64(ySize)}
	dataset.SetGeoTransform(geoTransform)
	// 设置投影
	dataset.SetProjection(srcDataset.Projection())

	// 设置 no data
	nodata,valid := srcDataset.RasterBand(1).NoDataValue()
	if valid {
		bandCount := dataset.RasterCount()
		for i:=1;i<=bandCount;i++{
			dataset.RasterBand(i).SetNoDataValue(nodata)
		}
	}
	
	// todo mode
}

// 得到有个数据集的范围
func getDatasetBounds(dataset gdal.Dataset) (bbox BBox) {
	// 得到size
	xSize := dataset.RasterXSize()
	ySize := dataset.RasterYSize()
	// 得到数据集的范围
	geoTransform := dataset.GeoTransform()
	// fmt.Println("geoTransform:", geoTransform)
	bbox.XMin = geoTransform[0]
	bbox.YMax = geoTransform[3]
	bbox.XMax = geoTransform[0] + geoTransform[1] * float64(xSize)
	bbox.YMin = geoTransform[3] + geoTransform[5] * float64(ySize)
	return
}

// 打开所有tiff文件，得到总的范围，计算总像素大小，以及返回datasets
func calcInfos(datasets []gdal.Dataset) (bbox BBox, xSize, ySize int) {
	// 循环所有dataset，得到总的范围
	bbox = getDatasetBounds(datasets[0])
	// 先把一个数据集的地理宽高记下来
	width := bbox.XMax - bbox.XMin
	height := bbox.YMax - bbox.YMin
	for i:=1;i<len(datasets);i++{
		dtBbox := getDatasetBounds(datasets[i])
		bbox.XMin = math.Min(bbox.XMin, dtBbox.XMin)
		bbox.YMax = math.Max(bbox.YMax, dtBbox.YMax)
		bbox.XMax = math.Max(bbox.XMax, dtBbox.XMax)
		bbox.YMin = math.Min(bbox.YMin, dtBbox.YMin)
	}
	
	// 计算总像素大小，方法：用总宽高除以一个数据集的宽高，得到数量，再乘以顶层金字塔的像素size
	band := datasets[0].RasterBand(1)
	// 得到最粗糙层overviews的像素大小
	overview := band.Overview(band.OverviewCount()-1)
	xCount := int(math.Round((bbox.XMax - bbox.XMin) / width)) 
	yCount := int(math.Round((bbox.YMax - bbox.YMin) / height))
	xSize = xCount * overview.XSize()
	ySize = yCount * overview.YSize()
	return 
}



// tiff文件，构建 COG
func cog_one(input, output string) {
	
    inputDataset, err := gdal.Open(input, gdal.ReadOnly)
    if err != nil {
        fmt.Println("Error opening TIFF file from OSS:", err)
        return
    }
    defer inputDataset.Close()

    // 读取文件信息等操作
	driver := inputDataset.Driver()
    // fmt.Println("Driver:", driver.LongName())
	// xSize := inputDataset.RasterXSize()
    // ySize := inputDataset.RasterYSize()
    // fmt.Println("Raster X Size:", xSize)
    // fmt.Println("Raster Y Size:", ySize)
	// datatype := inputDataset.RasterBand(1).RasterDataType()
	// fmt.Println("datatype:",datatype)

	// 创建输出 TIFF 文件
    outputDataset := driver.CreateCopy(output, inputDataset, 1, []string{"TILED=YES", "COMPRESS=DEFLATE", "COPY_SRC_OVERVIEWS=YES"}, nil,nil)
    if err != nil {
        fmt.Println("Error creating COG TIFF file:", err)
        return
    }
    defer outputDataset.Close()
	buildOverviews(&outputDataset)
	
    fmt.Println("COG TIFF file created successfully.")
}

// 构建金字塔
func buildOverviews(dataset *gdal.Dataset) {
	// 得到数据集的分辨率
	xSize := dataset.RasterXSize()
	ySize := dataset.RasterYSize()
	// 取较大值
	maxSize := max(xSize, ySize)
	// 计算金字塔级别
	levels := 0
	var overviews []int // 声明一个整数切片
	for maxSize > 256 {
		maxSize = maxSize / 2
		levels++
		overviews = append(overviews, 1 << levels)
	}
	
	// 得到波段数
	bandCount := dataset.RasterCount()
	// 循环波段，构建金字塔
	bands := make([]int, bandCount) // 创建长度为 n 的整数切片
	for i := range bands {
		bands[i] = i+1 // 从 0 到 n-1 赋值给切片元素
	}
	// fmt.Println("levels:", levels)
	// fmt.Println("overviews:", overviews)
	// fmt.Println("bandCount:", bandCount)
	// fmt.Println("bands:", bands)

	dataset.BuildOverviews("NEAREST", levels, overviews, bandCount, bands, 
		func(complete float64, message string, progressArg interface{}) int { return 1 }, nil) // 通过指定金字塔级别的大小来构建金字塔
}