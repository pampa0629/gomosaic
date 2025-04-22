package main
import (
	"os"
	"fmt"
	"time"
	"math"
	"runtime"
	"sync"
	"bytes"
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
func build(input, output string, ossfile bool, accessKeyID, accessKeySecret, endpoint, bucketName string) {
	startTime := time.Now() // 计时器

	// 根据输入目录，得到所有要处理的tiff文件, 以及生成的目标tiff文件
	sources,dests,_ := getTiffFilesInDirectory(input, output, ossfile, accessKeyID, accessKeySecret, endpoint, bucketName)
	fmt.Println("sources number: ", len(sources))
	for i,source := range sources {
		fmt.Println("tiff :", i, source)
	}
	if len(sources) == 0 || len(dests) == 0 {
		fmt.Println("No tiff files found in directory:", input)
		return
	}
	
	// 处理所有的tiff文件
	cog_all(sources, dests)	

	// 构造鸟瞰图，其实也是一个cog tiff
	buildAerialView(dests)
	
	elapsedTime := time.Since(startTime)
	fmt.Printf("build took %s\n", elapsedTime)
}

// cog 所有的tiff文件，分批用协程运行
func cog_all(sources, dests []string) {
	// 获取当前系统的 CPU 核心数
	numCPU := int(float32(runtime.NumCPU()) * 0.7) // 只用一部分，防止CPU过热
	fmt.Println("numCPU:", numCPU)

	numJobs := len(sources)
	concurrentWorkers := numCPU
	if numJobs < numCPU { // 如果任务数小于 CPU 核心数，就使用任务数作为并发协程数
		concurrentWorkers = numJobs
	}
	
	jobs := make(chan int, concurrentWorkers) // 控制同时运行的协程数量
	results := make(chan int, numJobs)

	// 使用 WaitGroup 来等待所有协程完成
	var wg sync.WaitGroup

	// 启动工作协程
	for i := 1; i <= concurrentWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for job := range jobs {
				// 这里放入具体的协程任务逻辑
				fmt.Printf("Worker %d processing job %d\n", id, job)
				// 模拟任务执行时间
				cog_one(sources[job], dests[job])
				results <- job * 2 // 例如，将任务结果发送到结果通道
			}			
		}(i)
	}

	// 提供工作
	go func() {
		for i := 0; i < numJobs; i++ {
			jobs <- i
		}
		close(jobs) // 关闭 jobs 通道，告诉协程没有更多的任务
	}()

	// 等待所有工作协程完成
	wg.Wait()

	// 关闭结果通道，以便在主协程中读取所有结果
	close(results)
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

	// 如果 output的前几个字符为 vsioss，则要换个写法
	if strings.HasPrefix(output, "/vsioss") {
		writeOssFile(output, jsonData)
	} else {
		err = os.WriteFile(output, jsonData, 0644)
	}
	
	if err != nil {
		fmt.Println("Error writing JSON to file:", err)
		return
	}
	// fmt.Println("JSON data written to memory.json successfully")
}

// 把内容写入oss
func writeOssFile(output string, data []byte) {
	// 设置 GDAL 支持阿里云 OSS
	ak,sk,ep := getOssOptions()
	// 创建 OSS 客户端
	client, err := oss.New(ep, ak, sk)
	if err != nil {
		fmt.Println("Error creating OSS client:", err)
		return
	}

	// 获取存储空间
	// 去掉output前面的 /vsioss，，再取/前面的内容，就是bucketName
	bucketName := strings.TrimPrefix(output, "/vsioss/")
	bucketName = strings.Split(bucketName, "/")[0]
	fmt.Println("bucketName:", bucketName)
	// 剩下的就是objectKey
	objectKey := strings.TrimPrefix(output, "/vsioss/"+bucketName+"/")
	fmt.Println("objectKey:", objectKey)
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		fmt.Println("Error getting bucket:", err)
		return
	}

	// 创建一个字节缓冲区
	buffer := bytes.NewBuffer(data)
	// 上传数组数据到 OSS 文件
	err = bucket.PutObject(objectKey, buffer)
	if err != nil {
		fmt.Println("Error uploading object:", err)
		return
	}
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
	datasets := make([]gdal.Dataset, 0)
	for _,tiff := range tiffs {
		// var err error
		dataset,err := gdal.Open(tiff, gdal.ReadOnly)
		if err != nil {
			fmt.Println("Error opening TIFF file:", err)			
		}
		// defer datasets[i].Close()
		datasets = append(datasets, dataset)
	}

	// 得到鸟瞰图总的范围，和像素size
	bbox, xSize, ySize := calcAerialViewInfos(datasets)
	
	// 计算每个像素代表的地理范围
	xRes := (bbox.XMax - bbox.XMin) / float64(xSize)
	yRes := (bbox.YMax - bbox.YMin) / float64(ySize)
		
	bandCount := datasets[0].RasterCount()
	datatype := datasets[0].RasterBand(1).RasterDataType()
	driver:=datasets[0].Driver()
	outDataset := driver.Create(output, xSize, ySize, bandCount, datatype, []string{"TILED=YES", "COMPRESS=DEFLATE", "COPY_SRC_OVERVIEWS=YES"})
	
	// 写入各类元数据
	setMetadata(outDataset, datasets[0], bbox, xSize, ySize)

	// 循环写入每一个tiff文件的像素data
	for _,dataset := range datasets {
		writeData(outDataset, dataset, xRes, yRes, bbox.XMin, bbox.YMax)
	}

	// 创建金字塔
	buildOverviews(&outDataset)
	buildJson(tiffs, datasets, output, outDataset)

	defer outDataset.Close()
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

// 得到tiff数据集的范围
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

// 得到datasets总的范围，计算总像素大小
func calcAerialViewInfos(datasets []gdal.Dataset) (bbox BBox, xSize, ySize int) {
	// 先把一个数据集的地理宽高记下来
	bbox = getDatasetBounds(datasets[0])
	// 得到一个数据集的宽高
	width := bbox.XMax - bbox.XMin
	height := bbox.YMax - bbox.YMin
	// 循环所有dataset，得到总的范围
	for i:=1;i<len(datasets);i++{
		dtBbox := getDatasetBounds(datasets[i])
		bbox.XMin = math.Min(bbox.XMin, dtBbox.XMin)
		bbox.XMax = math.Max(bbox.XMax, dtBbox.XMax)
		bbox.YMin = math.Min(bbox.YMin, dtBbox.YMin)
		bbox.YMax = math.Max(bbox.YMax, dtBbox.YMax)
	}

	// count 是x和y方向，数据集的个数	
	xCount := int(math.Round((bbox.XMax - bbox.XMin) / width)) 
	yCount := int(math.Round((bbox.YMax - bbox.YMin) / height))
	
	// 计算总像素大小，方法：用总宽高除以一个数据集的宽高，得到数量，再乘以顶层金字塔的像素size
	band := datasets[0].RasterBand(1)
	// 得到最粗糙层overviews的像素大小
	ovCount := band.OverviewCount()
	if ovCount > 0 { // 有金字塔
		overview := band.Overview(ovCount-1)
		// 计算x和y两个方向的数据集数量
		xSize = xCount * overview.XSize()
		ySize = yCount * overview.YSize()
	} else { // 没有金字塔，用原始数据集的像素大小
		fmt.Println("Error: No overviews found in dataset")
		xSize = xCount * datasets[0].RasterXSize()
		ySize = yCount * datasets[0].RasterYSize()
	}
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
    
	// 创建输出 TIFF 文件
    outputDataset := driver.CreateCopy(output, inputDataset, 1, []string{"TILED=YES", "COMPRESS=DEFLATE", "COPY_SRC_OVERVIEWS=YES"}, nil,nil)
    if err != nil {
        fmt.Println("Error creating COG TIFF file:", err)
        return
    }
	buildOverviews(&outputDataset)
    defer outputDataset.Close()
	
    fmt.Println("COG TIFF file created successfully:", output)
}

// 构建金字塔
func buildOverviews(dataset *gdal.Dataset) {
	// 得到数据集的分辨率
	xSize := dataset.RasterXSize()
	ySize := dataset.RasterYSize()
	// 取较大值
	maxSize := math.Max(float64(xSize), float64(ySize))
	// 计算金字塔级别
	levels := 0
	var overviews []int // 声明一个整数切片
	// 确定金字塔的层次，最顶层金字塔的像素不能超过TILE_SIZE
	for maxSize > TILE_SIZE { 
		maxSize = maxSize / 2
		levels++
		overviews = append(overviews, 1 << levels)
	}
	if len(overviews) == 0 {
		fmt.Println("No overviews needed for dataset")
		return
	}
	
	// 得到波段数
	bandCount := dataset.RasterCount()
	// 循环波段，构建金字塔
	bands := make([]int, bandCount) // 创建长度为 n 的整数切片
	for i := range bands {
		bands[i] = i+1 // 波段编号是从 1 到 n 
	}
	fmt.Println("overviews:", overviews)
	fmt.Println("bandCount:", bandCount, "bands:", bands)
	// fmt.Println(getDatasetName(dataset), " overviews:", overviews)
	dataset.BuildOverviews("NEAREST", levels, overviews, bandCount, bands, 
		func(complete float64, message string, progressArg interface{}) int { return 1 }, nil) // 通过指定金字塔级别的大小来构建金字塔
}

// 得到数据集的名字
// func getDatasetName(dataset *gdal.Dataset) string {
// 	fileList := dataset.FileList()
// 	if len(fileList) > 0 {
// 		return fileList[0]
// 	}
// 	return ""
// }

// 给gdal设置oss的ak sk和Endpoint
func setOssOptions(accessKeyID, accessKeySecret, endpoint string) {
	// 设置 GDAL 支持阿里云 OSS
	gdal.CPLSetConfigOption("OSS_ACCESS_KEY_ID", accessKeyID)
	gdal.CPLSetConfigOption("OSS_SECRET_ACCESS_KEY", accessKeySecret)
	gdal.CPLSetConfigOption("OSS_ENDPOINT", endpoint)
	// CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE
	gdal.CPLSetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES")	
}

// 把之前设置的oss的ak sk和Endpoint取出来
func getOssOptions() (accessKeyID, accessKeySecret, endpoint string) {
	accessKeyID = gdal.CPLGetConfigOption("OSS_ACCESS_KEY_ID", "")
	accessKeySecret = gdal.CPLGetConfigOption("OSS_SECRET_ACCESS_KEY", "")
	endpoint = gdal.CPLGetConfigOption("OSS_ENDPOINT", "")	
	return 
}