package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

)

func test() {
	test_minio()
	// test_gos()
	// return

	// input := "/Users/zengzhiming/Documents/gisdata/images/source/"
	// output := "/Users/zengzhiming/Documents/gisdata/images/dest/"
	// input := "images/source/"
	// output := "images/dest2/"
	// oss := true
	// endpoint := "oss-cn-beijing.aliyuncs.com" // oss-cn-beijing-internal.aliyuncs.com "oss-cn-beijing.aliyuncs.com"
	// bucket := "pampa-bj"
	// build(input, output, oss, ak, sk, endpoint, bucket)
	// service(output, oss, ak, sk, endpoint, bucket)
	// test_one_tile(output, oss, ak, sk, endpoint, bucket)
}

func test_minio() {
	endpoint := "localhost:9000"  // MinIO 服务器地址 50350 9000
	// endpoint := "http://127.0.0.1:9000"
 
	accessKeyID := "PEgwflHRQztgKXF6lMWG"    // 你的 MinIO 访问密钥
	secretAccessKey := "2jVSiPtFK9V5EgL6V5owJKZxwpkKnvk4Eux62TZh" // 你的 MinIO 秘密密钥
	bucketName := "pampa"  // 你的 MinIO 存储桶名称

	t0 := time.Now()

	// 使用 MinIO Go SDK 创建一个新的 Minio 客户端
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false, // 设置为 true 如果使用 TLS
	})
	if err != nil {
		fmt.Println(err)
	}

	// 检查存储桶是否存在，如果不存在则创建
	found, err := client.BucketExists(context.Background(), bucketName)
	if err != nil {
		fmt.Println(err)
	}
	if !found {
		err = client.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{})
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Successfully created bucket: %s\n", bucketName)
	}

	// 上传文件
	filePath := "/Users/pampa/Downloads/model.zip"
	objectName := "model.zip"
	// 使用PutObject上传文件
	_, err = client.FPutObject(context.Background(), bucketName, objectName, filePath, minio.PutObjectOptions{})
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Successfully uploaded %s to MinIO\n", objectName)

	t1:= time.Now()

	// 本地文件保存路径
	localFilePath := "/Users/pampa/Downloads/model2.zip"
	// MinIO 存储桶中的文件名
	objectName = "model.zip"

	// 使用 FGetObject 下载文件
	err = client.FGetObject(context.Background(), bucketName, objectName, localFilePath, minio.GetObjectOptions{})
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Successfully downloaded %s from MinIO to %s\n", objectName, localFilePath)

	t2:= time.Now()
	fmt.Println("upload: ", t1.Sub(t0).Nanoseconds()/1000000, " download: ", t2.Sub(t1).Nanoseconds()/1000000)

	
	// // 下载文件
	// downloadPath := "/Users/pampa/Downloads/模型-建筑2.zip"

	// err = bucket.GetObjectToFile(objectName, downloadPath)
	// if err != nil {
	// 	fmt.Println("Error downloading file:", err)
	// }

	// fmt.Printf("Successfully downloaded %s from MinIO to %s\n", objectName, downloadPath)

}

func test_gos() {
	// 计时器
	start := time.Now()
	var wg sync.WaitGroup
	// 开启四个协程，每个休眠400毫秒
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fmt.Println("go func", i)
			time.Sleep(200 * time.Millisecond)
			
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Println("Time used:", elapsed)
}

func test_one_tile(output string, oss bool, ak, sk, endpoint, bucket string) {
	var mosaic Mosaic
	mosaic.Open(output, oss, ak, sk, endpoint, bucket)
	g_gdalPool.Init()

	// 计时器
	start := time.Now()

	// mosaic.Open("images/source/mosaic.json", false, "", "", "", "")
	// 10 826 401
	data, _ := mosaic.ReadTile(10, 826, 401)

	elapsed := time.Since(start)
	fmt.Println("Time used:", elapsed)

	// 创建本地文件
	file, _ := os.Create("test.png")
	// 把data写入文件
	file.Write(data)

}
