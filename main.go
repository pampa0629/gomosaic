package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	// test()
	// return	

	fmt.Println(os.Args)
	if len(os.Args) < 2 {
		help()
		// return
	}

	// subCmd := os.Args[1]
	// os.Args = append(os.Args[:1], os.Args[2:]...)

	input := flag.String("i", "", "input")
	output := flag.String("o", "", "output")
	oss := flag.Bool("oss", false, "if oss enable")
	ak := flag.String("ak", "", "oss ak")
	sk := flag.String("sk", "", "oss sk")
	endpoint := flag.String("ep", "", "endpoint")
	bucket := flag.String("b", "", "bucket")
	
	flag.Parse()

	// endpoint := "oss-cn-beijing.aliyuncs.com"       
	// accessKeyID := "LTAke743m4KtJhh97UrRd4Z"   
	// accessKeySecret := "uYtPI44izxk0RNCIp44qAmoDIclg"
	// bucketName := "pampa-bj"       
	// input := "testimages/source"
	// input := "/Users/zengzhiming/Documents/gisdata/test/source"
	
	// build -i -o /Users/zengzhiming/Documents/gisdata/test/dest  

	subCmd := "service"
	*oss = false
	*input = "/Users/pampa/Downloads/images/source/"
	*input = "/Users/pampa/Downloads/images/dest/"


	switch subCmd {
	case "build":
		build(*input, *output, *oss, *ak, *sk, *endpoint, *bucket)
	case "service":
		service(*input, *oss, *ak, *sk, *endpoint, *bucket)
	case "help":
	default:
		help()	
	}
}

func help() {
	fmt.Println(`
gomosaic help 获取帮助信息\n 
gomosaic build -i input -o output 构建镶嵌数据集\n 
gomosaic service -i input 启动镶嵌数据服务\n 
对于input或output为oss地址，则还需要同时 -oss true 和提供 -ak -sk -ep -b 参数`)
}