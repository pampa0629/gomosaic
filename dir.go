package main
import (
	"os"
	"path/filepath"
	"fmt"
	"strings"
	"io"
	// "io/ioutil"
	// "time"
	// "math"	
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// 目录方向的方法都在这里

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

// 得到指定目录下的所有tiff文件，返回源文件和目标文件的集合
func getTiffFilesInDirectory(input, output string, ossfile bool, accessKeyID, accessKeySecret, endpoint, bucketName string ) (sources, dests []string ,err error) {
	// input = output
	suffixs := []string{".tif", ".tiff"}
	if ossfile {
		setOssOptions(accessKeyID, accessKeySecret, endpoint)

		client, err := oss.New(endpoint, accessKeyID, accessKeySecret)
		if err != nil {
			fmt.Println("Error creating OSS client:", err)
			return nil, nil, err
		}
	
		sources, _ = findOssFilesWithSuffixs(client, bucketName, input, suffixs)		
	} else {
		// 查找input下面所有后缀名为tif或者tiff的文件
		fmt.Println("input:", input)
		fmt.Println("suffixs:", suffixs)
		sources, err = findLocalFilesWithSuffixs(input, suffixs)
		if err != nil {
			fmt.Println("2 Error finding local files:", err)
			return nil, nil, err
		}
		fmt.Println("sources:", sources)
	}	

	// 循环tiffFiles，构造dests；这个后续要改为外部参数输入
	if ossfile { // 对于oss，需要构造oss的路径
		output = buildGdalOssPath(bucketName, output)
	}
	for _,tiff := range sources {
		// 得到tiff的文件名
		tiffName := filepath.Base(tiff)
		// 构造dest的文件名
		dest := filepath.Join(output, tiffName)
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
			fmt.Println("Error walking the path:", err)
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
	// marker := "/images/source/E50C001100.tif" 
	lsRes, err := bucket.ListObjects(oss.MaxKeys(1000), oss.Prefix(directory))
	if err != nil {
		return nil, err
	}

	for _, object := range lsRes.Objects {
		// 判断是否是以 ".tiff" 或 ".tif" 结尾的文件
		if hasExtension(object.Key, suffixs) {
			tiff :=	"/vsioss/" + bucketName + "/" + object.Key 
			// if i >= 90 {
				tiffFiles = append(tiffFiles, tiff)
			// }
		}
	}

	return tiffFiles, nil
}

// 构造gdal访问的oss目录
func buildGdalOssPath(bucketName, directory string) string {
	return "/vsioss/" + bucketName + "/" + directory+ "/" 	
}

// 读取json配置文件的内容
func readMosicJson(input string, ossfile bool, ak, sk, endpoint, bucket string) ([]byte,error) {
	// 得到input的目录		
	inputDir := filepath.Dir(input)

	if ossfile {
		setOssOptions(ak, sk, endpoint)

		client, err := oss.New(endpoint, ak, sk)
		if err != nil {
			fmt.Println("Error creating OSS client:", err)
			return nil,err
		}
		bucket, err := client.Bucket(bucket)
		if err != nil {
			fmt.Println("Error creating OSS Bucket:", err)
			return nil,err
		}
		body, err := bucket.GetObject(inputDir+"/mosaic.json")
		if err != nil {
			fmt.Println("Error OSS GetObject:", err)
			return nil,err
		}
		data, err := io.ReadAll(body)
		if err != nil {
			fmt.Println("Error io.ReadAll:", err)
			return nil,err
		}
		return data, err
	} else {
		jsonBytes, err := os.ReadFile(inputDir+"/mosaic.json")
		return jsonBytes,err
	}
}