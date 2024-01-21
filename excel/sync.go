package excel

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"fringe/cfg"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
)

func urlPrefix() (ossPrefix, fileName string) {
	var val = cfg.Val(func(cfg *cfg.AppCfg) any {
		return []string{
			cfg.Oss.Prefix + "/",
			cfg.Oss.TxtName,
		}
	})

	var txtName = val.([]string)[1]
	ossPrefix = val.([]string)[0]
	fileName = path.Base(txtName)

	return ossPrefix + txtName[:len(txtName)-len(fileName)], fileName
}
func HttpGet(url string, timeout time.Duration, fn func(rep *http.Response)) (*bytes.Buffer, error) {
	var client = http.Client{Timeout: timeout}
	var response, err = client.Get(url)
	if err != nil {
		return nil, err
	}

	var buffer = new(bytes.Buffer)
	_, err = buffer.ReadFrom(response.Body)

	if fn != nil {
		fn(response)
	}
	return buffer, err
}

func txtFileContent(url string) ([]string, error) {
	var buffer, err = HttpGet(url, time.Second*3, nil)
	if err != nil {
		return nil, err
	}

	var content = strings.Replace(buffer.String(), "\n", "", -1)
	content = strings.Replace(content, "\r", "", -1)

	return strings.Split(content, ","), nil
}

var localFileMd5 = make(map[string]string, 0)

func downloadFile(url, localPath string) (bool, error) {
	var fileMd5 string
	var buffer, err = HttpGet(url, time.Second*3, func(rep *http.Response) {
		fileMd5 = rep.Header.Get("Content-Md5")
	})
	if err != nil {
		return false, err
	}

	if len(fileMd5) <= 0 {
		return false, errors.New("GET 获取文件失败, url => " + url)
	}

	// 不需要更新文件
	if val, ok := localFileMd5[path.Base(url)]; ok && val == fileMd5 {
		return false, nil
	}

	var file, err01 = os.OpenFile(localPath+path.Base(url), os.O_WRONLY|os.O_CREATE, 0644)
	if err01 != nil {
		return false, err01
	}

	defer file.Close()
	var fileWriter = bufio.NewWriter(file)
	_, err = fileWriter.Write(buffer.Bytes())
	if err != nil {
		return false, err
	}

	if err = fileWriter.Flush(); err != nil {
		return false, err
	}
	return true, file.Sync()
}

func SyncJsonFile() error {
	var loadFunc = func() error {
		// 加载配置文件
		if err := loadJsonToExcel(); err != nil {
			return err
		}

		return nil
	}
	var ossSwitch = cfg.Val(func(cfg *cfg.AppCfg) any {
		return cfg.Oss.Switch
	}).(bool)
	if false == ossSwitch {
		return loadFunc()
	}
	var ossPrefix, fileName = urlPrefix()
	var files, err = txtFileContent(ossPrefix + fileName)
	if err != nil {
		fmt.Println(err)
		return err
	}

	var reload bool
	var fileNum int
	_ = os.MkdirAll(cfg.JsonFilePath, 0644)
	for _, file := range files {
		fileName = strings.Replace(file, " ", "", -1)
		if fileNum%40 == 0 {
			fileNum = 0
			<-time.After(time.Millisecond * 100)
		}
		var val, _ = downloadFile(ossPrefix+fileName, cfg.JsonFilePath)
		if val {
			reload = val
		}

		fileNum++
	}

	if reload {
		return loadFunc()
	}
	return nil
}
