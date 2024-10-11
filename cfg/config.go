package cfg

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
	"gorm.io/gorm/logger"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var cfgSec *AppCfg

func init() {
	cfgSec = &AppCfg{
		lock: new(sync.RWMutex),
	}
	if err := LoadConfig(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func Val(fn func(cfg *AppCfg) interface{}) interface{} {
	cfgSec.lock.RLock()
	defer cfgSec.lock.RUnlock()

	return fn(cfgSec)
}

type AppCfg struct {
	lock *sync.RWMutex
	Mysql
	Redis
	Jwt
	Oss
	IMPort         uint16 `yaml:"im_port"`
	ServerHttpPort uint16 `yaml:"server_http_port"`
	ServerTcpAddr  string `yaml:"server_tcp_addr"`
	Environment    string `yaml:"environment"`
	LogLevel       int8   `yaml:"log_level"`
	RetriesTimes   uint8  `yaml:"retries_times"`
}

type Mysql struct {
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	Addr            string `yaml:"addr"`
	DBName          string `yaml:"db_name"`
	Charset         string `yaml:"charset"`
	ParseTime       bool   `yaml:"parse_time"`
	Loc             string `yaml:"loc"`
	MaxIdleConns    uint8  `yaml:"max_idle_conns"`
	MaxOpenConns    uint16 `yaml:"max_open_conns"`
	ConnMaxLifetime uint8  `yaml:"conn_max_lifetime"`
	LogLevel        uint8  `yaml:"log_level"`
}

type Redis struct {
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	Addr         string `yaml:"addr"`
	DB           uint8  `yaml:"db"`
	PoolSize     uint8  `yaml:"pool_size"`
	DialTimeout  uint8  `yaml:"dial_timeout"`
	ReadTimeout  uint8  `yaml:"read_timeout"`
	WriteTimeout uint   `yaml:"write_timeout"`
	IdleTimeout  uint8  `yaml:"idle_timeout"`
	MaxIdleConns uint8  `yaml:"max_idle_conns"`
	MinIdleConns uint8  `yaml:"min_idle_conns"`
}

type Jwt struct {
	Issuer     string   `yaml:"issuer"`
	Subject    string   `yaml:"subject"`
	Audience   []string `yaml:"audience"`
	PrivateKey string   `yaml:"private_key"`
}

type Oss struct {
	Prefix  string `yaml:"prefix"`
	TxtName string `yaml:"txt_name"`
	Switch  bool   `yaml:"switch"`
}

func (a *AppCfg) Default() *AppCfg {
	a.Mysql = Mysql{
		Username:        "username",
		Password:        "password",
		Addr:            "127.0.0.1:3306",
		DBName:          "test-db",
		Charset:         "utf8mb4",
		ParseTime:       true,
		Loc:             "Local",
		MaxIdleConns:    30,
		MaxOpenConns:    100,
		ConnMaxLifetime: uint8(time.Hour.Hours() * 2),
		LogLevel:        uint8(logger.Error),
	}
	a.Redis = Redis{
		Username:     "default",
		Password:     "",
		Addr:         "127.0.0.1:6379",
		DB:           0,
		PoolSize:     15,
		DialTimeout:  15,
		ReadTimeout:  15,
		WriteTimeout: 15,
		IdleTimeout:  15,
		MaxIdleConns: 100,
		MinIdleConns: 10,
	}
	a.Jwt = Jwt{
		Issuer:     "example.com",
		Subject:    "user@example.com",
		Audience:   []string{"example.com"},
		PrivateKey: "o98%C!YJHky=^_ZGRocrf",
	}
	a.Environment = gin.ReleaseMode
	a.IMPort = 8081
	a.ServerHttpPort = 8080
	a.ServerTcpAddr = "127.0.0.1:8085"
	a.LogLevel = int8(zapcore.ErrorLevel)
	a.RetriesTimes = 30

	return a
}

func (a *AppCfg) YamlToBytes() []byte {
	var byteData, _ = yaml.Marshal(a)

	return byteData
}

func (a *AppCfg) validate() error {
	if a.LogLevel < -1 || a.LogLevel > 5 {
		return errors.New(fmt.Sprintf("日志(log_level)等级: -1 - 5"))
	}

	var debug, release, test = gin.DebugMode, gin.ReleaseMode, gin.TestMode
	if a.Environment != debug && a.Environment != release && a.Environment != test {
		return errors.New(fmt.Sprintf("运行环境(%s | %s | %s)配置错误", debug, release, test))
	}

	if logger.LogLevel(a.Mysql.LogLevel) < logger.Silent || logger.LogLevel(a.Mysql.LogLevel) > logger.Info {
		return errors.New(fmt.Sprintf("sql日志(%d - %d)等级配置错误", logger.Silent, logger.Info))
	}

	if a.Jwt.PrivateKey == "" {
		return errors.New("jwt private_key 必须填写")
	}

	if a.Oss.Switch {
		if a.Oss.Prefix == "" {
			return errors.New("oss.prefix 必须填写")
		}

		if a.Oss.TxtName == "" {
			return errors.New("下载依赖文件名称必须填写")
		}
	}

	return nil
}

func LoadConfig() error {
	var workdir, _ = os.Getwd()
	var cfgFileName = fmt.Sprintf("%s/%s.yaml", workdir, filepath.Base(os.Args[0]))
	if _, err := os.Stat(cfgFileName); os.IsNotExist(err) {
		var envFileName = ".example.yaml"
		var envFile, err01 = os.OpenFile(envFileName, os.O_RDWR|os.O_CREATE, 0644)
		if err01 != nil {
			return err01
		}
		defer envFile.Close()

		if info, _ := envFile.Stat(); info.Size() == 0 {
			var writer = bufio.NewWriter(envFile)
			if _, err = writer.Write(cfgSec.Default().YamlToBytes()); err != nil {
				return err
			}

			_ = writer.Flush()
			if err = envFile.Sync(); err != nil {
				return err
			}

			if _, err = envFile.Seek(0, 0); err != nil {
				return err
			}
		}

		var cfgFile, err02 = os.OpenFile(cfgFileName, os.O_WRONLY|os.O_CREATE, 0644)
		if err02 != nil {
			return err02
		}
		defer cfgFile.Close()

		if _, err = io.Copy(cfgFile, envFile); err != nil {
			return err
		}

		fmt.Println(fmt.Sprintf("--- [%s] 请填写相关配置 ---", cfgFileName))
		os.Exit(1)
	}

	var cfgFile, err03 = os.Open(cfgFileName)
	if err03 != nil {
		return err03
	}

	var reader = bufio.NewReader(cfgFile)
	var byteData, err = reader.ReadBytes('\x00')
	_ = cfgFile.Close()
	if err != nil && err != io.EOF {
		return err
	}
	if err = yaml.Unmarshal(byteData, &cfgSec); err != nil {
		return err
	}

	return cfgSec.validate()
}
