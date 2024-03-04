package cfg

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type CustomLogger struct {
	filePath   string
	currName   string
	logFile    *os.File
	errLogFile *os.File
	level      int8
}

func (l *CustomLogger) Printf(_ string, i ...interface{}) {
	var fileLine = strings.Split(i[0].(string), "/")
	if info, ok := i[len(i)-1].(string); ok {
		zap.L().Info("", zap.String(fileLine[len(fileLine)-1], info))
	}
}

func NewLogger(level int8) *CustomLogger {
	var _, file = filepath.Split(os.Args[0])
	var dir, _ = os.Getwd()

	var filePath = fmt.Sprintf("%s/logs/%s/", dir, file)
	_ = os.MkdirAll(filePath, os.ModePerm)

	var log = &CustomLogger{filePath: filePath, level: level}
	log.NewLogFile()
	zap.ReplaceGlobals(log.StartLogger(log.TextFormat()))

	return log
}

// CleanLogFiles 删除该目录下3天前 *.log 文件
func CleanLogFiles(directory string) error {
	if directory == "" {
		var dir, err = os.Getwd()
		if err != nil {
			return err
		}

		directory = filepath.Join(dir, "logs")
	}
	var timeAgo = time.Now().AddDate(0, 0, -3)
	var files, err = os.ReadDir(directory)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var info, err01 = file.Info()
		if err01 != nil {
			continue
		}

		if filepath.Ext(file.Name()) != ".log" {
			continue
		}
		if info.ModTime().Before(timeAgo) {
			if err = os.Remove(directory + "/" + file.Name()); err != nil {
				fmt.Println(fmt.Sprintf("删除文件失败：%s，错误：%+v", file.Name(), err))
			}
		}
	}

	return err
}

func (l *CustomLogger) NewLogFile() {
	var timeNow = time.Now().Format(time.DateOnly)
	if l.currName == timeNow {
		return
	}

	var filename = fmt.Sprintf("%s/%s.log", l.filePath, timeNow)
	var file *os.File
	var errFile *os.File
	var err error
	file, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("创建日志文件失败： %+v", err)
		return
	}

	errFile, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("创建日志文件失败： %+v", err)
		return
	}

	_ = zap.L().Sync()
	_ = l.logFile.Close()
	_ = l.errLogFile.Close()
	l.currName = timeNow
	l.logFile = file
	l.errLogFile = errFile
	gin.DefaultWriter = l.logFile
	gin.DefaultErrorWriter = l.errLogFile

	go CleanLogFiles(l.filePath)
}

func (l CustomLogger) Write(p []byte) (n int, err error) {
	l.NewLogFile()

	return len(p), nil
}

func (l *CustomLogger) format() map[string]zapcore.EncoderConfig {
	var caller = func(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
		//enc.AppendString(caller.FullPath())
		enc.AppendString(path.Base(caller.FullPath()))
	}
	// file 文件输出格式
	var fileCfg = zapcore.EncoderConfig{
		MessageKey:   "msg",
		LevelKey:     "lv",
		TimeKey:      "ts",
		CallerKey:    "fs",
		EncodeCaller: caller,
		EncodeLevel:  zapcore.CapitalLevelEncoder,
		EncodeTime:   zapcore.TimeEncoderOfLayout(time.TimeOnly),
	}

	// cmd 控制台输出格式
	var stdinCfg = zapcore.EncoderConfig{
		TimeKey:      "ts",
		CallerKey:    "fs",
		MessageKey:   "msg",
		EncodeCaller: caller,
		EncodeLevel:  zapcore.CapitalColorLevelEncoder,
		EncodeTime:   zapcore.TimeEncoderOfLayout(time.Kitchen),
	}

	var value = map[string]zapcore.EncoderConfig{
		"file": fileCfg,
	}
	if l.level-1 < -1 {
		value["std"] = stdinCfg
	}

	return value
}

func (l *CustomLogger) JsonFormat() map[string]zapcore.Encoder {
	var encodeMap = make(map[string]zapcore.Encoder, 0)
	for key, encoder := range l.format() {
		switch key {
		case "file":
			encodeMap[key] = zapcore.NewJSONEncoder(encoder)
		case "std":
			encodeMap[key] = zapcore.NewConsoleEncoder(encoder)
		default:
			encodeMap[key] = zapcore.NewJSONEncoder(encoder)

		}
	}

	return encodeMap
}

func (l *CustomLogger) TextFormat() map[string]zapcore.Encoder {
	var encodeMap = make(map[string]zapcore.Encoder, 0)
	for key, encoder := range l.format() {
		encodeMap[key] = zapcore.NewConsoleEncoder(encoder)
	}

	return encodeMap
}

func (l *CustomLogger) StartLogger(config map[string]zapcore.Encoder) *zap.Logger {
	var cores = make([]zapcore.Core, 0, len(config))
	var level = Val(func(cfg *AppCfg) any {
		return zapcore.Level(cfg.LogLevel)
	}).(zapcore.Level)

	for key, encoder := range config {
		switch key {
		case "file":
			cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(l.logFile), level))
			cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(l.errLogFile), zapcore.ErrorLevel))
		case "std":
			cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(zapcore.AddSync(os.Stdout)), level))
		}
	}

	return zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
}

// CloseFile 关闭文件句柄
func (l *CustomLogger) CloseFile() {
	_ = zap.L().Sync()
	_ = l.logFile.Close()
	_ = l.errLogFile.Close()
}
