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
	filePath string
	currName string
	logFile  *os.File
	level    int8
}

func (l *CustomLogger) Printf(_ string, i ...interface{}) {
	var fileLine = strings.Split(i[0].(string), "/")
	if info, ok := i[len(i)-1].(string); ok {
		zap.L().Info("", zap.String(fileLine[len(fileLine)-1], info))
	}
}

func NewLogger(level int8) {
	var _, file = filepath.Split(os.Args[0])
	var dir, _ = os.Getwd()

	var filePath = fmt.Sprintf("%s/logs/%s/", dir, file)
	_ = os.MkdirAll(filePath, os.ModePerm)

	var log = &CustomLogger{filePath: filePath, level: level}
	log.NewLogFile()
}

func (l *CustomLogger) NewLogFile() {
	var timeNow = time.Now().Format(time.DateOnly)
	if l.currName == timeNow {
		return
	}

	_ = zap.L().Sync()
	_ = l.logFile.Close()
	var filename = fmt.Sprintf("%s/%s.log", l.filePath, timeNow)
	var file, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("创建日志文件失败： %+v", err)
		return
	}

	l.currName = timeNow
	l.logFile = file
	gin.DefaultWriter = l
	gin.DefaultErrorWriter = l

	zap.ReplaceGlobals(l.StartLogger(l.TextFormat()))

}

func (l CustomLogger) Write(p []byte) (n int, err error) {
	l.NewLogFile()

	return l.logFile.Write(p)
}

func (l *CustomLogger) format() map[string]zapcore.EncoderConfig {
	var caller = func(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
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
		EncodeLevel:  zapcore.CapitalLevelEncoder,
		EncodeTime:   zapcore.TimeEncoderOfLayout(time.Kitchen),
	}

	return map[string]zapcore.EncoderConfig{
		"file": fileCfg,
		"std":  stdinCfg,
	}
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
		case "std":
			cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(zapcore.AddSync(os.Stdout)), level))
		}
	}

	return zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
}
