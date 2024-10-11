package cfg

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type GinLog struct {
	*CustomLogger
}

func (g *GinLog) Write(b []byte) (int, error) {
	g.NewLogFile(false)

	return g.logFile.Write(b)
}

type CustomLogger struct {
	filePath   string
	currName   string
	logFile    *os.File
	errLogFile *os.File
	level      zapcore.Level
	zapcore.Encoder
}

func (l *CustomLogger) With(fields []zapcore.Field) zapcore.Core {
	var clone = &CustomLogger{
		filePath:   l.filePath,
		currName:   l.currName,
		logFile:    l.logFile,
		errLogFile: l.errLogFile,
		level:      l.level,
		Encoder:    l.Encoder,
	}
	for i := range fields {
		fields[i].AddTo(clone.Encoder)
	}

	return clone
}

func (l *CustomLogger) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if l.Enabled(ent.Level) {
		return ce.AddCore(ent, l)
	}
	return ce
}

func (l *CustomLogger) Printf(_ string, i ...interface{}) {
	var fileLine = strings.Split(i[0].(string), "/")
	if info, ok := i[len(i)-1].(string); ok {
		zap.L().Info("", zap.String(fileLine[len(fileLine)-1], info))
	}
}

func NewLogger(level int8) *CustomLogger {
	var dir, _ = os.Getwd()

	var filePath = fmt.Sprintf("%s/logs/%s/", dir, filepath.Base(os.Args[0]))
	_ = os.MkdirAll(filePath, os.ModePerm)

	var log = &CustomLogger{filePath: filePath, level: zapcore.Level(level)}
	log.NewLogFile(true)
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

func (l *CustomLogger) NewLogFile(startApp bool) {
	var timeNow = time.Now().Format(time.DateOnly)
	if l.currName == timeNow {
		return
	}

	var filesCreateFn = func(filepath, ymd string) ([]*os.File, error) {
		var flag = os.O_APPEND | os.O_CREATE | os.O_WRONLY
		var f1, e1 = os.OpenFile(fmt.Sprintf("%s%s.log", filepath, ymd), flag, 0666)
		if e1 != nil {
			return nil, e1
		}
		var f2, e2 = os.OpenFile(fmt.Sprintf("%s%s_err.log", filepath, ymd), flag, 0666)
		if e2 != nil {
			_ = f1.Close()
			return nil, e2
		}

		return []*os.File{f1, f2}, nil
	}

	var logFiles, err = filesCreateFn(l.filePath, timeNow)
	if err != nil {
		zap.L().Error(err.Error())
		return
	}

	if !startApp {
		l.CloseFile()
	}
	l.currName = timeNow
	l.logFile = logFiles[0]
	l.errLogFile = logFiles[1]

	var ginLog = &GinLog{l}
	gin.DefaultWriter = ginLog
	gin.DefaultErrorWriter = ginLog

	_ = CleanLogFiles(l.filePath)
}

func (l *CustomLogger) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	l.NewLogFile(false)

	var buff *buffer.Buffer
	if buff, hit_err = l.EncodeEntry(ent, fields); hit_err != nil {
		return hit_err
	}
	_, hit_err = l.logFile.Write(buff.Bytes())
	defer buff.Free()
	if hit_err != nil {
		return hit_err
	}
	if ent.Level >= zapcore.ErrorLevel {
		_ = l.Sync()
		_, hit_err = l.errLogFile.Write(buff.Bytes())
	}
	return hit_err
}

func (l CustomLogger) Sync() error {
	return nil
}

func (l CustomLogger) Enabled(level zapcore.Level) bool {
	return l.level <= level
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
		EncodeTime:   zapcore.TimeEncoderOfLayout(time.TimeOnly + "000000"),
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
	if l.level < 0 {
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

	for key, encoder := range config {
		switch key {
		case "file":
			l.Encoder = encoder
			cores = append(cores, l)
		case "std":
			cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), l.level))
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

type responseBodyWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (r responseBodyWriter) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.ContentType() == binding.MIMEMultipartPOSTForm {
			c.Next()
			return
		}
		var statrTime = time.Now()
		if c.ContentType() != binding.MIMEJSON {
			_ = c.Request.ParseForm()
		}

		var content string
		if c.Request.ContentLength > 0 {
			var buf = new(bytes.Buffer)
			_, _ = buf.ReadFrom(c.Request.Body)
			content = buf.String()
			c.Request.Body = io.NopCloser(buf)
		}

		zap.L().Info("HttpRequest",
			zap.String("url", c.Request.URL.String()),
			zap.String("ip", c.ClientIP()),
			zap.String("method", c.Request.Method),
			zap.String("latency", fmt.Sprintf("%v", time.Since(statrTime))),
			zap.Any("header", c.Request.Header),
			zap.String("from", c.Request.Form.Encode()),
			zap.String("body", content),
		)
	}
}

func ResponseLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.ContentType() == binding.MIMEMultipartPOSTForm {
			c.Next()
			return
		}
		var w = &responseBodyWriter{
			body:           new(bytes.Buffer),
			ResponseWriter: c.Writer,
		}
		c.Writer = w

		c.Next()

		zap.L().Info("HttpResponse",
			zap.String("url", c.Request.URL.String()),
			zap.Int("status", c.Writer.Status()),
			zap.Any("header", c.Writer.Header()),
			zap.String("content", w.body.String()),
		)
	}
}
