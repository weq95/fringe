package main

import (
	"fringe/cfg"
	"go.uber.org/zap"
)

func main() {
	var log = cfg.NewLogger(-1)
	defer log.CloseFile()

	zap.L().Debug("这是Debug调试信息...")
	zap.L().Warn("这是Warn调试信息...")
	zap.L().Error("这是Error调试信息...")
	zap.L().DPanic("这是Panic调试信息...")
	var h = new(cfg.MyHandler)
	h.StartApp()
}
