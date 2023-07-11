package main

import (
	"fringe/cfg"
	"fringe/im/router"
	"fringe/middleware"
	"fringe/netsrv"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"os"
	"strconv"
)

func main() {
	cfg.LoggerDefault()

	var err error
	var tcpConn *netsrv.Manager
	var port uint16

	var ipAddrRouter = cfg.Val(func(cfg *cfg.AppCfg) interface{} {
		gin.SetMode(cfg.Environment)
		port = cfg.IMPort

		return map[string]netsrv.InitRouter{
			strconv.Itoa(int(cfg.IMPort)): router.NewWsRouter(),
			cfg.ServerTcpAddr:             router.NewTcpRouter(),
		}
	}).(map[string]netsrv.InitRouter)

	tcpConn, err = netsrv.GetManager().AddClient(ipAddrRouter)
	defer tcpConn.ClientClosed()
	if err != nil {
		cfg.Log.Error(err.Error())
		os.Exit(1)
	}

	if err = cfg.NewDatabase(); err != nil {
		cfg.Log.Error("db connect failed", zap.Error(err))
		os.Exit(1)
	}
	if err = cfg.NewRedisClient(); err != nil {
		cfg.Log.Error("redis connect failed", zap.Error(err))
		os.Exit(1)
	}
	defer cfg.ClosedRedis()

	var r = gin.Default()
	r.Use(middleware.LoginMiddleware())
	r.GET("/ws", router.WsUpGrader)

	if err = r.Run(":" + strconv.Itoa(int(port))); err != nil {
		cfg.Log.Fatal(err.Error())
	}
}
