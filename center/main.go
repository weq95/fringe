package main

import (
	"fringe/center/business"
	"fringe/center/router"
	"fringe/cfg"
	"fringe/middleware"
	"fringe/netsrv"
	"github.com/gin-gonic/gin"
	"os"
	"strconv"
)

func main() {
	cfg.LoggerDefault()

	var tcpSrvAddr string
	var port string
	_ = cfg.Val(func(cfg *cfg.AppCfg) interface{} {
		tcpSrvAddr = cfg.ServerTcpAddr
		port = ":" + strconv.Itoa(int(cfg.ServerHttpPort))

		return nil
	})
	var m, err = netsrv.GetManager().AddServer(
		map[string]netsrv.TcpClients{
			tcpSrvAddr: router.NewSrvRouter(),
		})
	if err != nil {
		cfg.Log.Error(err.Error())
		os.Exit(1)
	}
	defer m.ServerClosed()

	if err = cfg.NewDatabase(); err != nil {
		cfg.Log.Error(err.Error())
		os.Exit(1)
	}
	if err = cfg.NewRedisClient(); err != nil {
		cfg.Log.Error(err.Error())
		os.Exit(1)
	}
	defer cfg.ClosedRedis()

	business.SyncData()
	var r = gin.Default()
	r.POST("/login", business.Login)
	r.Use(middleware.LoginMiddleware())
	if err = r.Run(port); err != nil {
		cfg.Log.Fatal(err.Error())
	}
}
