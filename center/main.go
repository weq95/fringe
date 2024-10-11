package main

import (
	"context"
	"errors"
	"fmt"
	"fringe/center/business"
	"fringe/center/router"
	"fringe/cfg"
	"fringe/netsrv"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"net/http"
	"strconv"
	"time"
)

type DeferManager struct {
	fns []func()
}

func main() {
	var tcpSrvAddr string
	var port string
	var logLevel int8
	var df = DeferManager{
		fns: make([]func(), 0, 10),
	}
	defer func() {
		for i := len(df.fns) - 1; i >= 0; i-- {
			if df.fns[i] != nil {
				df.fns[i]()
			}
		}
	}()
	_ = cfg.Val(func(cfg *cfg.AppCfg) interface{} {
		tcpSrvAddr = cfg.ServerTcpAddr
		port = ":" + strconv.Itoa(int(cfg.ServerHttpPort))
		logLevel = cfg.LogLevel
		gin.SetMode(cfg.Environment)

		return nil
	})
	var logger = cfg.NewLogger(logLevel)
	df.fns = append(df.fns, func() {
		_ = zap.L().Sync()
	})
	var m, err = netsrv.GetManager().AddServer(
		map[string]netsrv.TcpClients{
			tcpSrvAddr: router.NewSrvRouter(),
		})
	if err != nil {
		zap.L().Error(err.Error())
		return
	}
	df.fns = append(df.fns, func() {
		m.ServerClosed()
	})

	if err = cfg.NewDatabase(logger); err != nil {
		zap.L().Error(err.Error())
		return
	}
	if err = cfg.NewRedisClient(); err != nil {
		zap.L().Error(err.Error())
		return
	}
	df.fns = append(df.fns, func() {
		cfg.ClosedRedis()
	})

	business.SyncData()
	var r = gin.New()
	r.Use(
		func(c *gin.Context) {
			method := c.Request.Method
			origin := c.Request.Header.Get("Origin")
			if origin != "" {
				c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
				c.Writer.Header().Set("Access-Control-Allow-Methods", "POST,GET,OPTIONS,PUT,DELETE,UPDATE")
				c.Writer.Header().Set("Access-Control-Allow-Headers", "Authorization,content-type,Content-Length,X-CSRF-Token,Token,session,Access-Control-Allow-Headers,account")
				c.Writer.Header().Set("Access-Control-Expose-Headers", "Content-Length,Access-Control-Allow-Origin,Access-Control-Allow-Headers")
				c.Writer.Header().Set("Access-Control-Max-Age", "172800")
				c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
				c.Writer.Header().Set("Content-Type", "application/json")
			}

			if method == "OPTIONS" {
				c.JSON(http.StatusNoContent, nil)
				return
			}

			c.Next()
		},
		cfg.ResponseLogger(),
		cfg.RequestLogger(),
		gin.CustomRecoveryWithWriter(gin.DefaultErrorWriter, func(c *gin.Context, err any) {
			if err == nil {
				return
			}

			c.Abort()
			c.JSON(http.StatusOK, gin.H{"code": http.StatusInternalServerError, "message": "server error"})
		}),
	)
	r.POST("/api/login", business.Login)

	var srv = &http.Server{
		Addr:    port,
		Handler: r,
	}
	go func() {
		zap.L().Info("程序 [Center] 已启动...")
		if err = srv.ListenAndServe(); err != nil && !errors.Is(http.ErrServerClosed, err) {
			zap.L().Fatal(fmt.Sprintf("listen：%+v", err))
		}
	}()

	<-cfg.WaitCtrlC()
	zap.L().Info("Shutdown Server ...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	df.fns = append(df.fns, func() {
		cancel()
	})

	if err = srv.Shutdown(ctx); err != nil {
		zap.L().Fatal(fmt.Sprintf("Server Shutdown: %v", err))
	}
	zap.L().Info("Server exiting")
}
