package main

import (
	"context"
	"errors"
	"fmt"
	"fringe/center/business"
	"fringe/center/router"
	"fringe/cfg"
	"fringe/middleware"
	"fringe/netsrv"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"net/http"
	"strconv"
	"time"
)

func main() {
	var tcpSrvAddr string
	var port string
	var logLevel int8
	_ = cfg.Val(func(cfg *cfg.AppCfg) interface{} {
		tcpSrvAddr = cfg.ServerTcpAddr
		port = ":" + strconv.Itoa(int(cfg.ServerHttpPort))
		logLevel = cfg.LogLevel
		gin.SetMode(cfg.Environment)

		return nil
	})
	_ = cfg.NewLogger(logLevel)
	defer func() {
		_ = zap.L().Sync()
	}()
	var m, err = netsrv.GetManager().AddServer(
		map[string]netsrv.TcpClients{
			tcpSrvAddr: router.NewSrvRouter(),
		})
	if err != nil {
		zap.L().Error(err.Error())
		return
	}
	defer m.ServerClosed()

	if err = cfg.NewDatabase(); err != nil {
		zap.L().Error(err.Error())
		return
	}
	if err = cfg.NewRedisClient(); err != nil {
		zap.L().Error(err.Error())
		return
	}
	defer cfg.ClosedRedis()

	business.SyncData()
	var r = gin.New()
	r.Use(
		gin.Logger(),
		func(c *gin.Context) {
			method := c.Request.Method
			origin := c.Request.Header.Get("Origin")
			if origin != "" {
				c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
				c.Header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE,UPDATE")
				c.Header("Access-Control-Allow-Headers", "Authorization, content-type, Content-Length, X-CSRF-Token, Token,session,Access-Control-Allow-Headers,account")
				c.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers")
				c.Header("Access-Control-Max-Age", "172800")
				c.Header("Access-Control-Allow-Credentials", "true")
				c.Set("Content-Type", "application/json")
			}

			if method == "OPTIONS" {
				c.JSON(http.StatusNoContent, nil)
				return
			}

			c.Next()
		},
		func(c *gin.Context) {
			var startTime = time.Now()
			c.Next()
			var duration = time.Now().Sub(startTime)
			if duration > time.Second*1 {
				zap.L().Warn("[HttpWeb] 慢请求",
					zap.String("url", c.Request.URL.String()),
					zap.String("method", c.Request.Method),
				)
			}
		},
		gin.CustomRecoveryWithWriter(gin.DefaultErrorWriter, func(c *gin.Context, err any) {
			if err != nil {
				return
			}
			
			c.Abort()
			c.JSON(http.StatusOK, gin.H{"code": http.StatusInternalServerError, "message": "server error"})
		}),
	)
	r.POST("/login", business.Login)
	r.Use(middleware.LoginMiddleware())

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
	defer cancel()

	if err = srv.Shutdown(ctx); err != nil {
		zap.L().Fatal(fmt.Sprintf("Server Shutdown: %v", err))
	}
	zap.L().Info("Server exiting")
}
