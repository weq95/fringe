package business

import (
	"encoding/json"
	"errors"
	"fmt"
	"fringe/cfg"
	"fringe/netsrv"
	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"net/http"
)

func GetServeIp() (string, error) {
	if srv, ok := netsrv.GetManagerServer(); ok {
		var ips = srv.GetClientIps()
		if len(ips) > 0 {
			var idx = cfg.CryptRandom(int64(len(ips)))
			return ips[idx], nil
		}
	}

	return "", errors.New("没有可用的服务")
}

type LoginRequest struct {
	Userid   uint64 `json:"userid" validate:"required,gte=1"`
	Client   string `json:"client" validate:"required"`
	Platform string `json:"platform" validate:"required"`
}

func Login(ctx *gin.Context) {
	var req LoginRequest
	var token string
	var err error
	if err = ctx.BindJSON(&req); err != nil {
		ctx.JSON(http.StatusOK, gin.H{
			"code": http.StatusBadRequest,
			"msg":  "请求参数错误",
		})
		return
	}

	var validate = validator.New()
	if err = validate.Struct(req); err != nil {
		ctx.JSON(http.StatusOK, gin.H{
			"code": http.StatusBadRequest,
			"msg":  err.Error(),
		})
		return
	}

	var ip string
	ip, err = GetServeIp()
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{
			"code": http.StatusInternalServerError,
			"msg":  err.Error(),
		})
		return
	}

	token, err = cfg.NewJwtToken(req.Userid, req.Client, req.Platform, ip)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{
			"code": http.StatusInternalServerError,
			"msg":  err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"code":  http.StatusOK,
		"token": token,
		"uuid":  req.Userid,
		"ip":    ip,
		"msg":   "success",
	})
}

// Heartbeat 心跳包
func Heartbeat(data []byte, responseWriter func(uint32, []byte)) {
	cfg.Log.Info(fmt.Sprintf("心跳: %s", string(data)))
	responseWriter(1000, []byte(`pong`))
}

func UpdateSrvStatus(data []byte, _ func(uint32, []byte)) {
	cfg.Log.Info(fmt.Sprintf("ws信息: %s", string(data)))
	type ServeInfo struct {
		Ip   string `json:"ip"`
		Port uint16 `json:"port"`
	}
	var info ServeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return
	}

	if srv, ok := netsrv.GetManagerServer(); ok {
		srv.Serviceable(info.Ip, info.Port)
	}
}
