package middleware

import (
	"fringe/cfg"
	"github.com/gin-gonic/gin"
	"net/http"
)

func LoginMiddleware() func(ctx *gin.Context) {
	return func(ctx *gin.Context) {
		var token, err = cfg.ParseJwtToken(ctx.GetHeader("Authorization"))
		if err != nil {
			ctx.JSON(http.StatusOK, gin.H{
				"code":    http.StatusUnauthorized,
				"message": err.Error(),
			})
			ctx.Abort()
			return
		}

		ctx.Set("user", token)
		ctx.Next()
	}
}
