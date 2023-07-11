package cfg

import (
	"errors"
	"github.com/golang-jwt/jwt/v5"
	"time"
)

type CustomClaims struct {
	Userid   uint64 `json:"userid"`
	Client   string `json:"client"`
	Platform string `json:"platform"`
	ServeIp  string `json:"serve_ip"`
	jwt.RegisteredClaims
}

func ParseJwtToken(tokenStr string) (*CustomClaims, error) {
	var token, err = jwt.ParseWithClaims(tokenStr, &CustomClaims{},
		func(token *jwt.Token) (interface{}, error) {
			return Val(func(cfg *AppCfg) interface{} {
				return []byte(cfg.Jwt.PrivateKey)
			}).([]byte), nil
		})

	if err != nil {
		return nil, err
	}

	var claims, ok = token.Claims.(*CustomClaims)
	if ok && token.Valid {
		return claims, nil
	}

	if errors.Is(err, jwt.ErrTokenExpired) || errors.Is(err, jwt.ErrTokenNotValidYet) {
		return nil, errors.New("token has expired or has not been activated")
	}

	return nil, errors.New("invalid token")
}

func NewJwtToken(userid uint64, client, platform, ip string) (string, error) {
	var claims = &CustomClaims{
		Userid:   userid,
		Client:   client,
		Platform: platform,
		ServeIp:  ip,
	}

	var key []byte
	_ = Val(func(cfg *AppCfg) interface{} {
		claims.Issuer = cfg.Jwt.Issuer
		claims.Subject = cfg.Jwt.Subject
		claims.Audience = cfg.Jwt.Audience
		key = []byte(cfg.Jwt.PrivateKey)
		return nil
	})

	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Minute * 60))

	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
}
