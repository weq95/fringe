package cfg

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

var client *redis.Client

func GetRedis() *redis.Client {
	return client
}

func ClosedRedis() {
	_ = client.Conn().Close()
}

func NewRedisClient() error {
	client = Val(func(cfg *AppCfg) interface{} {
		return redis.NewClient(&redis.Options{
			Username:        cfg.Redis.Username,
			Password:        cfg.Redis.Password,
			Addr:            cfg.Redis.Addr,
			DB:              int(cfg.Redis.DB),
			PoolSize:        int(cfg.Redis.PoolSize),
			DialTimeout:     time.Duration(cfg.DialTimeout) * time.Second,
			ReadTimeout:     time.Duration(cfg.ReadTimeout) * time.Second,
			WriteTimeout:    time.Duration(cfg.WriteTimeout) * time.Second,
			ConnMaxIdleTime: time.Duration(cfg.IdleTimeout) * time.Minute,
			MaxIdleConns:    int(cfg.Redis.MaxIdleConns),
			MinIdleConns:    int(cfg.Redis.MinIdleConns),
		})

	}).(*redis.Client)

	return client.Ping(context.TODO()).Err()
}
