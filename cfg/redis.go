package cfg

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	r2 "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
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


// RedisTxFn 事务： 业务超时时间10秒
func RedisTxFn(key string, fn func(ctx context.Context, tx r2.Pipeliner) (interface{}, error)) (interface{}, error) {
	var redisClusterClient = GetRedis()
	var c = redisClusterClient
	var lockTimeout = time.Second * 10
	var ctx, cancel = context.WithTimeout(context.Background(), lockTimeout)
	defer cancel()
	var err error
	var lockVal int64

	var lockKey = fmt.Sprintf("amotic_lock:%s", key)
	if lockVal, err = c.Exists(ctx, lockKey).Result(); err != nil {
		return nil, err
	}
	if lockVal > 0 {
		return nil, errors.New("lock is already held")
	}
	var retriesNum int
ForStop:
	for {
		lockVal, err = c.Eval(ctx, `
		if redis.call("EXISTS", KEYS[1]) == 0 then
            redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", tonumber(ARGV[2]))
            return 1
        end
        return 0`, []string{lockKey}, uuid.New().String(), lockTimeout.Seconds()).Int64()
		if err != nil {
			return nil, err
		}
		if lockVal > 0 {
			break ForStop
		}

		if retriesNum >= 5 && lockVal <= 0 {
			return nil, errors.New("failed to acquire lock")
		}
		retriesNum++
		zap.L().Warn("redis锁重试： ",
			zap.String("key", key),
			zap.Int("retries_num", retriesNum),
		)
		time.Sleep(time.Millisecond * 100)
	}
	defer func(lockKey string) {
		//释放锁
		if _, err = c.Del(ctx, lockKey).Result(); err != nil {
			zap.L().Error(err.Error())
		}
	}(lockKey)

	var tx = c.TxPipeline()
	var result any
	if result, err = fn(ctx, tx); err != nil {
		tx.Discard()
		return nil, err
	}

	_, err = tx.Exec(ctx)
	return result, err
}

func RedisTxFnBak(key string, fn func(ctx context.Context, tx r2.Pipeliner) (interface{}, error)) (interface{}, error) {
	var c = GetRedis()
	var lockTimeout = time.Second * 30
	var ctx, cancel = context.WithTimeout(context.Background(), lockTimeout)
	defer cancel()
	var err error
	var lockVal int64

	var lockKey = fmt.Sprintf("amotic_lock:%s", key)
	if lockVal, err = c.Exists(ctx, lockKey).Result(); err != nil {
		return nil, err
	}
	if lockVal > 0 {
		return nil, errors.New("lock is already held")
	}
	var retriesNum int
	var luaLock = `
local key = KEYS[1]
local requestId = KEYS[2]
local ttl = tonumber(KEYS[3])
local slot = redis.call("CLUSTER_KEYSLOT", key)
local node = redis.call("CLUSTER_NODES", slot)[1]

local result = redis.call("setnx", key, requestId)
if result == 1 then
    redis.call("pexpire", key, ttl)
else
    result = -1
    local value = redis.call("get", key)
    if value == requestId then
        result = 1
        redis.call("pexpire", key, ttl)
    end
end

return result
`
	var luaUnlock = `
local key = KEYS[1]
local requestId = KEYS[2]
local slot = redis.call("CLUSTER_KEYSLOT", key)
local node = redis.call("CLUSTER_NODES", slot)[1]

local value = redis.call("get", key)
if value == requestId then
    redis.call("del", key)
    return 1
end

return -1

`
	var script = []string{lockKey, uuid.New().String()}
ForStop:
	for {
		lockVal, err = c.Eval(ctx, luaLock, script, lockTimeout.Seconds()).Int64()
		if err != nil {
			return nil, err
		}
		if lockVal > 0 {
			break ForStop
		}

		if retriesNum >= 5 && lockVal <= 0 {
			return nil, errors.New("failed to acquire lock")
		}
		retriesNum++
		zap.L().Warn("redis锁重试： ",
			zap.String("key", key),
			zap.Int("retries_num", retriesNum),
		)
		time.Sleep(time.Millisecond * 100)
	}

	var tx = c.TxPipeline()
	defer func(l string) {
		//释放锁
		if err = c.Eval(ctx, luaUnlock, script).Err(); err != nil {
			zap.L().Error(err.Error())
		}
	}(lockKey)

	//自动续期
	go func(script []string) {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(lockTimeout / 2):
				_, err = c.Eval(ctx, luaLock, script, lockTimeout.Seconds()/2).Int64()
				if err != nil {
					zap.L().Error(fmt.Sprintf("redis lock auto renew err: %s", err.Error()))
				}
			}
		}
	}(script)

	var result any
	if result, err = fn(ctx, tx); err != nil {
		tx.Discard()
		return nil, err
	}

	_, err = tx.Exec(ctx)
	return result, err
}
