package core

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

func InitRedis(ctx context.Context, ip string, pw string, db int) (qr *redis.Client) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     ip,
		Password: pw, // no password set
		DB:       db, // use default DB
	})
	pong, err := redisClient.Ping(ctx).Result()
	if pong != "PONG" || err != nil {
		panic(errors.New("Redis is close!!!"))
	}
	return redisClient
}

//清除所有的key值 避免影响下次测试
func ClearAllKeys(ctx context.Context, redisCli *redis.Client) {
	{
		ret, _ := redisCli.Keys(ctx, "*").Result()
		redisCli.Del(ctx, ret...)
	}
}
