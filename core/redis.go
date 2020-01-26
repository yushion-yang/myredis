package core

import (
	"errors"
	"github.com/go-redis/redis"
)

func InitRedis(ip string, pw string, db int) (qr *redis.Client) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     ip,
		Password: pw, // no password set
		DB:       db, // use default DB
	})
	pong, err := redisClient.Ping().Result()
	if pong != "PONG" || err != nil {
		panic(errors.New("Redis is close!!!"))
	}
	return redisClient
}

//清除所有的key值 避免影响下次测试
func ClearAllKeys(redisCli *redis.Client) {
	{
		ret, _ := redisCli.Keys("*").Result()
		redisCli.Del(ret...)
	}
}
