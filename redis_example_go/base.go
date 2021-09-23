package main

import (
	"context"
	"github.com/go-redis/redis/v8"
	"redis-learn/core"
)

var redisCli *redis.Client

//初始化连接
func init() {
	ctx := context.Background()
	redisCli = core.InitRedis(ctx, "127.0.0.1:6379", "", 0)
}

func main() {
	ctx := context.Background()
	core.ClearAllKeys(ctx, redisCli)
}
