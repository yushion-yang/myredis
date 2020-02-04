package main

import (
	"github.com/go-redis/redis"
	"redis-learn/core"
)

var redisCli *redis.Client

//初始化连接
func init() {
	redisCli = core.InitRedis("127.0.0.1:6379", "", 0)
}

func main() {
	core.ClearAllKeys(redisCli)
}
