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

//采用分块http可以实现生成并发送增量式数据 websocket可以实现服务器推送（更佳）
//当客户端进行轻轻请求时 创建一个流式API然后处理监听 其他模块执行时都发布事件到制定频道 该流式API订阅此频道来获取消息
//然后执行过滤再返回给客户端

func main() {
	ctx := context.Background()
	core.ClearAllKeys(ctx, redisCli)
}
