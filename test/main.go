package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"redis-learn/core"
)

var redisCli *redis.Client

//初始化连接
func init() {
	ctx := context.Background()
	redisCli = core.InitRedis(ctx, "127.0.0.1:6379", "", 0)
}

var key = "test_key"

//测试事务的结果的获取
func ExampleClient_Watch(conn *redis.Client) error {
	ctx := context.Background()
	conn.Set(ctx, key, 100, 0)
	var cmd *redis.StringCmd
	var ret int64
	txf := func(tx *redis.Tx) error {
		// get current value or zero
		n, err := tx.Get(ctx, key).Int()
		if err != nil && err != redis.Nil {
			return err
		}
		// actual opperation (local in optimistic lock)
		n++
		// runs only if the watched keys remain unchanged
		_, err2 := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			// pipe handles the error case
			cmd = pipe.Get(ctx, key)
			return nil
		})
		return err2
	}
	err := conn.Watch(ctx, txf, key)
	if err != redis.TxFailedErr {
		fmt.Println("err != redis.TxFailedErr err:", err)
		ret, _ = cmd.Int64()
		fmt.Println("ret:", ret)
		return err
	} else {
		fmt.Println("err == redis.TxFailedErr")
	}
	return nil
}

func main() {
	ctx := context.Background()
	ExampleClient_Watch(redisCli)
	core.ClearAllKeys(ctx, redisCli)
}
