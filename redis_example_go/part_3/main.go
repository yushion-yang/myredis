package main

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"math"
	"redis-learn/core"
	"strings"
	"sync"
	"time"
)

var redisCli *redis.Client

//初始化连接
func init() {
	redisCli = core.InitRedis("127.0.0.1:6379", "", 0)
}

const ONE_WEEK_IN_SECONDS = 7 * 86400
const VOTE_SCORE = 432
const ARTICLES_PER_PAGE = 25

func Ex3_1() {
	{
		ret := redisCli.Get("key").Val() //尝试获取一个不存在的键将得到一个None值，终端不会显示这个值。
		fmt.Println("ret:", ret)
	}
	{
		ret := redisCli.Incr("key").Val() //我们既可以对不存在的键执行自增操作	也可以通过可选的参数来指定自增操作的增量。
		fmt.Println("ret:", ret)
	}
	{
		ret := redisCli.IncrBy("key", 15).Val()
		fmt.Println("ret:", ret)
	}
	{
		ret := redisCli.Decr("key").Val() //和自增操作一样，	执行自减操作的函数也可以通过可选的参数来指定减量。
		fmt.Println("ret:", ret)
	}
	{
		ret := redisCli.Get("key").Val() //在尝试获取一个键的时候，命令以字符串格式返回被存储的整数。
		fmt.Println("ret:", ret)
	}
	{
		ret := redisCli.Set("key", "13", 0).Val() //即使在设置键时输入的值为字符串，	但只要这个值可以被解释为整数，
		fmt.Println("ret:", ret)
	}
	{
		ret := redisCli.Incr("key").Val() //我们就可以把它当作整数来处理。
		fmt.Println("ret:", ret)
	}
}

func Ex3_2() {
	{
		ret := redisCli.Append("new-string-key", "hello ").Val()
		fmt.Println("ret:", ret)
	}
	//...
}

//...

func update_token(conn *redis.Client, token string, user string, item string) {
	timestamp := float64(time.Now().Unix())
	conn.HSet("login:", token, user)
	conn.ZAdd("recent:", redis.Z{Score: timestamp, Member: token})
	if item != "" {
		key := "viewed:" + token
		// 如果指定的元素存在于列表当中，那么移除它
		conn.LRem(key, 1, item)
		// 将元素推入到列表的右端，使得 ZRANGE 和 LRANGE 可以取得相同的结果
		conn.RPush(key, item)
		// 对列表进行修剪，让它最多只能保存 25 个元素
		conn.LTrim(key, -25, -1)
	}
	conn.ZIncrBy("viewed:", -1, item)
}

func Publisher(data string) {
	time.Sleep(time.Second)
	for {
		err := redisCli.Publish("message", data).Err()
		if err != nil {
			fmt.Println("发布失败")
			return
		}
		time.Sleep(time.Second * 2)
	}
}

func Subscribe() {
	go Publisher("test")
	//参数1 频道名 字符串类型
	pubsub := redisCli.Subscribe("message")
	_, err := pubsub.Receive()
	if err != nil {
		return
	}
	ch := pubsub.Channel()
	count := 0
	for {
		msg, ok := <-ch
		fmt.Println(msg.Channel, msg.Payload)
		if !ok {
			break
		}
		count++
		if count == 4 {
			pubsub.Unsubscribe()
		}
		if count == 5 {
			break
		}
	}
	fmt.Println("end")
}

func ArticleVote(conn *redis.Client, user string, article string) {
	// 在进行投票之前，先检查这篇文章是否仍然处于可投票的时间之内
	cutoff := float64(time.Now().Unix() - ONE_WEEK_IN_SECONDS)
	posted := conn.ZScore("time:", article).Val()
	if posted < cutoff {
		return
	}

	// 从article:id标识符（identifier）里面取出文章的ID。
	article_id := strings.Split(article, ":")[1]
	pipeline := conn.Pipeline()
	pipeline.SAdd("voted:"+article_id, user) //执行前可能被其他客户端修改
	pipeline.Expire("voted:"+article_id, time.Duration(posted-cutoff))
	// 如果用户是第一次为这篇文章投票，那么增加这篇文章的投票数量和评分。
	if _, err := pipeline.Exec(); err == nil {
		pipeline.ZIncrBy("score:", VOTE_SCORE, article)
		pipeline.HIncrBy(article, "votes", 1)
		_, _ = pipeline.Exec()
	}
}

func ArticleVote2(conn *redis.Client, user string, article string) error {
	// 在进行投票之前，先检查这篇文章是否仍然处于可投票的时间之内
	cutoff := float64(time.Now().Unix() - ONE_WEEK_IN_SECONDS)
	posted := conn.ZScore("time:", article).Val()
	article_id := strings.Split(article, ":")[1]
	voted := "voted" + article_id
	pipeline := conn.Pipeline()
	for posted > cutoff {
		// 从article:id标识符（identifier）里面取出文章的ID。
		txf := func(tx *redis.Tx) error {
			if tx.SIsMember(voted, user).Val() {
				_, err := tx.TxPipelined(func(pipe redis.Pipeliner) error {
					pipeline.SAdd("voted:"+article_id, user)
					pipeline.Expire("voted:"+article_id, time.Duration(posted-cutoff))
					pipeline.ZIncrBy("score:", VOTE_SCORE, article)
					pipeline.HIncrBy(article, "votes", 1)
					return nil
				})
				return err
			} else {

			}
			return nil
		}
		err := conn.Watch(txf, voted)
		if err != redis.TxFailedErr {
			return err
		}
		cutoff = float64(time.Now().Unix() - ONE_WEEK_IN_SECONDS)
	}
	return nil
}

func ExampleClient_Watch(conn *redis.Client) {
	const routineCount = 100
	// Transactionally increments key using GET and SET commands.
	increment := func(key string) error {
		txf := func(tx *redis.Tx) error {
			// get current value or zero
			n, err := tx.Get(key).Int()
			if err != nil && err != redis.Nil {
				return err
			}
			// actual opperation (local in optimistic lock)
			n++
			// runs only if the watched keys remain unchanged
			_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
				// pipe handles the error case
				pipe.Set(key, n, 0)
				return nil
			})
			return err
		}
		for retries := routineCount; retries > 0; retries-- {
			err := conn.Watch(txf, key)
			if err != redis.TxFailedErr {
				return err
			}
			// optimistic lock lost
		}
		return errors.New("increment reached maximum number of retries")
	}
	var wg sync.WaitGroup
	wg.Add(routineCount)
	for i := 0; i < routineCount; i++ {
		go func() {
			defer wg.Done()

			if err := increment("counter3"); err != nil {
				fmt.Println("increment error:", err)
			}
		}()
	}
	wg.Wait()

	n, err := conn.Get("counter3").Int()
	fmt.Println("ended with", n, err)
	// Output: ended with 100 <nil>
}

func GetArticles(conn *redis.Client, page int, order string) []interface{} {
	if order == "" {
		order = "score:"
	}
	// 设置获取文章的起始索引和结束索引。
	start := int64(math.Max(float64(page-1), 0) * ARTICLES_PER_PAGE)
	end := start + ARTICLES_PER_PAGE - 1

	// 获取多个文章ID。
	ids := conn.ZRevRange(order, start, end).Val()
	cmds := make([]*redis.StringStringMapCmd, len(ids))
	pipeline := conn.Pipeline()
	for i, id := range ids {
		cmds[i] = pipeline.HGetAll(id)
	}
	articles := make([]interface{}, 0)
	// 根据文章ID获取文章的详细信息。
	for i, v := range cmds {
		v.Val()["id"] = ids[i]
		articles = append(articles, v)
	}
	return articles
}

//列表和散列都无法在操作的同时设置过期时间 所有需要在操作完之后单独调用过期设置函数

func main() {
	//Subscribe()
	ExampleClient_Watch(redisCli)
	core.ClearAllKeys(redisCli)
}
