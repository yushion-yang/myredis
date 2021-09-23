package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"math"
	"net/url"
	"redis-learn/core"
	"strings"
	"time"
)

var redisCli *redis.Client

//初始化连接
func init() {
	ctx := context.Background()
	redisCli = core.InitRedis(ctx, "127.0.0.1:6379", "", 0)
}

type Login struct {
	Tokens map[string]string
}

type Recent struct {
	TokenTimestamp map[string]float64 `sorted:"1"`
}

type Viewed_Token struct {
	ItemTimestamp map[string]bool `sorted:"0"`
}

type Cart_Session struct {
	Items map[string]int
}

type Cach_Request struct {
	Content string
}

type Delay struct {
	RowIdDelay map[string]float64 `sorted:"1"`
}

type Schedule struct {
	RowIdTimestamp map[string]float64 `sorted:"1"`
}

type InvRowId struct {
	Row string
}

//	2-1 获取令牌对应的用户
func check_token(conn *redis.Client, token string) string {
	ctx := context.Background()
	return conn.HGet(ctx, "login:", token).Val() // 尝试获取并返回令牌对应的用户。
}

//  2-2 更新玩家的令牌 如果有新物品则添加处理
func update_token1(conn *redis.Client, token string, user string, item string) {
	ctx := context.Background()
	// 获取当前时间戳。
	timestamp := float64(time.Now().Unix())
	// 维持令牌与已登录用户之间的映射。
	conn.HSet(ctx, "login:", token, user)
	// 记录令牌最后一次出现的时间。
	conn.ZAdd(ctx, "recent:", &redis.Z{Score: timestamp, Member: token})
	if item != "" {
		// 记录用户浏览过的商品。
		conn.ZAdd(ctx, "viewed:"+token, &redis.Z{Score: timestamp, Member: item})
		// 移除旧的记录，只保留用户最近浏览过的25个商品。
		//移除从开始到倒数26的元素
		conn.ZRemRangeByRank(ctx, "viewed:"+token, 0, -26)
	}
}

var QUIT = false
var LIMIT int64 = 10000000

//  2-3 守护进出任务	清除多余的session
func clean_sessions(conn *redis.Client) {
	ctx := context.Background()
	defer fmt.Println("close clean_sessions")
	for !QUIT {
		// 找出目前已有令牌的数量。
		size := conn.ZCard(ctx, "recent:").Val()
		// 令牌数量未超过限制，休眠并在之后重新检查。
		if size <= LIMIT {
			time.Sleep(time.Second)
			continue
		}
		//  获取需要移除的令牌ID。
		end_index := math.Min(float64(size-LIMIT), 100)
		//最旧的那些令牌 timestamp最小
		tokens := conn.ZRange(ctx, "recent:", 0, int64(end_index-1)).Val()

		// 为那些将要被删除的令牌构建键名。
		session_keys := make([]string, 0)
		for _, token := range tokens {
			session_keys = append(session_keys, "viewed:"+token)
		}
		// 移除最旧的那些令牌。
		conn.Del(ctx, session_keys...)
		conn.HDel(ctx, "login:", tokens...)
		for _, v := range tokens {
			conn.ZRem(ctx, "recent:", v)
		}
	}
}

//  2-4	将物品添加到购物车
func add_to_cart(conn *redis.Client, session string, item string, count int) {
	ctx := context.Background()
	if count <= 0 {
		// 从购物车里面移除指定的商品。
		conn.HDel(ctx, "cart:"+session, item)
	} else {
		// 将指定的商品添加到购物车。
		conn.HSet(ctx, "cart:"+session, item, count)
	}
}

//  2-5 守护任务清理session 添加了购物车对应的清理
func clean_full_sessions(conn *redis.Client) {
	ctx := context.Background()
	defer fmt.Println("close clean_full_sessions")
	for !QUIT {
		size := conn.ZCard(ctx, "recent:").Val()
		if size <= LIMIT {
			time.Sleep(time.Second)
			continue
		}
		end_index := math.Min(float64(size-LIMIT), 100)
		sessions := conn.ZRange(ctx, "recent:", 0, int64(end_index-1)).Val()

		session_keys := make([]string, 0)
		for _, sess := range sessions {
			session_keys = append(session_keys, "viewed:"+sess)
			session_keys = append(session_keys, "cart:"+sess) // 新增加的这行代码用于删除旧会话对应用户的购物车。
		}
		conn.Del(ctx, session_keys...)
		conn.HDel(ctx, "login:", sessions...)
		for _, v := range sessions {
			conn.ZRem(ctx, "recent:", v)
		}
	}
}

// 2-6 缓存页面
func cache_request(conn *redis.Client, request string, callback func(string) string) string {
	ctx := context.Background()
	// 对于不能被缓存的请求，直接调用回调函数。
	if !can_cache(conn, request) {
		return callback(request)
	}
	// 将请求转换成一个简单的字符串键，方便之后进行查找。
	page_key := "cache:" + hash_request(request)
	// 尝试查找被缓存的页面。
	content := conn.Get(ctx, page_key).Val()

	if content == "" {
		// 如果页面还没有被缓存，那么生成页面。
		content = callback(request)
		// 将新生成的页面放到缓存里面。
		conn.SetNX(ctx, page_key, content, 300*time.Second)
	}
	// 返回页面。
	return content
}

//  2-7	缓存计划
func schedule_row_cache(conn *redis.Client, row_id string, delay float64) {
	ctx := context.Background()
	// 先设置数据行的延迟值。
	conn.ZAdd(ctx, "delay:", &redis.Z{Score: delay, Member: row_id})
	// 立即缓存数据行。
	conn.ZAdd(ctx, "schedule:", &redis.Z{Score: float64(time.Now().Unix()), Member: row_id})
}

//  2-8	对数据进行缓存
func cache_rows(conn *redis.Client) {
	ctx := context.Background()
	defer fmt.Println("close cache_rows")
	for !QUIT {
		// 尝试获取下一个需要被缓存的数据行以及该行的调度时间戳，
		// 命令会返回一个包含零个或一个元组（tuple）的列表。
		next := conn.ZRangeWithScores(ctx, "schedule:", 0, 0).Val()
		now := float64(time.Now().Unix())
		if next == nil || len(next) == 0 || next[0].Score > now {
			// 暂时没有行需要被缓存，休眠50毫秒后重试。
			time.Sleep(50 * time.Microsecond)
			continue
		}
		fmt.Println("cache_rows update...")
		row_id := next[0].Member.(string)
		// 获取下一次调度前的延迟时间。
		delay := conn.ZScore(ctx, "delay:", row_id).Val()
		if delay <= 0 {
			// 不必再缓存这个行，将它从缓存中移除。
			conn.ZRem(ctx, "delay:", row_id)
			conn.ZRem(ctx, "schedule:", row_id)
			conn.Del(ctx, "inv:"+row_id)
			continue
		}
		// 读取数据行。
		row := InventoryGet(row_id)
		// 更新调度时间并设置缓存值。
		conn.ZAdd(ctx, "schedule:", &redis.Z{Score: float64(now + delay), Member: row_id})
		conn.Set(ctx, "inv:"+row_id, row, 0)
	}
}

//获取数据行内容
func InventoryGet(rowId string) string {
	return "{\"testData\":\"123\",\"name\":\"xiaoming\",\"row_id\":\"" + rowId + "\"}"
}

//  2-9 减小排序值 热度更高
func update_token(conn *redis.Client, token string, user string, item string) {
	ctx := context.Background()
	timestamp := float64(time.Now().Unix())
	conn.HSet(ctx, "login:", token, user)
	conn.ZAdd(ctx, "recent:", &redis.Z{Score: timestamp, Member: token})
	if item != "" {
		conn.ZAdd(ctx, "viewed:"+token, &redis.Z{Score: timestamp, Member: item})
		conn.ZRemRangeByRank(ctx, "viewed:"+token, 0, -26)
		conn.ZIncrBy(ctx, "viewed:", -1, item) // 这行代码是新添加的。
	}
}

//  2-10	缩减热度物品数据
func rescale_viewed(conn *redis.Client) {
	ctx := context.Background()
	defer fmt.Println("close rescale_viewed")
	for !QUIT {
		// 删除所有排名在20 000名之后的商品。
		conn.ZRemRangeByRank(ctx, "viewed:", 20000, -1)
		// 将浏览次数降低为原来的一半
		conn.ZInterStore(ctx, "viewed:", &redis.ZStore{Weights: []float64{0.5}, Keys: []string{"viewed:"}})
		// 5分钟之后再执行这个操作。
		time.Sleep(time.Second * 300)
	}
}

//  2-11	判断物品是否可以被缓存
func can_cache(conn *redis.Client, request string) bool {
	ctx := context.Background()
	// 尝试从页面里面取出商品ID。
	item_id := extract_item_id(request)
	// 检查这个页面能否被缓存以及这个页面是否为商品页面。
	if item_id == "" || is_dynamic(request) {
		return false
	}
	// 取得商品的浏览次数排名。
	rank, err := conn.ZRank(ctx, "viewed:", item_id).Result()
	// 根据商品的浏览次数排名来判断是否需要缓存这个页面。
	return err == nil && rank < 10000
}

//--------------- 以下是用于测试代码的辅助函数 --------------------------------

func extract_item_id(request string) string {
	parsed, _ := url.Parse(request)
	query, _ := url.ParseQuery(parsed.RawQuery)
	val, ok := query["item"]
	if ok {
		return val[0]
	}
	return ""
}

func is_dynamic(request string) bool {
	parsed, _ := url.Parse(request)
	query, _ := url.ParseQuery(parsed.RawQuery)
	for _, v := range query {
		for _, v2 := range v {
			if strings.Contains(v2, "_") {
				return true
			}
		}
	}
	return false
}

func hash_request(request string) string {
	return request + "000"
}

func TestCh02_test_login_cookies() {
	ctx := context.Background()
	conn := redisCli
	token := core.GenID()

	update_token(conn, token, "username", "itemX")
	fmt.Println("We just logged-in/updated token:", token)
	fmt.Println("For user:", "username")

	fmt.Println("What username do we Get when we look-up that token?")
	r := check_token(conn, token)
	fmt.Println(r)

	fmt.Println("Let s drop the maximum number of cookies to 0 to clean them out")
	fmt.Println("We will start a thread to do the cleaning, while we stop it later")

	LIMIT = 0
	go clean_sessions(conn)
	time.Sleep(time.Second)
	QUIT = true
	time.Sleep(time.Second * 2)

	s := conn.HLen(ctx, "login:").Val()
	fmt.Println("The current number of sessions still available is:", s)
}

func TestCh02_test_shoping_cart_cookies() {
	ctx := context.Background()
	conn := redisCli
	token := core.GenID()

	fmt.Println("We'll refresh our session...")
	update_token(conn, token, "username", "itemX")
	fmt.Println("And add an item to the shopping cart")
	add_to_cart(conn, token, "itemY", 3)
	r := conn.HGetAll(ctx, "cart:"+token).Val()
	fmt.Println("Our shopping cart currently has:", r)

	fmt.Println("Let's clean out our sessions and carts")
	LIMIT = 0
	go clean_full_sessions(conn)
	time.Sleep(time.Second)
	QUIT = true
	time.Sleep(2 * time.Second)

	r2 := conn.HGetAll(ctx, "cart:"+token).Val()
	fmt.Println("Our shopping cart now contains:", r2)
}

func TestCh02_test_cache_request() {
	conn := redisCli
	token := core.GenID()

	callback := func(request string) string {
		return "content for " + request
	}

	update_token(conn, token, "username", "itemX")
	url := "http://test.com/?item=itemX"
	fmt.Println("We are going to cache a simple request against ", url)
	result := cache_request(conn, url, callback)
	fmt.Println("We got initial content:", result)

	fmt.Println("To test that we've cached the request, we'll pass a bad callback")
	result2 := cache_request(conn, url, nil)
	fmt.Println("We ended up Getting the same response! ", result2)

	fmt.Println(can_cache(conn, "http://test.com/"))
	fmt.Println(can_cache(conn, "http://test.com/?item=itemX&_=1234536"))
}

func TestCh02_test_cache_rows() {
	ctx := context.Background()

	conn := redisCli

	fmt.Println("First, let's schedule caching of itemX every 5 seconds")
	schedule_row_cache(conn, "itemX", 5)
	fmt.Println("Our schedule looks like:")
	s := conn.ZRangeWithScores(ctx, "schedule:", 0, -1).Val()
	fmt.Println(s)

	fmt.Println("We.ll start a caching thread that will cache the data...")
	go cache_rows(conn)

	time.Sleep(time.Second)
	fmt.Println("Our cached data looks like:")
	r := conn.Get(ctx, "inv:itemX").Val()
	fmt.Println(r)
	fmt.Println("We'll check again in 5 seconds...")
	time.Sleep(5 * time.Second)
	fmt.Println("Notice that the data has changed...")
	r2 := conn.Get(ctx, "inv:itemX").Val()
	fmt.Println(r2)

	fmt.Println("Let's force un-caching")
	schedule_row_cache(conn, "itemX", -1)
	time.Sleep(time.Second)
	r3 := conn.Get(ctx, "inv:itemX").Val()
	fmt.Println("The cache was cleared?", r3 == "")

	QUIT = true
	time.Sleep(2 * time.Second)
}

func main() {
	ctx := context.Background()
	//TestCh02_test_login_cookies()
	//TestCh02_test_cache_rows()
	//TestCh02_test_cache_request()
	TestCh02_test_shoping_cart_cookies()
	core.ClearAllKeys(ctx, redisCli)
}
