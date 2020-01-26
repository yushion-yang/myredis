package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"redis-learn/core"
	"strconv"
	"strings"
	"time"
)

var redisCli *redis.Client

//初始化连接
func init() {
	redisCli = core.InitRedis("127.0.0.1:6379", "", 0)
}

//测试redis的string类型
func Ex1_1_string() {
	{
		ret, err := redisCli.Set("hello", "world", 0).Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.Get("hello").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.Del("hello").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.Get("hello").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
}

//测试redis的list类型
func Ex1_2_list() {
	{
		ret, err := redisCli.RPush("list-key", "item").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.RPush("list-key", "item2").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.RPush("list-key", "item").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.LRange("list-key", 0, 1).Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.LIndex("list-key", 1).Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.LPop("list-key").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.LRange("list-key", 0, -1).Result()
		fmt.Println("ret:", ret, " err:", err)
	}
}

//测试hash的 getall
func test() {
	{
		ret, err := redisCli.HSet("hsh-key", "key1", "val1").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.HSet("hsh-key", "key2", "val2").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.HSet("hsh-key", "key3", "val3").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.HGetAll("hsh-key").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
}

//测试redis的set类型
func Ex1_3_set() {
	{
		ret, err := redisCli.SAdd("set-key", "item").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.SAdd("set-key", "item2").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.SAdd("set-key", "item3").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.SAdd("set-key", "item").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.SMembers("set-key").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.SIsMember("set-key", "item4").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.SIsMember("set-key", "item").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.SRem("set-key", "item2").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.SRem("set-key", "item2").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.SMembers("set-key").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
}

//测试redis的hash类型
func Ex1_4_hssh() {
	{
		ret, err := redisCli.HSet("hash-ey", "sub-key1", "value1").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.HSet("hash-ey", "sub-key2", "value2").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.HSet("hash-ey", "sub-key1", "value1").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.HGetAll("hash-ey").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.HDel("hash-ey", "sub-key2").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.HDel("hash-ey", "sub-key2").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.HGet("hash-ey", "sub-key1").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.HGetAll("hash-ey").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
}

//测试redis的zset类型
func Ex1_5_zset() {
	{
		ret, err := redisCli.ZAdd("zset-ey", &redis.Z{Score: 728, Member: "member1"}).Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.ZAdd("zset-ey", &redis.Z{Score: 982, Member: "member0"}).Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.ZAdd("zset-ey", &redis.Z{Score: 982, Member: "member0"}).Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.ZRangeWithScores("zset-ey", 0, -1).Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		//type ZRangeBy struct {
		//	Min, Max      string
		//	Offset, Count int64			//limit
		//}
		ret, err := redisCli.ZRangeByScore("zset-ey", &redis.ZRangeBy{Min: "0", Max: "800"}).Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.ZRem("zset-ey", "member1").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.ZRem("zset-ey", "member1").Result()
		fmt.Println("ret:", ret, " err:", err)
	}
	{
		ret, err := redisCli.ZRangeWithScores("zset-ey", 0, -1).Result()
		fmt.Println("ret:", ret, " err:", err)
	}
}

type Article struct {
	Title  string
	Link   string
	Poster string
	Time   time.Time
	Votes  int
}

type ArticleTimeSort struct {
	KV []struct {
		Article int64
		Score   float64
	}
}

type ArticleScoreSort struct {
	KV []struct {
		Article int64
		Score   float64
	}
}

type Voted struct {
	User []int64
}

//	准备好需要用到的常量
const ONE_WEEK_IN_SECONDS = 7 * 86400
const VOTE_SCORE = 432

//为文章投票1-6
func ArticleVote(conn *redis.Client, user string, article string) {
	//计算文章的投票截止时间。
	cutoff := float64(time.Now().Unix() - ONE_WEEK_IN_SECONDS)

	// 检查是否还可以对文章进行投票
	//（虽然使用散列也可以获取文章的发布时间，
	// 但有序集合返回的文章发布时间为浮点数，
	// 可以不进行转换直接使用）。
	if conn.ZScore("time:", article).Val() < cutoff {
		return
	}

	// 从article:id标识符（identifier）里面取出文章的ID。
	article_id := strings.Split(article, ":")[1]

	// 如果用户是第一次为这篇文章投票，那么增加这篇文章的投票数量和评分。
	if conn.SAdd("voted:"+article_id, user).Val() > 0 {
		conn.ZIncrBy("score:", VOTE_SCORE, article)
		conn.HIncrBy(article, "votes", 1)
		conn.HIncrBy(article, "score", VOTE_SCORE)
	}
}

//发布新的文章1-7
func PostArticle(conn *redis.Client, user string, title string, link string) string {
	// 生成一个新的文章ID。
	article_id := fmt.Sprintf("%v", conn.Incr("article:").Val())

	voted := "voted:" + article_id
	// 将发布文章的用户添加到文章的已投票用户名单里面，
	// 然后将这个名单的过期时间设置为一周（第3章将对过期时间作更详细的介绍）。
	conn.SAdd(voted, user)
	conn.Expire(voted, ONE_WEEK_IN_SECONDS*time.Second)

	now := time.Now().Unix()
	article := "article:" + article_id
	score := float64(now + VOTE_SCORE)
	// 将文章信息存储到一个散列里面。
	hmset(conn, article, "title", title, "link", link, "poster", user, "time", now, "votes", 1, "score", score)

	// 将文章添加到根据发布时间排序的有序集合和根据评分排序的有序集合里面。
	conn.ZAdd("score:", &redis.Z{Score: score, Member: article})
	conn.ZAdd("time:", &redis.Z{Score: float64(now), Member: article})
	return article_id
}

const ARTICLES_PER_PAGE = 25

//获取以某种排序的所有文章1-8 结合分组集合做交际  可以得出某分组的某种排序结果
func GetArticles(conn *redis.Client, page int, order string) []interface{} {
	if order == "" {
		order = "score:"
	}
	// 设置获取文章的起始索引和结束索引。
	start := int64((page - 1) * ARTICLES_PER_PAGE)
	end := start + ARTICLES_PER_PAGE - 1

	// 获取多个文章ID。
	ids := conn.ZRevRange(order, start, end).Val()
	articles := make([]interface{}, 0)
	// 根据文章ID获取文章的详细信息。
	for _, id := range ids {
		article_data := conn.HGetAll(id).Val()
		article_data["id"] = id
		articles = append(articles, article_data)
	}
	return articles
}

//将文章添加到组 或者将文章从某组中删除1-9
func AddRemoveGroups(conn *redis.Client, article_id int, to_add []string, to_remove []string) {
	// 构建存储文章信息的键名。
	article := "article:" + strconv.Itoa(article_id)
	for _, group := range to_add {
		// 将文章添加到它所属的群组里面。
		conn.SAdd("group:"+group, article)
	}
	for _, group := range to_remove {
		// 从群组里面移除文章。
		conn.SRem("group:"+group, article)
	}
}

//获取分组所有文章1-10
func GetGroupArticles(conn *redis.Client, group string, page int, order string) []interface{} {
	if order == "" {
		order = "score:"
	}
	// 为每个群组的每种排列顺序都创建一个键。
	key := order + group
	// 检查是否有已缓存的排序结果，如果没有的话就现在进行排序。
	if conn.Exists(key).Val() == 0 {
		// 根据评分或者发布时间，对群组文章进行排序。
		// hash 的集合没有分数 默认分数为1 进行合并的weight为每个集合的权重 在继续集合操作前会乘以该权重
		conn.ZInterStore(key, &redis.ZStore{
			Keys:      []string{"group:" + group, order},
			Aggregate: "MAX",
		})
		// 让Redis在60秒钟之后自动删除这个有序集合。
		conn.Expire(key, time.Minute)
	}
	// 调用之前定义的get_articles()函数来进行分页并获取文章数据。
	return GetArticles(conn, page, key)
}

//为文章投票1-11 额外练习
func ArticleOpposeVote(conn *redis.Client, user string, article string) {
	//计算文章的投票截止时间。
	cutoff := float64(time.Now().Unix() - ONE_WEEK_IN_SECONDS)

	// 检查是否还可以对文章进行投票
	//（虽然使用散列也可以获取文章的发布时间，
	// 但有序集合返回的文章发布时间为浮点数，
	// 可以不进行转换直接使用）。
	if conn.ZScore("time:", article).Val() < cutoff {
		return
	}

	// 从article:id标识符（identifier）里面取出文章的ID。
	article_id := strings.Split(article, ":")[1]

	// 如果用户是第一次为这篇文章投票，那么增加这篇文章的投票数量和评分。
	if conn.SAdd("oppose_voted:"+article_id, user).Val() > 0 {
		conn.ZIncrBy("score:", -VOTE_SCORE, article)
		conn.HIncrBy(article, "votes", -1)
		conn.HIncrBy(article, "score", -VOTE_SCORE)
	}
}

//总测试
func TestCh01() {
	conn := redisCli
	article_id := PostArticle(conn, "username", "A title", "http://www.google.com")
	fmt.Println("We posted a new article with id:", article_id)

	fmt.Println("Its HASH looks like:")
	r := conn.HGetAll("article:" + article_id).Val()
	fmt.Println(r)

	ArticleVote(conn, "other_user", "article:"+article_id)
	fmt.Println("We voted for the article, it now has votes:")
	v := conn.HGet("article:"+article_id, "votes").Val()
	fmt.Println(v)

	ArticleOpposeVote(conn, "other_user", "article:"+article_id)
	fmt.Println("We oppose voted for the article, it now has votes:")
	v2 := conn.HGet("article:"+article_id, "votes").Val()
	fmt.Println(v2)

	fmt.Println("The currently highest-scoring articles are:")
	articles := GetArticles(conn, 1, "")
	fmt.Println(articles)
	fmt.Println("article count:", len(articles))

	aid, _ := strconv.ParseInt(article_id, 10, 64)
	AddRemoveGroups(conn, int(aid), []string{"new-group"}, nil)
	fmt.Println("We added the article to a new group, other articles include:")
	articles = GetGroupArticles(conn, "new-group", 1, "")
	fmt.Println(articles)
	fmt.Println("article count:", len(articles))
}

type HASH_KV struct {
	key string
	val interface{}
}

func HMset(conn *redis.Client, key string, kvs []*HASH_KV) {
	for _, v := range kvs {
		conn.HSet(key, v.key, v.val)
	}
}

func hmset(conn *redis.Client, key string, kvs ...interface{}) {
	length := len(kvs)
	if length%2 != 0 {
		fmt.Println("ERR wrong number of arguments for 'hmset' command")
		return
	}
	//先参数检测再调用
	for i := 0; i < length; i += 2 {
		if _, ok := kvs[i].(string); !ok {
			fmt.Println("kvs[i] is not string")
			return
		}
	}
	for i := 0; i < length; i += 2 {
		conn.HSet(key, kvs[i].(string), kvs[i+1])
	}
}

func main() {
	//test()
	//Ex1_1_string()
	//Ex1_2_list()
	//Ex1_3_set()
	//Ex1_4_hssh()
	//Ex1_5_zset()
	TestCh01()
	core.ClearAllKeys(redisCli)
}
