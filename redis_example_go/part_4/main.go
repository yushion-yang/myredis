package main

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"io/ioutil"
	"os"
	"redis-learn/core"
	"strconv"
	"time"
)

var redisCli *redis.Client

func init() {
	redisCli = core.InitRedis("127.0.0.1:6379", "", 0)
}

type Users_Id struct {
	Name  string
	Funds int
}

type Inventory_Id struct {
	Items []string
}

func GetFilesAndDirs(dirPth string) []string {
	dir, err := ioutil.ReadDir(dirPth)
	dirs := make([]string, 0)
	if err != nil {
		return nil
	}

	for _, fi := range dir {
		if fi.IsDir() { // 目录, 递归遍历
			dirs = append(dirs, fi.Name())
		}
	}
	return dirs
}

// 代码清单 4-2
// 日志处理函数接受的其中一个参数为回调函数，
// 这个回调函数接受一个Redis连接和一个日志行作为参数，
// 并通过调用流水线对象的方法来执行Redis命令。
func process_logs(conn *redis.Client, path string, callback func(redis.Pipeliner, string) string) {
	// 获取文件当前的处理进度。
	ret := conn.MGet("progress:file", "progress:position").Val()
	current_file, offset := ret[0].(int), ret[1].(int64)

	pipe := conn.Pipeline()

	// 通过使用闭包（closure）来减少重复代码
	update_progress := func(pipe redis.Pipeliner, fname int, offset int64) {
		// 更新正在处理的日志文件的名字和偏移量。
		pipe.MSet("progress:file", fname, "progress:position", offset)
		// 这个语句负责执行实际的日志更新操作，
		// 并将日志文件的名字和目前的处理进度记录到Redis里面。
		pipe.Exec()
	}
	// 有序地遍历各个日志文件。
	for fname := range GetFilesAndDirs(path) {
		// 略过所有已处理的日志文件。
		if fname < current_file {
			continue
		}
		inp, _ := os.Open(path + strconv.Itoa(fname))
		// 在接着处理一个因为系统崩溃而未能完成处理的日志文件时，略过已处理的内容。
		if fname == current_file {
			_, _ = inp.Seek(offset, 0)
		} else {
			offset = 0
		}
		current_file = 0

		// 枚举函数遍历一个由文件行组成的序列，
		// 并返回任意多个二元组，
		// 每个二元组包含了行号lno和行数据line，
		// 其中行号从0开始。
		var lines []string
		//for lno, line := range enumerate(inp) {
		for lno, line := range lines {
			// 处理日志行。
			callback(pipe, line)
			// 更新已处理内容的偏移量。
			offset += offset + int64(len(line))

			// 每当处理完1000个日志行或者处理完整个日志文件的时候，
			// 都更新一次文件的处理进度。
			if (lno+1)%1000 == 0 {
				update_progress(pipe, fname, offset)
			}
		}
		update_progress(pipe, fname, offset)
		//inp.close()
	}
}

func wait_for_sync(mconn *redis.Client, sconn *redis.Client) {
	identifier := core.GenID()
	// 将令牌添加至主服务器。
	mconn.ZAdd("sync:wait", &redis.Z{Member: identifier, Score: float64(time.Now().Unix())})

	// 如果有必要的话，等待从服务器完成同步。
	for sconn.Info("master_link_status").Val() != "up" {
		time.Sleep(time.Microsecond)
	}
	// 等待从服务器接收数据更新。
	for sconn.ZScore("sync:wait", identifier).Val() <= 0 {
		time.Sleep(time.Microsecond)
	}
	// 最多只等待一秒钟。
	deadline := float64(time.Now().Unix()) + 1.01
	for float64(time.Now().Unix()) < deadline {
		// 检查数据更新是否已经被同步到了磁盘。
		if i, _ := sconn.Info("aof_pending_bio_fsync").Int(); i == 0 {
			break
		}
		time.Sleep(time.Microsecond)
	}

	// 清理刚刚创建的新令牌以及之前可能留下的旧令牌。
	mconn.ZRem("sync:wait", identifier)
	mconn.ZRemRangeByScore("sync:wait", "0", strconv.Itoa(int(time.Now().Unix()-900)))
}

//代码清单 4-5
func list_item(conn *redis.Client, itemid string, sellerid string, price int) bool {
	inventory := "inventory:" + sellerid
	item := itemid + sellerid
	end := time.Now().Unix() + 5
	pipe := conn.Pipeline()

	for time.Now().Unix() < end {
		// 监视用户包裹发生的变化。
		txf := func(tx *redis.Tx) error {
			if !pipe.SIsMember(inventory, itemid).Val() {
				// 如果指定的物品不在用户的包裹里面，
				// 那么停止对包裹键的监视并返回一个空值。
				return errors.New("!pipe.SIsMember(inventory, itemid).Val()")
			}
			_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.ZAdd("market:", &redis.Z{Score: float64(price), Member: item})
				pipe.SRem(inventory, itemid)
				return nil
			})
			return err
		}
		err := conn.Watch(txf, inventory)
		if err != redis.TxFailedErr {
			fmt.Println("err:", err)
			return true
		} else {
			continue
		}
	}
	return false
}

//出售商品
func purchase_item(conn *redis.Client, buyerid string, itemid string, sellerid string, lprice int) bool {
	buyer := "users:" + buyerid
	seller := "users:" + sellerid
	item := itemid + sellerid
	inventory := "inventory:" + buyerid
	end := time.Now().Unix() + 10
	pipe := conn.Pipeline()

	for time.Now().Unix() < end {
		// 对物品买卖市场以及买家账号信息的变化进行监视。
		txf := func(tx *redis.Tx) error {
			// 检查指定物品的价格是否出现了变化，
			// 以及买家是否有足够的钱来购买指定的物品。
			price := int(pipe.ZScore("market:", item).Val())
			funds, _ := pipe.HGet(buyer, "funds").Int()
			if price != lprice || price > funds {
				//pipe.unwatch()
				return errors.New("price != lprice || price > funds")
			}
			_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				// 将买家支付的货款转移给卖家，并将卖家出售的物品移交给买家。
				pipe.HIncrBy(seller, "funds", int64(price))
				pipe.HIncrBy(buyer, "funds", int64(-price))
				pipe.SAdd(inventory, itemid)
				pipe.ZRem("market:", item)
				return nil
			})
			return err
		}
		err := conn.Watch(txf, inventory)
		if err != redis.TxFailedErr {
			fmt.Println("err:", err)
			return true
		} else {
			continue
		}
	}
	return false
}

//更新令牌
func update_token(conn *redis.Client, token string, user string, item string) {
	// 获取时间戳。
	timestamp := time.Now().Unix()
	// 创建令牌与已登录用户之间的映射。
	conn.HSet("login:", token, user)
	// 记录令牌最后一次出现的时间。
	conn.ZAdd("recent:", &redis.Z{Score: float64(timestamp), Member: token})
	if item != "" {
		// 把用户浏览过的商品记录起来。
		conn.ZAdd("viewed:"+token, &redis.Z{Score: float64(timestamp), Member: token})
		// 移除旧商品，只记录最新浏览的25件商品。
		conn.ZRemRangeByRank("viewed:"+token, 0, -26)
		// 更新给定商品的被浏览次数。
		conn.ZIncrBy("viewed:", -1, item)
	}
}

//使用流水线的方式更新令牌
func update_token_pipeline(conn *redis.Client, token string, user string, item string) {
	timestamp := time.Now().Unix()
	// 设置流水线。
	pipe := conn.Pipeline() //A
	pipe.HSet("login:", token, user)
	pipe.ZAdd("recent:", &redis.Z{Score: float64(timestamp), Member: token})
	if item != "" {
		pipe.ZAdd("viewed:"+token, &redis.Z{Score: float64(timestamp), Member: item})
		pipe.ZRemRangeByRank("viewed:"+token, 0, -26)
		pipe.ZIncrBy("viewed:", -1, item)
	}
	// 执行那些被流水线包裹的命令。
	pipe.Exec() //B
}

//性能测试
func benchmark_update_token(conn *redis.Client, duration int64) {
	// 测试会分别执行update_token()函数和update_token_pipeline()函数。
	funcs := []func(conn *redis.Client, token string, user string, item string){update_token, update_token_pipeline}
	for _, function := range funcs {
		// 设置计数器以及测试结束的条件。
		count := 0                 //B
		start := time.Now().Unix() //B
		end := start + duration    //B
		for time.Now().Unix() < end {
			count += 1
			// 调用两个函数的其中一个。
			function(conn, "token", "user", "item") //C
		}
		// 计算函数的执行时长。
		//delta := time.Now().Unix() - start //D
		_ = time.Now().Unix() - start //D
		// 打印测试结果。
		//fmt.Println(function.__name__, count, delta, count/delta //E
	}
}

func main() {

}
