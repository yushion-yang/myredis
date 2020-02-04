package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"math"
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

//将联系人添加到用户的最近联系人列表中
func add_update_contact(conn *redis.Client, user string, contact string) {
	ac_list := "recent:" + user
	// 准备执行原子操作。
	pipeline := conn.Pipeline()
	// 如果联系人已经存在，那么移除他。
	pipeline.LRem(ac_list, 1, contact)
	// 将联系人推入到列表的最前端。
	pipeline.LPush(ac_list, contact)
	// 只保留列表里面的前100个联系人。
	pipeline.LTrim(ac_list, 0, 99)
	// 实际地执行以上操作。
	pipeline.Exec()
}

//将联系人从用户的最近联系人列表中删除
func remove_contact(conn *redis.Client, user string, contact string) {
	conn.LRem("recent:"+user, 1, contact)
}

//返回带有前缀的联系人
func fetch_autocomplete_list(conn *redis.Client, user string, prefix string) []string {
	// 获取自动补完列表。
	candidates := conn.LRange("recent:"+user, 0, -1).Val()
	matches := make([]string, 0)
	// 检查每个候选联系人。
	for _, v := range candidates {
		// 发现一个匹配的联系人。
		if strings.HasPrefix(strings.ToLower(v), prefix) {
			matches = append(matches, v)
		}
	}
	// 返回所有匹配的联系人。
	return matches
}

// 准备一个由已知字符组成的列表。
var valid_characters = "`abcdefghijklmnopqrstuvwxyz{"

//获取前缀的首尾标识  用于在有序集合中获取区间范围
func find_prefix_range(prefix string) (string, string) {
	// 在字符列表中查找前缀字符所处的位置。
	posn := strings.Index(valid_characters, prefix)
	// 找到前驱字符。
	suffix := ""
	if posn < 0 {
		suffix = string(valid_characters[0])
	} else {
		suffix = string(valid_characters[posn-1])
	}
	// 返回范围。
	return prefix[:len(prefix)-1] + suffix + "{", prefix + "{"
}

//使用redis进行自动补全 添加标识的起始和结尾点 用于在有序集合中获取到前缀匹配的区间范围
func autocomplete_on_prefix(conn *redis.Client, guild string, prefix string) []string {
	// 根据给定的前缀计算出查找范围的起点和终点。
	start, end := find_prefix_range(prefix)
	identifier := core.GenID()
	start += identifier //考虑多个成员对同一工会成员进行发生消息时 避免重复添加相同的其实和结束元素
	end += identifier
	zset_name := "members:" + guild

	// 将范围的起始元素和结束元素添加到有序集合里面。
	conn.ZAdd(zset_name, &redis.Z{Score: 0, Member: start}, &redis.Z{Score: 0, Member: end})
	var cmd *redis.StringSliceCmd
	var items []string
	for {
		txf := func(tx *redis.Tx) error {
			// 找到两个被插入元素在有序集合中的排名。
			sindex := tx.ZRank(zset_name, start).Val()
			eindex := tx.ZRank(zset_name, end).Val()
			erange := math.Min(float64(sindex+9), float64(eindex-2))
			_, err := tx.TxPipelined(func(pipe redis.Pipeliner) error {
				// 获取范围内的值，然后删除之前插入的起始元素和结束元素。
				pipe.ZRem(zset_name, start, end)
				cmd = pipe.ZRange(zset_name, sindex, int64(erange))
				return nil
			})
			return err
		}

		err := conn.Watch(txf, zset_name)
		// 如果自动补完有序集合已经被其他客户端修改过了，那么进行重试。
		if err != redis.TxFailedErr {
			fmt.Print("err != redis.TxFailedErr err:", err)
			items = cmd.Val()
			break
		}
	}
	// 如果有其他自动补完操作正在执行，
	// 那么从获取到的元素里面移除起始元素和终结元素。
	retItems := make([]string, 0)
	for _, v := range items {
		if !strings.Contains(v, "{") {
			retItems = append(retItems, v)
		}
	}
	return retItems
}

//加入工会
func join_guild(conn *redis.Client, guild string, user string) {
	conn.ZAdd("members:"+guild, &redis.Z{Score: 0, Member: user})
}

//离开工会
func leave_guild(conn *redis.Client, guild string, user string) {
	conn.ZRem("members:"+guild, user)
}

//使用watch事务来包裹交易确保数据的一致性
func list_item(conn *redis.Client, itemid string, sellerid int, price float64) error {
	//...
	var inv = ""
	var item = ""
	// 监视卖家包裹发生的变动。
	txf := func(tx *redis.Tx) error {
		if !tx.SIsMember(inv, itemid).Val() {
			return errors.New("itemid is not member of inv")
		}
		_, err := tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.ZAdd("market:", &redis.Z{Score: price, Member: item})
			pipe.SRem(inv, itemid)
			return nil
		})
		return err
	}
	err := conn.Watch(txf, inv)
	if err != redis.TxFailedErr {
		return err
	}
	return nil
}

//购买商品
func purchase_item(conn *redis.Client, buyerid string, itemid int, sellerid int, lprice int64) bool {
	//...
	var seller = ""
	var buyer = ""
	var item = ""
	var inventory = ""

	txf := func(tx *redis.Tx) error {
		price := int64(tx.ZScore("market:", item).Val())
		funds, _ := strconv.ParseInt(tx.HGet(buyer, "funds").Val(), 64, 10)
		// 检查物品是否已经售出、物品的价格是否已经发生了变化，
		// 以及买家是否有足够的金钱来购买这件物品。
		if price != lprice || price > funds {
			return errors.New("price != lprice ||price > funds")
		}
		_, err := tx.TxPipelined(func(pipe redis.Pipeliner) error {
			// 将买家支付的货款转移给卖家，并将被卖出的物品转移给买家。
			pipe.HIncrBy(seller, "funds", price)
			pipe.HIncrBy(buyerid, "funds", -price)
			pipe.SAdd(inventory, itemid)
			pipe.ZRem("market:", item)
			return nil
		})
		return err
	}
	// 监视市场以及买家个人信息发生的变化。
	err := conn.Watch(txf, buyer)
	if err != redis.TxFailedErr {
		fmt.Println("err:", err)
		return false
	}
	return true
}

//获取分布式锁
func acquire_lock(conn *redis.Client, lockname string, acquire_timeout int64) string {
	if acquire_timeout < 10 {
		acquire_timeout = 10
	}
	// 128位随机标识符。
	identifier := core.GenID()
	end := time.Now().Unix() + acquire_timeout
	for time.Now().Unix() < end {
		// 尝试取得锁。
		if conn.SetNX("lock:"+lockname, identifier, 0).Val() {
			return identifier
		}
		time.Sleep(time.Microsecond)
	}
	return ""
}

//使用锁来进行商品交易
func purchase_item_with_lock(conn *redis.Client, buyerid string, itemid string, sellerid string) bool {
	buyer := "users:" + buyerid
	seller := "users:" + sellerid
	item := itemid + sellerid
	inventory := "inventory:" + buyerid

	// 尝试获取锁。
	locked := acquire_lock(conn, "market:", 0)
	if locked == "" {
		return false
	}
	defer release_lock(conn, "market:", locked)

	pipe := conn.Pipeline()
	// 检查物品是否已经售出，以及买家是否有足够的金钱来购买物品。
	priceCmd := pipe.ZScore("market:", item)
	fundsCmd := pipe.HGet(buyer, "funds")
	pipe.Exec()
	price := int64(priceCmd.Val())
	funds, _ := fundsCmd.Int64()
	if price == 0 || price > funds {
		return false
	}

	// 将买家支付的货款转移给卖家，并将售出的物品转移给买家。
	pipe.HIncrBy(seller, "funds", price)
	pipe.HIncrBy(buyer, "funds", -price)
	pipe.SAdd(inventory, itemid)
	pipe.ZRem("market:", item)
	pipe.Exec()
	return true
}

//释放锁
func release_lock(conn *redis.Client, lockname string, identifier string) bool {
	lockname = "lock:" + lockname
	for {
		// 检查并确认进程还持有着锁。
		txf := func(tx *redis.Tx) error {
			if tx.Get(lockname).Val() == identifier {
				_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
					pipe.Del(lockname)
					return nil
				})
				return err
			} else {
				// 进程已经失去了锁。
				return errors.New("tx.Get(lockname) !=identifier")
			}
		}
		err := conn.Watch(txf, lockname)
		//watch值改变
		if err == redis.TxFailedErr {
			continue
			//成功
		} else if err == nil {
			return true
			//失去锁
		} else {
			fmt.Println("err:", err)
			return false
		}
	}
}

//带过期时间的分布式锁
func acquire_lock_with_timeout(conn *redis.Client, lockname string, acquire_timeout int64, lock_timeout time.Duration) string {
	acquire_timeout = 10
	lock_timeout = 10
	// 128位随机标识符。
	identifier := core.GenID()
	lockname = "lock:" + lockname
	// 确保传给EXPIRE的都是整数。
	//lock_timeout := int(math.ceil(lock_timeout))

	end := time.Now().Unix() + acquire_timeout
	for time.Now().Unix() < end {
		// 获取锁并设置过期时间。
		if conn.SetNX(lockname, identifier, lock_timeout*time.Second).Val() {
			return identifier
			// 检查过期时间，并在有需要时对其进行更新。 -1代表没有设置过期时间
		} else if conn.TTL(lockname).Val() == -1 {
			conn.Expire(lockname, lock_timeout*time.Second)
		}
		time.Sleep(time.Microsecond)
	}
	return ""
}

//将时间戳作为分数的有序集合 对于不过期的一定优先排名的将获取到信号量 对于时钟不一致的多个分布式机器是不公平的抢夺信号量
func acquire_semaphore(conn *redis.Client, semname string, limit int64, timeout int64) string {
	timeout = 10
	// 128位随机标识符。
	identifier := core.GenID()
	now := time.Now().Unix()

	pipeline := conn.Pipeline()
	// 清理过期的信号量持有者。
	pipeline.ZRemRangeByScore(semname, "-inf", strconv.Itoa(int(now-timeout)))
	// 尝试获取信号量。
	pipeline.ZAdd(semname, &redis.Z{Score: float64(now), Member: identifier})
	// 检查是否成功取得了信号量。
	limitCmd := pipeline.ZRank(semname, identifier)
	pipeline.Exec()
	if limitCmd.Val() < limit {
		return identifier
	}
	// 获取信号量失败，删除之前添加的标识符。
	conn.ZRem(semname, identifier)
	return ""
}

//释放信号量
func release_semaphore(conn *redis.Client, semname string, identifier string) int64 {
	// 如果信号量已经被正确地释放，那么返回True；
	// 返回False则表示该信号量已经因为过期而被删除了。
	return conn.ZRem(semname, identifier).Val()
}

//公平的获取信号量 使用自增值作为评判依据 允许个机器的时钟存在一定的偏差
func acquire_fair_semaphore(conn *redis.Client, semname string, limit int64, timeout int64) string {
	timeout = 10
	// 128位随机标识符。
	identifier := core.GenID()
	czset := semname + ":owner"
	ctr := semname + ":counter"

	now := time.Now().Unix()
	pipeline := conn.Pipeline()
	// 删除超时的信号量。
	pipeline.ZRemRangeByScore(semname, "-inf", strconv.Itoa(int(now-timeout)))
	pipeline.ZInterStore(czset, &redis.ZStore{Keys: []string{czset, semname}, Weights: []float64{1, 0}})

	// 对计数器执行自增操作，并获取操作执行之后的值。
	counterCmd := pipeline.Incr(ctr)
	counter := counterCmd.Val()

	// 尝试获取信号量。
	pipeline.ZAdd(semname, &redis.Z{Score: float64(now), Member: identifier})
	pipeline.ZAdd(czset, &redis.Z{Score: float64(counter), Member: identifier})

	// 通过检查排名来判断客户端是否取得了信号量。
	pipeline.ZRank(czset, identifier)
	if counter < limit {
		// 客户端成功取得了信号量。
		return identifier
	}
	// 客户端未能取得信号量，清理无用数据。
	pipeline.ZRem(semname, identifier)
	pipeline.ZRem(czset, identifier)
	pipeline.Exec()
	return ""
}

//释放公平的信号量
func release_fair_semaphore(conn *redis.Client, semname string, identifier string) int64 {
	pipeline := conn.Pipeline()
	retCmd := pipeline.ZRem(semname, identifier)
	pipeline.ZRem(semname+":owner", identifier)
	// 返回True表示信号量已被正确地释放，
	// 返回False则表示想要释放的信号量已经因为超时而被删除了。
	return retCmd.Val()
}

//更新信号量的持有时间（续命） ZADD操作会刷新已经存在的值
func refresh_fair_semaphore(conn *redis.Client, semname string, identifier string) bool {
	// 更新客户端持有的信号量。
	if conn.ZAdd(semname, &redis.Z{Score: float64(time.Now().Unix()), Member: identifier}).Val() > 0 {
		// 告知调用者，客户端已经失去了信号量。
		release_fair_semaphore(conn, semname, identifier)
		return false
	}
	// 客户端仍然持有信号量。
	return true
}

//带锁的方式获取信号量	acquire_fair_semaphore 内部有对有序集合的写操作  存在竞态
func acquire_semaphore_with_lock(conn *redis.Client, semname string, limit int64, timeout int64) string {
	timeout = 10
	identifier := acquire_lock(conn, semname, 1)
	if identifier != "" {
		return acquire_fair_semaphore(conn, semname, limit, timeout)
	}
	return ""
}

//将邮件序列化之后推入队列中
func send_sold_email_via_queue(conn *redis.Client, seller string, item string, price int, buyer string) {
	// 准备好待发送邮件。
	data := map[string]interface{}{
		"seller_id": seller,
		"item_id":   item,
		"price":     price,
		"buyer_id":  buyer,
		"time":      time.Now(),
	}
	// 将待发送邮件推入到队列里面。
	bytes, _ := json.Marshal(data)
	conn.RPush("queue:email", string(bytes))
}

var QUIT bool

//
func process_sold_email_queue(conn *redis.Client) {
	for !QUIT {
		// 尝试获取一封待发送邮件。
		packed := conn.BLPop(30*time.Second, "queue:email").Val()
		// 队列里面暂时还没有待发送邮件，重试。
		if packed == nil || len(packed) == 0 {
			continue
		}
		// 从JSON对象中解码出邮件信息。
		var to_send map[string]string
		json.Unmarshal([]byte(packed[1]), to_send)
		// 使用预先编写好的邮件发送函数来发送邮件。
		//if err:=fetch_data_and_send_sold_email(to_send);err!=nil {
		//	fmt.Println("Failed to send sold email", err, to_send)
		//} else {
		//	fmt.Println("Sent sold email", to_send)
		//}
	}
}

//阻塞弹出任务进行执行	根据任务名进行对应函数的反射调用
func worker_watch_queue(conn *redis.Client, queue string, callbacks map[string]func([]string)) {
	for !QUIT {
		// 尝试从队列里面取出一项待执行任务。
		packed := conn.BLPop(30*time.Second, queue).Val()
		// 队列为空，没有任务需要执行；重试。
		if packed == nil || len(packed) == 0 {
			continue
		}
		// 解码任务信息。
		var name string
		var args []string
		//name, args := json.loads(packed[1])	//自行实现
		// 没有找到任务指定的回调函数，用日志记录错误并重试。
		if _, ok := callbacks[name]; !ok {
			fmt.Println("Unknown callback ", name)
			continue
		} else {
			callbacks[name](args)
		}
	}
}

//go-redis的客户端API中BLPop支持对多个列表进行取值操作
func worker_watch_queues(conn *redis.Client, queues []string, callbacks map[string]func([]string)) { // 实现优先级特性要修改的第一行代码。
	for !QUIT {
		// 尝试从队列里面取出一项待执行任务。
		packed := conn.BLPop(30*time.Second, queues...).Val() // 实现优先级特性要修改的第二行代码。
		// 队列为空，没有任务需要执行；重试。
		if packed == nil || len(packed) == 0 {
			continue
		}
		// 解码任务信息。
		var name string
		var args []string
		//name, args := json.loads(packed[1])	//自行实现
		// 没有找到任务指定的回调函数，用日志记录错误并重试。
		if _, ok := callbacks[name]; !ok {
			fmt.Println("Unknown callback ", name)
			continue
		} else {
			callbacks[name](args)
		}
	}
}

//延迟队列 如果任务带有延迟属性则将任务添加到延迟队列中（一个以时间戳为分数的有序集合）
func execute_later(conn *redis.Client, queue string, name string, args string, delay int64) string {
	delay = 0
	// 创建唯一标识符。
	identifier := core.GenID()
	// 准备好需要入队的任务。
	data := map[string]string{
		"identifier": identifier,
		"queue":      queue,
		"name":       name,
		"args":       args,
	}
	bytes, _ := json.Marshal(data)
	item := string(bytes)
	if delay > 0 {
		// 延迟执行这个任务。
		conn.ZAdd("delayed:", &redis.Z{Score: float64(time.Now().Unix() + delay), Member: item})
	} else {
		// 立即执行这个任务。
		conn.RPush("queue:"+queue, item)
	}
	// 返回标识符。
	return identifier
}

//将不同优先级的任务分发到不同的队列以实现优先队列 分发包含的队列名集合和获取传递的队列名集合需要一致
func poll_queue(conn *redis.Client) {
	for !QUIT {
		// 获取队列中的第一个任务。
		item := conn.ZRangeWithScores("delayed:", 0, 0).Val()
		// 队列没有包含任何任务，或者任务的执行时间未到。
		if item == nil || len(item) == 0 || item[0].Score > float64(time.Now().Unix()) {
			time.Sleep(time.Microsecond)
			continue
		}

		// 解码要被执行的任务，弄清楚它应该被推入到哪个任务队列里面。
		item1 := item[0].Member.(string)
		var identifier, queue, _, _ string
		//identifier, queue, function, args := json.loads(item1)

		// 为了对任务进行移动，尝试获取锁。
		locked := acquire_lock(conn, identifier, 0)
		// 获取锁失败，跳过后续步骤并重试。
		if locked == "" {
			continue
		}

		// 将任务推入到适当的任务队列里面。
		if conn.ZRem("delayed:", item1).Val() > 0 {
			conn.RPush("queue:"+queue, item1)
		}
		// 释放锁。
		release_lock(conn, identifier, locked)
	}
}

//将所有参与的人拉到一个有序集合中 并初始化已读信息的有序集合（分数代表当前已读）
func create_chat(conn *redis.Client, sender string, recipients []string, message string, chat_id string) string {
	chat_id = ""
	// 获得新的群组ID。
	if chat_id == "" {
		chat_id = strconv.Itoa(int(conn.Incr("ids:chat:").Val()))
	}
	// 创建一个由用户和分值组成的字典，字典里面的信息将被添加到有序集合里面。
	recipients = append(recipients, sender)
	recipientsd := make([]*redis.Z, len(recipients))
	for i, v := range recipients {
		recipientsd[i] = &redis.Z{Member: v}
	}

	pipeline := conn.Pipeline()
	// 将所有参与群聊的用户添加到有序集合里面。
	pipeline.ZAdd("chat:"+chat_id, recipientsd...)
	// 初始化已读有序集合。
	for _, rec := range recipients {
		pipeline.ZAdd("seen:"+rec, &redis.Z{Score: 0, Member: chat_id})
	}
	pipeline.Exec()

	// 发送消息。
	return send_message(conn, chat_id, sender, message)
}

//发送消息
func send_message(conn *redis.Client, chat_id string, sender string, message string) string {
	identifier := acquire_lock(conn, "chat:"+chat_id, 0)
	if identifier == "" {
		fmt.Println("Couldn't get the lock")
		return ""
	}

	// 筹备待发送的消息。
	mid := conn.Incr("ids:" + chat_id).Val()
	ts := time.Now().Unix()
	packed, _ := json.Marshal(map[string]interface{}{
		"id":      mid,
		"ts":      ts,
		"sender":  sender,
		"message": message,
	})

	// 将消息发送至群组。
	conn.ZAdd("msgs:"+chat_id, &redis.Z{Score: float64(mid), Member: packed})
	release_lock(conn, "chat:"+chat_id, identifier)
	return chat_id
}

type ChatInfo struct {
	chat_id  string
	seen_id  string
	messages []map[string]string
}

//获取最新的消息
func fetch_pending_messages(conn *redis.Client, recipient string) []ChatInfo {
	// 获取最后接收到的消息的ID。
	seen := conn.ZRangeWithScores("seen:"+recipient, 0, -1).Val()

	pipeline := conn.Pipeline()

	// 获取所有未读消息。
	cmds := make([]*redis.StringSliceCmd, len(seen))
	for i, v := range seen {
		chat_id, seen_id := v.Member.(string), v.Score
		cmds[i] = pipeline.ZRangeByScore("msgs:"+chat_id, &redis.ZRangeBy{Min: strconv.Itoa(int(seen_id + 1)), Max: "inf"})
	}
	// 这些数据将被返回给函数调用者。
	//chat_info := zip(seen, pipeline.Exec())
	//chat_info := zip(seen, cmds[i].Val())
	chat_info := []ChatInfo{}

	for _, v := range chat_info {
		if v.messages == nil {
			continue
		}
		chat_id := v.chat_id
		var messages []map[string]interface{}
		//messages[:] := map(json.loads,	messages)
		// 使用最新收到的消息来更新群组有序集合。
		seen_id := messages[len(messages)-1]["id"].(float64)
		conn.ZAdd("chat:"+chat_id, &redis.Z{Score: seen_id, Member: recipient})

		// 找出那些所有人都已经阅读过的消息。
		min_id := conn.ZRangeWithScores("chat:"+chat_id, 0, 0).Val()

		// 更新已读消息有序集合。
		pipeline.ZAdd("seen:"+recipient, &redis.Z{Score: seen_id, Member: chat_id})
		if min_id == nil || len(min_id) == 0 {
			// 清除那些已经被所有人阅读过的消息。
			pipeline.ZRemRangeByScore("msgs:"+chat_id, "0", strconv.Itoa(int(min_id[0].Score)))
		}
		//chat_info[i] = (chat_id, messages)
	}
	pipeline.Exec()
	return chat_info
}

//加入聊天
func join_chat(conn *redis.Client, chat_id string, user string) {
	// 取得最新群组消息的ID。
	message_id, _ := strconv.ParseFloat(conn.Get("ids:"+chat_id).Val(), 10)

	pipeline := conn.Pipeline()
	// 将用户添加到群组成员列表里面。
	pipeline.ZAdd("chat:"+chat_id, &redis.Z{Score: message_id, Member: user})
	// 将群组添加到用户的已读列表里面。
	pipeline.ZAdd("seen:"+user, &redis.Z{Score: message_id, Member: chat_id})
	pipeline.Exec()
}

//离开群组 有序集合chat:chat_id 分数为最小已阅读的消息No 所有从0-No的消息时可以删除的
func leave_chat(conn *redis.Client, chat_id string, user string) {
	pipeline := conn.Pipeline()
	// 从群组里面移除给定的用户。
	pipeline.ZRem("chat:"+chat_id, user)
	pipeline.ZRem("seen:"+user, chat_id)
	// 查看群组剩余成员的数量。
	zCmd := pipeline.ZCard("chat:" + chat_id)
	pipeline.Exec()
	if zCmd.Val() <= 0 {
		// 删除群组。
		pipeline.Del("msgs:" + chat_id)
		pipeline.Del("ids:" + chat_id)
		pipeline.Exec()
	} else {
		// 查找那些已经被所有成员阅读过的消息。
		oldest := conn.ZRangeWithScores("chat:"+chat_id, 0, 0).Val()
		// 删除那些已经被所有成员阅读过的消息。
		conn.ZRemRangeByScore("msgs:"+chat_id, "0", strconv.Itoa(int(oldest[0].Score)))
	}
}

var aggregates map[string]map[string]int

//本地聚合计算回调函数，用于每天以国家维度对日志行进行聚合（一天执行一次 process_logs_from_redis 函数中的参数）
func daily_country_aggregate(conn *redis.Client, line string) {
	if line != "" {
		line := strings.Split(line, "|")
		// 提取日志行中的信息。
		//ip := line[0]
		_ = line[0]
		day := line[1]
		// 根据IP地址判断用户所在国家。
		//country := find_city_by_ip_local(ip)[2]
		country := ""
		// 对本地聚合数据执行自增操作。
		aggregates[day][country] += 1
		return
	}
	// 当天的日志文件已经处理完毕，将聚合计算的结果写入到Redis里面。
	items := make(map[string]string)
	//for	day, aggregate :=range aggregates.items(){
	for day, aggregate := range items {
		conn.ZAdd("daily:country:"+day, &redis.Z{Member: aggregate})
		delete(aggregates, day)
	}
}

type deque struct {
	logfile string
	fsize   int
}

//将日志拷贝到redis中
func copy_logs_to_redis(conn *redis.Client, path string, channel string, count string, limit int, quit_when_done bool) {
	count = "10"
	limit = 300
	quit_when_done = true
	bytes_in_redis := 0
	waiting := make([]deque, 0)
	// 创建用于向客户端发送消息的群组。
	create_chat(conn, "source", nil, "", channel)
	// 遍历所有日志文件。
	var paths []string
	for _, logfile := range paths {
		//for	logfile	in	sorted(os.listdir(path))	{
		//	full_path := os.path.join(path, logfile)
		//fsize := os.stat(full_path).st_size
		fsize := 0
		// 如果程序需要更多空间，那么清除已经处理完毕的文件。
		for bytes_in_redis+fsize > limit {
			count1, _ := strconv.ParseInt(count, 64, 10)
			cleaned := _clean(conn, channel, waiting, count1)
			if cleaned != 0 {
				bytes_in_redis -= cleaned
			} else {
				time.Sleep(250 * time.Microsecond)
			}
		}
		// 将文件上传至Redis。
		//with open(full_path, "rb")	as	inp{
		{
			block := " "
			for block != "" {
				//block = inp.read(2 * *17)
				block = ""
				conn.Append(channel+logfile, block)
			}
		}
		// 提醒监听者，文件已经准备就绪。
		send_message(conn, channel, "source", logfile)

		// 对本地记录的Redis内存占用量相关信息进行更新。
		bytes_in_redis += fsize
		waiting = append(waiting, deque{logfile, fsize})
	}
	// 所有日志文件已经处理完毕，向监听者报告此事。
	if quit_when_done {
		send_message(conn, channel, "source", ":done")
	}
	// 在工作完成之后，清理无用的日志文件。
	for waiting != nil {
		count1, _ := strconv.ParseInt(count, 64, 10)
		cleaned := _clean(conn, channel, waiting, count1)
		if cleaned != 0 {
			bytes_in_redis -= cleaned
		} else {
			time.Sleep(250 * time.Microsecond)
		}
	}
}

// 对Redis进行清理的详细步骤。
func _clean(conn *redis.Client, channel string, waiting []deque, count int64) int {
	if len(waiting) == 0 {
		return 0
	}
	w0 := waiting[0].logfile
	//如果ret和count相等则代表日志被完全分析处理了
	if ret, _ := conn.Get(channel + w0 + ":done").Int64(); ret == count {
		conn.Del(channel+w0, channel+w0+":done")
		return waiting[len(waiting)-1].fsize
	}
	return 0
}

//从redis中处理日志 客户端获取日志  然后在本地进行聚合处理 处理结束后上报给redis
func process_logs_from_redis(conn *redis.Client, id string, callback func(client *redis.Client, line string)) {
	for true {
		// 获取文件列表。
		fdata := fetch_pending_messages(conn, id)

		//for ch, mdata in fdata:
		for ch := range fdata {
			var mdata []map[string]string
			for _, message := range mdata {
				logfile := message["message"]

				// 所有日志行已经处理完毕。
				if logfile == ":done" {
					return
				} else if logfile == "" {
					continue
				}
				// 选择一个块读取器（block reader）。
				//block_reader := readblocks
				//if logfile.endswith(".gz") {
				//	block_reader := readblocks_gz
				//}
				// 遍历日志行。
				lines := make([]string, 0)
				//for line    in    readlines(conn, ch+logfile, block_reader){
				for _, line := range lines {
					// 将日志行传递给回调函数。
					callback(conn, line)
				}
				// 强制地刷新聚合数据缓存。
				callback(conn, "")

				// 报告日志已经处理完毕。
				conn.Incr(strconv.Itoa(ch) + logfile + ":done")
			}
		}
		if fdata == nil {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

//读取行数据
func readlines(conn *redis.Client, key string, rblocks func(client *redis.Client, key string) []string) string {
	out := ""
	for _, block := range rblocks(conn, key) {
		out += block
		// 查找位于文本最右端的断行符；如果断行符不存在，那么rfind()返回-1。
		posn := strings.Index(out, "\n")
		// 找到一个断行符。
		if posn >= 0 {
			// 根据断行符来分割日志行。
			for _, line := range strings.Split(out, "\n") {
				// 向调用者返回每个行。
				return line + "\n"
				//yield line + '\n'	//TODO 对于go如何实现呢
				// 保留余下的数据。
				out = out[posn+1:]
			}
		}
		// 所有数据块已经处理完毕。
		if block == "" {
			break
		}
	}
	return ""
}

//读取数据
func readblocks(conn *redis.Client, key string, blocksize int) []string {
	blocksize = 34
	lb := blocksize
	pos := 0
	// 尽可能地读取更多数据，直到出现不完整读操作（partial read）为止。
	for lb == blocksize {
		// 获取数据块。
		//block := conn.substr(key, pos, pos+blocksize-1)
		block := "" //TODO go的redis客户端没有对应的API
		// 准备进行下一次遍历。
		//yield block
		lb := len(block)
		pos += lb
	}
	//yield ''	//TODO 对于yield go应该如何处理呢
	return nil
}

//读取压缩的代码
func readblocks_gz(conn *redis.Client, key string) {
	//...读取压缩的数据  代码省略
}

func main() {
	core.ClearAllKeys(redisCli)
}
