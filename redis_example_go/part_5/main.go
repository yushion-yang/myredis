package main

import (
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

var QUIT = false
var SAMPLE_COUNT int64 = 100

var config_connection = ""

// 设置一个字典，它可以帮助我们将大部分日志的安全级别转换成某种一致的东西。
const (
	DEBUG    = "debug"
	INFO     = "info"
	WARNING  = "warning"
	ERROR    = "error"
	CRITICAL = "critical"
)

//// 代码清单 5-1
//// <start id:="recent_log"/>
//
func log_recent(conn *redis.Client, name string, message string, severity string, pipe redis.Pipeliner) {
	// 尝试将日志的级别转换成简单的字符串。
	severity = INFO
	// 创建负责存储消息的键。
	destination := "recent:" + name + ":" + severity
	// 将当前时间添加到消息里面，用于记录消息的发送时间。
	message = time.Now().String() + " " + message
	// 使用流水线来将通信往返次数降低为一次。
	if pipe == nil {
		pipe = conn.Pipeline()
	}
	// 将消息添加到日志列表的最前面。
	pipe.LPush(destination, message)
	// 对日志列表进行修剪，让它只包含最新的100条消息。
	pipe.LTrim(destination, 0, 99)
	// 执行两个命令。
	pipe.Exec()
}

// 代码清单 5-2
// <start id:="common_log"/>
//结构 一个小时内的日志使用一个有序集合进行记录 日志行为有序集合的元素 出现的次数为分数
//redis会记录每种级别日志的最近一小时记录并每小时进行一次归档（将有序集合、记录最近记录所处的小时的键重命名 用于查找获取）
//在添加普通日志时也会将日志添加到最新日志记录中
func log_common(conn *redis.Client, name string, message string, severity string, timeout int64) {
	timeout = 10
	// 设置日志的级别。
	severity = INFO
	// 负责存储最新日志的键。
	destination := "common:" + name + ":" + severity
	// 因为程序每小时需要轮换一次日志，所以它使用一个键来记录当前所处的小时数。
	start_key := destination + ":start"
	end := time.Now().Unix() + timeout
	for time.Now().Unix() < end {
		// 对记录当前小时数的键进行监视，确保轮换操作可以正确地执行。
		txf := func(tx *redis.Tx) error {
			// 取得当前时间。
			//now := time.Now()
			// 取得当前所处的小时数。
			//hour_start := datetime(*now[:4]).isoformat()
			hour_start := 10
			existing, _ := tx.Get(start_key).Int()

			_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				// 如果目前的常见日志列表是上一个小时的……
				if existing > 0 && existing < hour_start {
					// ……那么将旧的常见日志信息进行归档。
					pipe.Rename(destination, destination+":last")
					pipe.Rename(start_key, destination+":pstart")
					// 更新当前所处的小时数。
					pipe.Set(start_key, hour_start, 0)

					// 对记录日志出现次数的计数器执行自增操作。
					pipe.ZIncrBy(destination, 1, message)
					// log_recent()函数负责记录日志并调用execute()函数。
					log_recent(conn, name, message, severity, pipe)
				}
				return nil
			})
			return err
		}
		//如果程序因为其他客户端在执行归档操作而出现监视错误，那么重试。
		err := conn.Watch(txf, start_key)
		if err != redis.TxFailedErr {
			fmt.Println("err:", err)
			return
		} else if err == nil {
			return
		}
	}
	return
}

// <end id:="common_log"/>

// 代码清单 5-3
// <start id:="update_counter"/>
// 以秒为单位的计数器精度，分别为1秒钟、5秒钟、1分钟、5分钟、1小时、5小时、1天——用户可以按需调整这些精度。
//结构每个精度都对应一个散列 键为时间片 值为计数器数值
var PRECISION = []int64{1, 5, 60, 300, 3600, 18000, 86400} //A

//按照不同的时间频率来更新计数器
func update_counter(conn *redis.Client, name string, count int64, now int64) {
	count = 1
	// 通过取得当前时间来判断应该对哪个时间片执行自增操作。
	now = time.Now().Unix()
	// 为了保证之后的清理工作可以正确地执行，这里需要创建一个事务型流水线。
	pipe := conn.Pipeline()
	// 为我们记录的每种精度都创建一个计数器。
	for _, prec := range PRECISION {
		// 取得当前时间片的开始时间。
		pnow := (now / prec) * prec
		// 创建负责存储计数信息的散列。
		hash := fmt.Sprintf("%v:%v", prec, name)
		// 将计数器的引用信息添加到有序集合里面，
		// 并将其分值设置为0，以便在之后执行清理操作。
		pipe.ZAdd("known:", redis.Z{Member: hash})
		// 对给定名字和精度的计数器进行更新。
		pipe.HIncrBy("count:"+hash, strconv.Itoa(int(pnow)), count)
	}
	pipe.Exec()
}

//
func get_counter(conn *redis.Client, name string, precision int) {
	// 取得存储着计数器数据的键的名字。
	//hash := fmt.Sprintf("%v:%v",precision, name)
	// 从Redis里面取出计数器数据。
	//data := conn.HGetAll("count:" + hash).Val()
	// 将计数器数据转换成指定的格式。
	//to_return := make([]int,0)
	//for key, value :=range data	{
	//to_return = append( (int(key), int(value)))
	// 对数据进行排序，把旧的数据样本排在前面。
	//}
	//to_return.sort()
	//return to_return
}

//清除计数器
func clean_counters(conn *redis.Client) {
	// 为了平等地处理更新频率各不相同的多个计数器，程序需要记录清理操作执行的次数。
	passes := 0
	// 持续地对计数器进行清理，直到退出为止。
	for !QUIT {
		// 记录清理操作开始执行的时间，用于计算清理操作执行的时长。
		start := time.Now().Unix()
		// 渐进地遍历所有已知的计数器。
		var index int64 = 0
		for index < conn.ZCard("known:").Val() {
			// 取得被检查计数器的数据。
			hash := conn.ZRange("known:", index, index).Val()
			index += 1
			if len(hash) == 0 {
				break
			}
			//hash = hash[0]
			// 取得计数器的精度。
			//prec := int(hash.partition(":")[0])
			//var prec int64 = 60
			// 因为清理程序每60秒钟就会循环一次，
			// 所以这里需要根据计数器的更新频率来判断是否真的有必要对计数器进行清理。
			//bprec := int(prec // 60) or 1
			bprec := 1
			// 如果这个计数器在这次循环里不需要进行清理，
			// 那么检查下一个计数器。
			// （举个例子，如果清理程序只循环了三次，而计数器的更新频率为每5分钟一次，
			// 那么程序暂时还不需要对这个计数器进行清理。）
			if passes%bprec != 0 {
				continue
			}
			//hkey := "count:" + hash
			hkey := "count:" + "hash"
			// 根据给定的精度以及需要保留的样本数量，
			// 计算出我们需要保留什么时间之前的样本。
			//cutoff := time.Now().Unix() - SAMPLE_COUNT * prec
			// 获取样本的开始时间，并将其从字符串转换为整数。
			//samples := map(int, conn.hkeys(hkey))
			var samples map[int]string
			// 计算出需要移除的样本数量。
			//samples.sort()
			//remove := bisect.bisect_right(samples, cutoff)
			remove := 1

			// 按需移除计数样本。
			if remove > 0 {
				conn.HDel(hkey, samples[remove])
				// 这个散列可能已经被清空。
				if remove == len(samples) {
					// 在尝试修改计数器散列之前，对其进行监视。
					txf := func(tx *redis.Tx) error {
						// 验证计数器散列是否为空，如果是的话，
						// 那么从记录已知计数器的有序集合里面移除它。
						// 计数器散列并不为空，
						// 继续让它留在记录已有计数器的有序集合里面。
						if tx.HLen(hkey).Val() == 0 {
							return errors.New("pipe.HLen(hkey).Val()==0")
						}
						_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
							pipe.ZRem("known:", hash)
							// 在删除了一个计数器的情况下，
							// 下次循环可以使用与本次循环相同的索引。
							index -= 1
							return nil
						})
						return err
					}
					// 有其他程序向这个计算器散列添加了新的数据，
					// 它已经不再是空的了，继续让它留在记录已知计数器的有序集合里面。
					err := conn.Watch(txf, hkey)
					if err != redis.TxFailedErr {
						fmt.Println("err:", err)
						break
					} else if err == nil {
						break
					}
				}
			}
		}
		// 为了让清理操作的执行频率与计数器更新的频率保持一致，
		// 对记录循环次数的变量以及记录执行时长的变量进行更新。
		passes += 1
		duration := math.Min(float64(time.Now().Unix()-start)+1, 60)
		// 如果这次循环未耗尽60秒钟，那么在余下的时间内进行休眠；
		// 如果60秒钟已经耗尽，那么休眠一秒钟以便稍作休息。
		time.Sleep(time.Second * time.Duration(math.Max(float64(60-duration), 1)))
	}
}

// 代码清单 5-6
//更新状态
func update_stats(conn *redis.Client, context string, type_ string, value int64, timeout int64) float64 {
	timeout = 5
	// 设置用于存储统计数据的键。
	destination := fmt.Sprintf("stats:%v:%v", context, type_)
	// 像common_log()函数一样，
	// 处理当前这一个小时的数据和上一个小时的数据。
	start_key := destination + ":start"
	end := time.Now().Unix() + timeout
	var retCmd *redis.FloatCmd
	for time.Now().Unix() < end {
		txf := func(tx *redis.Tx) error {
			//now := time.Now().Unix()
			hour_start := time.Now().Hour()
			existing, _ := tx.Get(start_key).Int()
			_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				if existing > 0 && existing < hour_start {
					pipe.Rename(destination, destination+":last")
					pipe.Rename(start_key, destination+":pstart")
					pipe.Set(start_key, hour_start, 0)
				}
				tkey1 := core.GenID()
				tkey2 := core.GenID()
				// 将值添加到临时键里面。
				pipe.ZAdd(tkey1, redis.Z{Score: float64(value), Member: "min"})
				pipe.ZAdd(tkey2, redis.Z{Score: float64(value), Member: "max"})
				// 使用合适聚合函数MIN和MAX，
				// 对存储统计数据的键和两个临时键进行并集计算。
				pipe.ZUnionStore(destination, redis.ZStore{Aggregate: "MIN"}, []string{destination, tkey1}...)
				pipe.ZUnionStore(destination, redis.ZStore{Aggregate: "MAX"}, []string{destination, tkey2}...)

				// 删除临时键。
				pipe.Del(tkey1, tkey2)
				// 对有序集合中的样本数量、值的和、值的平方之和三个成员进行更新。
				retCmd = pipe.ZIncrBy(destination, 1, "count")
				pipe.ZIncrBy(destination, float64(value), "sum")
				pipe.ZIncrBy(destination, float64(value*value), "sumsq")

				// 返回基本的计数信息，以便函数调用者在有需要时做进一步的处理。
				return nil
			})
			return err
		}
		err := conn.Watch(txf, start_key)
		if err != redis.TxFailedErr {
			fmt.Println("err:", err)
			return 0
		} else if err == nil {
			return 0
		}
		return retCmd.Val()
	}
	return 0
}

// 代码清单 5-7
// <start id:="get_stats"/>
//获取状态
func get_stats(conn *redis.Client, context string, type_ string) map[string]int {
	// 程序将从这个键里面取出统计数据。
	//key := fmt.Sprintf("stats:%v:%v",context, type_)
	_ = fmt.Sprintf("stats:%v:%v", context, type_)
	// 获取基本的统计数据，并将它们都放到一个字典里面。
	//data := dict(conn.zrange(key, 0, -1, withscores:=True))
	var data map[string]int
	// 计算平均值。
	data["average"] = data["sum"] / data["count"]
	// 计算标准差的第一个步骤。
	numerator := data["sumsq"] - data["sum"]*2/data["count"]
	// 完成标准差的计算工作。
	if data["count"]-1 == 0 {
		data["stddev"] = (numerator / (1)) * 5
	} else {
		data["stddev"] = (numerator / (data["count"] - 1)) * 5
	}
	return data
}

// 代码清单 5-8
// <start id:="access_time_context_manager"/>
// 将这个Python生成器用作上下文管理器。
func access_time(conn *redis.Client, context string) {
	// 记录代码块执行前的时间。
	start := time.Now().Unix()
	// 运行被包裹的代码块。

	// 计算代码块的执行时长。
	delta := time.Now().Unix() - start
	// 更新这一上下文的统计数据。
	//stats := update_stats(conn, context, "AccessTime", delta,0)
	_ = update_stats(conn, context, "AccessTime", delta, 0)
	// 计算页面的平均访问时长。
	//average := stats[1] / stats[0]
	var average float64 = 0

	pipe := conn.Pipeline()
	// 将页面的平均访问时长添加到记录最慢访问时间的有序集合里面。
	pipe.ZAdd("slowest:AccessTime", redis.Z{Score: average, Member: context})
	// AccessTime有序集合只会保留最慢的100条记录。
	pipe.ZRemRangeByRank("slowest:AccessTime", 0, -101)
	pipe.Exec()
}

// <start id:="access_time_use"/>
// 这个视图（view）接受一个Redis连接以及一个生成内容的回调函数为参数。
func process_view(conn *redis.Client, callback func()) func() {
	// 计算并记录访问时长的上下文管理器就是这样包围代码块的。
	//with access_time(conn, request.path):
	// 当上下文管理器中的yield语句被执行时，这个语句就会被执行。
	return callback
}

// 代码清单 5-9
// <start id:="_1314_14473_9188"/>
//将ip作为分数值
func ip_to_score(ip_address string) int64 {
	var score int64 = 0
	for _, v := range strings.Split(ip_address, ".") {
		n, _ := strconv.ParseInt(v, 64, 10)
		score = score*256 + n
	}
	return score
}

// 代码清单 5-10
// <start id:="_1314_14473_9191"/>
// 这个函数在执行时需要给定GeoLiteCity-Blocks.csv文件所在的位置。
//将 csv文件的城市和ip导入到redis中
func import_ips_to_redis(conn *redis.Client, filename string) {
	//csv_file := csv.reader(open(filename, "rb"))
	//for count, row in enumerate(csv_file):
	//// 按需将IP地址转换为分值。
	//start_ip := row[0] if row else ''
	//if "i" in start_ip.lower():
	//	continue
	//if "." in start_ip:
	//	start_ip := ip_to_score(start_ip)
	//	elif start_ip.isdigit():
	//	start_ip := int(start_ip, 10)
	//else:
	//	// 略过文件的第一行以及格式不正确的条目。
	//	continue
	//
	//// 构建唯一城市ID。
	//city_id := row[2] + "_" + str(count)
	//// 将城市ID及其对应的IP地址分值添加到有序集合里面。
	//conn.zadd("ip2cityid:", city_id, start_ip)
}

// 代码清单 5-11
// <start id:="_1314_14473_9194"/>
// 这个函数在执行时需要给定GeoLiteCity-Location.csv文件所在的位置。
//将城市信息导入到redis中
func import_cities_to_redis(conn *redis.Client, filename string) {
	//for row in csv.reader(open(filename, "rb")):
	//if len(row) < 4 or not row[0].isdigit():
	//	continue
	//row := [i.decode("latin-1") for i in row]
	//// 准备好需要添加到散列里面的信息。
	//city_id := row[0]
	//country := row[1]
	//region := row[2]
	//city := row[3]
	//// 将城市信息添加到Redis里面。
	//conn.hset("cityid2city:", city_id,
	//json.dumps([city, region, country]))
}

// 代码清单 5-12
// <start id:="_1314_14473_9197"/>
//通过ip查找城市
func find_city_by_ip(conn *redis.Client, ip_address string) {
	// 将IP地址转换为分值以便执行ZREVRANGEBYSCORE命令。
	//if isinstance(ip_address, str) { //A
	//	ip_address := ip_to_score(ip_address) //A
	//}
	//// 查找唯一城市ID。
	//city_id := conn.zrevrangebyscore(                       //B
	//"ip2cityid:", ip_address, 0, start:=0, num:=1)       //B
	//
	//if not city_id:
	//return None
	//
	//// 将唯一城市ID转换为普通城市ID。
	//city_id := city_id[0].partition("_")[0]                 //C
	//// 从散列里面取出城市信息。
	//return json.loads(conn.hget("cityid2city:", city_id))  //D
}

// 代码清单 5-13
// <start id:="is_under_maintenance"/>
var LAST_CHECKED int64 = 0
var IS_UNDER_MAINTENANCE = false

//判断是否在维护状态
func is_under_maintenance(conn *redis.Client) bool {
	// 将两个变量设置为全局变量以便在之后对它们进行写入。
	// 距离上次检查是否已经超过1秒钟？
	if LAST_CHECKED < time.Now().Unix()-1 {
		//B
		// 更新最后检查时间。
		LAST_CHECKED = time.Now().Unix() //C
	}
	// 检查系统是否正在进行维护。
	ret, _ := conn.Get("is-under-maintenance").Int()
	//IS_UNDER_MAINTENANCE = 	ret>0   //D

	// 返回一个布尔值，用于表示系统是否正在进行维护。
	return ret > 0 //E
}

// 代码清单 5-14
// <start id:="set_config"/>
//设置配置
func set_config(conn *redis.Client, type_ string, component string, config string) {
	//conn.Set(fmt.Sprintf("config:%v:%v",type_, component),"" json.dumps(config))
	conn.Set(fmt.Sprintf("config:%v:%v", type_, component), "", 0)
}

// 代码清单 5-15
// <start id:="get_config"/>
//CONFIGS := {}
//CHECKED := {}
//获取配置	从redis中获取组件所使用的配置 然后跟当前使用的配置进行比对 不一致则更新
func get_config(conn *redis.Client, type_ string, component string, wait int) {
	//wait =1
	//key := fmt.Sprintf("config:%v:%v",type_, component)
	//
	//// 检查是否需要对这个组件的配置信息进行更新。
	//if CHECKED.get(key) < time.time() - wait:
	//// 有需要对配置进行更新，记录最后一次检查这个连接的时间。
	//CHECKED[key] := time.time()
	//// 取得Redis存储的组件配置。
	//config := json.loads(conn.get(key) or "{}")
	//// 将潜在的Unicode关键字参数转换为字符串关键字参数。
	//config := dict((str(k), config[k]) for k in config)
	//// 取得组件正在使用的配置。
	//old_config := CONFIGS.get(key)
	//
	//// 如果两个配置并不相同……
	//if config !:= old_config:
	//// ……那么对组件的配置进行更新。
	//CONFIGS[key] := config
	//
	//return CONFIGS.get(key)
}

// 代码清单 5-16
// <start id:="redis_connection"/>
//REDIS_CONNECTIONS := {}
//
//// 将应用组件的名字传递给装饰器。
//func redis_connection(component string, wait:=1):                        //A
//// 因为函数每次被调用都需要获取这个配置键，所以我们干脆把它缓存起来。
//key := "config:redis:" + component                           //B
//// 包装器接受一个函数作为参数，并使用另一个函数来包裹这个函数。
//
//func wrapper(function):                                      //C
//// 将被包裹函数里的一些有用的元数据复制到配置处理器。
//
//@functools.wraps(function)                              //D
//// 创建负责管理连接信息的函数。
//func call(*args, **kwargs):                              //E
//// 如果有旧配置存在，那么获取它。
//old_config := CONFIGS.get(key, object())             //F
//// 如果有新配置存在，那么获取它。
//_config := get_config(                               //G
//config_connection, "redis", component, wait)    //G
//
//config := {}
//// 对配置进行处理并将其用于创建Redis连接。
//for k, v in _config.iteritems():                    //L
//config[k.encode("utf-8")] := v                   //L
//
//// 如果新旧配置并不相同，那么创建新的连接。
//if config !:= old_config:                            //H
//REDIS_CONNECTIONS[key] := redis.Redis(**config)  //H
//
//// 将Redis连接以及其他匹配的参数传递给被包裹函数，然后调用函数并返回执行结果。
//return function(                                    //I
//REDIS_CONNECTIONS.get(key), *args, **kwargs)    //I
//// 返回被包裹的函数。
//return call                                             //J
//// 返回用于包裹Redis函数的包装器。
//return wrapper                                              //K
//}
//
//
//// 代码清单 5-17
//'''
//// <start id:="recent_log_decorator"/>
//@redis_connection("logs")                   // redis_connection()装饰器非常容易使用。
//func log_recent1(conn, app, message):         // 这个函数的定义和之前展示的一样，没有发生任何变化。
//"the old log_recent() code"
//
//log_recent("main", "User 235 logged in")    // 我们再也不必在调用log_recent()函数时手动地向它传递日志服务器的连接了。
//// <end id:="recent_log_decorator"/>
//'''

func main() {
	core.ClearAllKeys(redisCli)
}
