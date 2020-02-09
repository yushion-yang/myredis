package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"redis-learn/core"
	"strconv"
	"time"
)

var redisCli *redis.Client

func main() {
	Test(redisCli)
	core.ClearAllKeys(redisCli)
}

//初始化连接
func init() {
	redisCli = core.InitRedis("127.0.0.1:6379", "", 0)
}

//交 并 差  运算  使用redis的sort功能
//搜索原理 反向查找：使用网页内容关键词建立网页索引 通过关键词查找获取到相关网页id集合
//然后对多个集合进行并集 交集 差集的计算最终得出想要的结果

//可以使用有序集合记录字符串的分值来对字符串进行排序
func Test(conn *redis.Client) {
	items1 := []string{"item1", "item2", "item3"}
	items2 := []string{"item1", "item2", "item4"}
	items3 := []string{"item2", "item3"}
	conn.SAdd("set1", items1)
	conn.SAdd("set2", items2)
	conn.SAdd("set3", items3)

	fmt.Println("conn.SInter set1,set2 :", conn.SInter("set1", "set2").Val())
	fmt.Println("conn.SInter set1,set3 :", conn.SInter("set1", "set3").Val())
	fmt.Println("conn.SInter set2,set3 :", conn.SInter("set2", "set3").Val())
	conn.SInterStore("destination", "set1", "set2")
	fmt.Println("conn.SUnion set1,set2 :", conn.SUnion("set1", "set2").Val())
	fmt.Println("conn.SUnion set1,set3 :", conn.SUnion("set1", "set3").Val())
	fmt.Println("conn.SUnion set2,set3 :", conn.SUnion("set2", "set3").Val())
	conn.SUnionStore("destination", "set1", "set2")
	fmt.Println("conn.SDiff set1,set2 :", conn.SDiff("set1", "set2").Val())
	fmt.Println("conn.SDiff set2,set1 :", conn.SDiff("set2", "set1").Val())
	fmt.Println("conn.SDiff set1,set3 :", conn.SDiff("set1", "set3").Val())
	fmt.Println("conn.SDiff set3,set1 :", conn.SDiff("set3", "set1").Val())
	fmt.Println("conn.SDiff set2,set3 :", conn.SDiff("set2", "set3").Val())
	fmt.Println("conn.SDiff set3,set2 :", conn.SDiff("set3", "set2").Val())
	conn.SDiffStore("destination", "set1", "set2")

	now := time.Now().Unix()

	datas := [][]struct {
		score  float64
		member string
	}{
		{ //点击数量的有序集合
			{555, "item1"},
			{333, "item2"},
			{444, "item3"},
			{4552, "item4"},
		},
		{ //排名的有序集合
			{99, "item1"},
			{88, "item2"},
			{55, "item4"},
		},
		{ //时间戳的有序集合
			{float64(now + 10), "item1"},
			{float64(now + 30), "item3"},
			{float64(now + 50), "item4"},
		},
		{ //需求的技能熟练度
			{-10, "item1"},
			{-20, "item2"},
			{-100, "item3"},
		},
		{ //掌握的技能熟练度
			{20, "item1"},
			{40, "item2"},
			{200, "item3"},
			{60, "item4"},
		},
	}

	for no, arr := range datas {
		for _, v := range arr {
			keyName := "zset" + strconv.Itoa(no)
			conn.ZAdd(keyName, &redis.Z{Score: v.score, Member: v.member})
		}
	}
	base := []string{"item1", "item2", "item3", "item4"}
	conn.SAdd("base", base)

	zustores := []*redis.ZStore{
		{ //按点击数量排序
			Keys:    []string{"base", "zset0"},
			Weights: []float64{},
			//Aggregate: "",
		},
		{ //按排名排序
			Keys:    []string{"base", "zset1"},
			Weights: []float64{},
			//Aggregate: "",
		},
		{ //按时间戳排序
			Keys:    []string{"base", "zset2"},
			Weights: []float64{},
			//Aggregate: "",
		},
		{ //技能熟练度的合并（分析契合度）
			Keys:    []string{"zset3", "zset4"},
			Weights: []float64{},
			//Aggregate: "",
		},
	}
	aggregates := []string{"SUM", "MIN", "MAX"}
	for _, v := range zustores {
		for _, agg := range aggregates {
			v.Aggregate = agg
			conn.ZUnionStore("destination", v)
			fmt.Println("conn.ZUnionStore keys:", v.Keys, " aggregates:", agg, " ret:", conn.ZRangeWithScores("destination", 0, -1).Val())
		}
	}

	zistores := []*redis.ZStore{
		{ //按点击数量排序
			Keys:    []string{"base", "zset0"},
			Weights: []float64{},
			//Aggregate: "",
		},
		{ //按排名排序
			Keys:    []string{"base", "zset1"},
			Weights: []float64{},
			//Aggregate: "",
		},
		{ //按时间戳排序
			Keys:    []string{"base", "zset2"},
			Weights: []float64{},
			//Aggregate: "",
		},
		{ //技能熟练度的合并（分析契合度）
			Keys:    []string{"zset3", "zset4"},
			Weights: []float64{},
			//Aggregate: "",
		},
	}

	for _, v := range zistores {
		for _, agg := range aggregates {
			v.Aggregate = agg
			conn.ZInterStore("destination", v)
			fmt.Println("conn.ZInterStore keys:", v.Keys, " aggregates:", agg, " ret:", conn.ZRangeWithScores("destination", 0, -1).Val())
		}
	}

	//通过将一个不存在的键作为参数传给 BY 选项， 可以让 SORT 跳过排序操作， 直接返回结果
	//这种用法和 GET 选项配合， 就可以在不排序的情况下， 获取多个外部键， 相当于执行一个整合的获取操作（类似于 SQL 数据库的 join 关键字）。
	//将具备某个前缀的一堆键名的后半部分存储在一个集合中，通过sort的这个方法可以一次性拿取到所有键值
	conn.Sort("key", &redis.Sort{
		//By            string	//通过使用 BY 选项，可以让按其他键的元素来排序 例如user_level_{uid} SORT uid BY user_level_*
		//甚至可以将哈希作为外部键来使用SORT uid BY user_info_*->level
		//Offset, Count int64	//LIMIT 修饰符限制返回结果
		//Get           []string	//1使用 GET 选项， 可以根据排序的结果来取出相应的键值。2可以同时使用多个 GET 选项，
		// 获取多个外部键的值。3GET 有一个额外的参数规则，那就是 —— 可以用 # 获取被排序键的值
		//Order         string	//SORT key DESC ：
		//Alpha         bool	//使用 ALPHA 修饰符对字符串进行排序
	})
	temps := []string{"sfsfys", "yys", "zdfdswe", "zdfdsweersfs"}
	for i, v := range base {
		conn.Set(v, temps[i], 0)
	}
	//TODO 不是string类型不行!
	nums := []string{"4", "2", "3", "1"}
	conn.SAdd("temp", nums)
	cint := conn.SortStore("temp", "store", &redis.Sort{
		Get:   []string{"#", "item*"},
		By:    "item*",
		Alpha: true,
	}).Val()
	fmt.Println("cint:", cint)
	//fmt.Println("store type:",conn.Type("store"))
	//sort temp by item* get item* get # alpha
	fmt.Println("store:", conn.LRange("store", 0, -1).Val())
}

//
//
//var AVERAGE_PER_1K = {}
//
//// 代码清单 7-1
//// <start id="tokenize-and-index"/>
//// 预先定义好从网上获取的停止词。
//STOP_WORDS = set('''able about across after all almost also am among
//an and any are as at be because been but by can cannot could dear did
//do does either else ever every for from get got had has have he her
//hers him his how however if in into is it its just least let like
//likely may me might most must my neither no nor not of off often on
//only or other our own rather said say says she should since so some
//than that the their them then there these they this tis to too twas us
//wants was we were what when where which while who whom why will with
//would yet you your'''.split())
//
////  根据定义提取单词的正则表达式。
//WORDS_RE = re.compile("[a-z"]{2,}")
//
//func tokenize(content):
//// 将文章包含的单词储存到 Python 集合里面。
//words = set()
//// 遍历文章包含的所有单词。
//for match in WORDS_RE.finditer(content.lower()):
//// 剔除所有位于单词前面或后面的单引号。
//word = match.group().strip(""")
//// 保留那些至少有两个字符长的单词。
//if len(word) >= 2:
//words.add(word)
//// 返回一个集合，集合里面包含了所有被保留并且不是停止词的单词。
//return words - STOP_WORDS
//
//func index_document(conn, docid, content):
//// 对内容进行标记化处理，并取得处理产生的单词。
//words = tokenize(content)
//
//pipeline = conn.pipeline(True)
//// 将文章添加到正确的反向索引集合里面。
//for word in words:
//pipeline.sadd("idx:" + word, docid)
//// 计算一下，程序为这篇文章添加了多少个独一无二并且不是停止词的单词。
//return len(pipeline.execute())
//// <end id="tokenize-and-index"/>
//
//
//// 代码清单 7-2
//// <start id="_1314_14473_9158"/>
//func _set_common(conn, method, names, ttl=30, execute=True):
//// 创建一个新的临时标识符。
//id = str(uuid.uuid4())
//// 设置事务流水线，确保每个调用都能获得一致的执行结果。
//pipeline = conn.pipeline(True) if execute else conn
//// 给每个单词加上 "idx:" 前缀。
//names = ["idx:" + name for name in names]
//// 为将要执行的集合操作设置相应的参数。
//getattr(pipeline, method)("idx:" + id, *names)
//// 吩咐 Redis 在将来自动删除这个集合。
//pipeline.expire("idx:" + id, ttl)
//if execute:
//// 实际地执行操作。
//pipeline.execute()
//// 将结果集合的 ID 返回给调用者，以便做进一步的处理。
//return id
//
//// 执行交集计算的辅助函数。
//func intersect(conn, items, ttl=30, _execute=True):
//return _set_common(conn, "sinterstore", items, ttl, _execute)
//
//// 执行并集计算的辅助函数。
//func union(conn, items, ttl=30, _execute=True):
//return _set_common(conn, "sunionstore", items, ttl, _execute)
//
//// 执行差集计算的辅助函数。
//func difference(conn, items, ttl=30, _execute=True):
//return _set_common(conn, "sdiffstore", items, ttl, _execute)
//// <end id="_1314_14473_9158"/>
//
//
//// 代码清单 7-3
//// <start id="parse-query"/>
//// 查找需要的单词、不需要的单词以及同义词的正则表达式。
//QUERY_RE = re.compile("[+-]?[a-z"]{2,}")
//
//func parse(query):
//// 这个集合将用于储存不需要的单词。
//unwanted = set()
//// 这个列表将用于储存需要执行交集计算的单词。
//all = []
//// 这个集合将用于储存目前已发现的同义词。
//current = set()
//// 遍历搜索查询语句中的所有单词。
//for match in QUERY_RE.finditer(query.lower()):
//// 检查单词是否带有 + 号前缀或 - 号前缀。
//word = match.group()
//prefix = word[:1]
//if prefix in "+-":
//word = word[1:]
//else:
//prefix = None
//
//// 剔除所有位于单词前面或者后面的单引号，并略过所有停止词。
//word = word.strip(""")
//if len(word) < 2 or word in STOP_WORDS:
//continue
//
//// 如果这是一个不需要的单词，
//// 那么将它添加到储存不需要单词的集合里面。
//if prefix == "-":
//unwanted.add(word)
//continue
//
//// 如果在同义词集合非空的情况下，
//// 遇到了一个不带 + 号前缀的单词，
//// 那么创建一个新的同义词集合。
//if current and not prefix:
//all.append(list(current))
//current = set()
//current.add(word)
//
//// 将正在处理的单词添加到同义词集合里面。
//if current:
//all.append(list(current))
//
//// 把所有剩余的单词都放到最后的交集计算里面进行处理。
//return all, list(unwanted)
//// <end id="parse-query"/>
//
//
//// 代码清单 7-4
//// <start id="search-query"/>
//func parse_and_search(conn, query, ttl=30):
//// 对查询语句进行分析。
//all, unwanted = parse(query)
//// 如果查询语句只包含停止词，那么这次搜索没有任何结果。
//if not all:
//return None
//
//to_intersect = []
//// 遍历各个同义词列表。
//for syn in all:
//// 如果同义词列表包含的单词不止一个，那么执行并集计算。
//if len(syn) > 1:
//to_intersect.append(union(conn, syn, ttl=ttl))
//// 如果同义词列表只包含一个单词，那么直接使用这个单词。
//else:
//to_intersect.append(syn[0])
//
//// 如果单词（或者并集计算的结果）有不止一个，那么执行交集计算。
//if len(to_intersect) > 1:
//intersect_result = intersect(conn, to_intersect, ttl=ttl)
//// 如果单词（或者并集计算的结果）只有一个，那么将它用作交集计算的结果。
//else:
//intersect_result = to_intersect[0]
//
//// 如果用户给定了不需要的单词，
//// 那么从交集计算结果里面移除包含这些单词的文章，然后返回搜索结果。
//if unwanted:
//unwanted.insert(0, intersect_result)
//return difference(conn, unwanted, ttl=ttl)
//
//// 如果用户没有给定不需要的单词，那么直接返回交集计算的结果作为搜索的结果。
//return intersect_result
//// <end id="search-query"/>
//
//
//// 代码清单 7-5
//// <start id="sorted-searches"/>
//// 用户可以通过可选的参数来传入已有的搜索结果、指定搜索结果的排序方式，并对结果进行分页。
//func search_and_sort(conn, query, id=None, ttl=300, sort="-updated",
//start=0, num=20):
//// 决定基于文章的哪个属性进行排序，以及是进行升序排序还是降序排序。
//desc = sort.startswith("-")
//sort = sort.lstrip("-")
//by = "kb:doc:*->" + sort
//// 告知 Redis ，排序是以数值方式进行还是字母方式进行。
//alpha = sort not in ("updated", "id", "created")
//
//// 如果用户给定了已有的搜索结果，
//// 并且这个结果仍然存在的话，
//// 那么延长它的生存时间。
//if id and not conn.expire(id, ttl):
//id = None
//
//// 如果用户没有给定已有的搜索结果，
//// 或者给定的搜索结果已经过期，
//// 那么执行一次新的搜索操作。
//if not id:
//id = parse_and_search(conn, query, ttl=ttl)
//
//pipeline = conn.pipeline(True)
//// 获取结果集合的元素数量。
//pipeline.scard("idx:" + id)
//// 根据指定属性对结果进行排序，并且只获取用户指定的那一部分结果。
//pipeline.sort("idx:" + id, by=by, alpha=alpha,
//desc=desc, start=start, num=num)
//results = pipeline.execute()
//
//// 返回搜索结果包含的元素数量、搜索结果本身以及搜索结果的 ID ，
//// 其中搜索结果的 ID 可以用于在之后再次获取本次搜索的结果。
//return results[0], results[1], id
//// <end id="sorted-searches"/>
//
//
//// 代码清单 7-6
//// <start id="zset_scored_composite"/>
//// 和之前一样，函数接受一个已有搜索结果的 ID 作为可选参数，
//// 以便在结果仍然可用的情况下，对其进行分页。
//func search_and_zsort(conn, query, id=None, ttl=300, update=1, vote=0,
//start=0, num=20, desc=True):
//
//// 尝试更新已有搜索结果的生存时间。
//if id and not conn.expire(id, ttl):
//id = None
//
//// 如果传入的结果已经过期，
//// 或者这是函数第一次进行搜索，
//// 那么执行标准的集合搜索操作。
//if not id:
//id = parse_and_search(conn, query, ttl=ttl)
//
//scored_search = {
//// 函数在计算并集的时候也会用到传入的 ID 键，
//// 但这个键不会被用作排序权重（weight）。
//id: 0,
//// 对文章评分进行调整以平衡更新时间和投票数量。
//// 根据待排序数据的需要，投票数量可以被调整为 1 、10 、100 ，甚至更高。
//"sort:update": update,
//"sort:votes": vote
//}
//// 使用代码清单 7-7 定义的辅助函数执行交集计算。
//id = zintersect(conn, scored_search, ttl)
//
//pipeline = conn.pipeline(True)
//// 获取结果有序集合的大小。
//pipeline.zcard("idx:" + id)
////  从搜索结果里面取出一页（page）。
//if desc:
//pipeline.zrevrange("idx:" + id, start, start + num - 1)
//else:
//pipeline.zrange("idx:" + id, start, start + num - 1)
//results = pipeline.execute()
//
//// 返回搜索结果，以及分页用的 ID 值。
//return results[0], results[1], id
//// <end id="zset_scored_composite"/>
//
//
//// 代码清单 7-7
//// <start id="zset_helpers"/>
//func _zset_common(conn, method, scores, ttl=30, **kw):
//// 创建一个新的临时标识符。
//id = str(uuid.uuid4())
//// 调用者可以通过传递参数来决定是否使用事务流水线。
//execute = kw.pop("_execute", True)
//// 设置事务流水线，保证每个单独的调用都有一致的结果。
//pipeline = conn.pipeline(True) if execute else conn
//// 为输入的键添加 ‘idx:’ 前缀。
//for key in scores.keys():
//scores["idx:" + key] = scores.pop(key)
//// 为将要被执行的操作设置好相应的参数。
//getattr(pipeline, method)("idx:" + id, scores, **kw)
//// 为计算结果有序集合设置过期时间。
//pipeline.expire("idx:" + id, ttl)
//// 除非调用者明确指示要延迟执行操作，否则实际地执行计算操作。
//if execute:
//pipeline.execute()
//// 将计算结果的 ID 返回给调用者，以便做进一步的处理。
//return id
//
//// 对有序集合执行交集计算的辅助函数。
//func zintersect(conn, items, ttl=30, **kw):
//return _zset_common(conn, "zinterstore", dict(items), ttl, **kw)
//
//// 对有序集合执行并集计算的辅助函数。
//func zunion(conn, items, ttl=30, **kw):
//return _zset_common(conn, "zunionstore", dict(items), ttl, **kw)
//// <end id="zset_helpers"/>
//
//
//
//// 代码清单 7-8
//// <start id="string-to-score"/>
//func string_to_score(string, ignore_case=False):
//// 用户可以通过参数来决定是否以大小写无关的方式建立前缀索引。
//if ignore_case:
//string = string.lower()
//
//// 将字符串的前 6 个字符转换为相应的数字值，
//// 比如把空字符转换为 0 、制表符（tab）转换为 9 、大写 A 转换为 65 ，
//// 诸如此类。
//pieces = map(ord, string[:6])
//// 为长度不足 6 个字符的字符串添加占位符，以此来表示这是一个短字符。
//while len(pieces) < 6:
//pieces.append(-1)
//
//score = 0
//// 对字符串进行转换得出的每个值都会被计算到分值里面，
//// 并且程序处理空字符的方式和处理占位符的方式并不相同。
//for piece in pieces:
//score = score * 257 + piece + 1
//
//// 通过多使用一个二进制位，
//// 程序可以表明字符串是否正好为 6 个字符长，
//// 这样它就可以正确地区分出 “robber” 和 “robbers” ，
//// 尽管这对于区分 “robbers” 和 “robbery” 并无帮助。
//return score * 2 + (len(string) > 6)
//// <end id="string-to-score"/>
//
//func to_char_map(set):
//out = {}
//for pos, val in enumerate(sorted(set)):
//out[val] = pos-1
//return out
//
//LOWER = to_char_map(set([-1]) | set(xrange(ord("a"), ord("z")+1)))
//ALPHA = to_char_map(set(LOWER) | set(xrange(ord("A"), ord("Z")+1)))
//LOWER_NUMERIC = to_char_map(set(LOWER) | set(xrange(ord("0"), ord("9")+1)))
//ALPHA_NUMERIC = to_char_map(set(LOWER_NUMERIC) | set(ALPHA))
//
//func string_to_score_generic(string, mapping):
//length = int(52 / math.log(len(mapping), 2))    //A
//
//pieces = map(ord, string[:length])              //B
//while len(pieces) < length:                     //C
//pieces.append(-1)                           //C
//
//score = 0
//for piece in pieces:                            //D
//value = mapping[piece]                      //D
//score = score * len(mapping) + value + 1    //D
//
//return score * 2 + (len(string) > length)       //E
//
//
//
//// <start id="zadd-string"/>
//func zadd_string(conn, name, *args, **kwargs):
//pieces = list(args)                         // 为了进行之后的修改，
//for piece in kwargs.iteritems():            // 对传入的不同类型的参数进行合并（combine）
//pieces.extend(piece)                    //
//
//for i, v in enumerate(pieces):
//if i & 1:                               // 将字符串格式的分值转换为整数分值
//pieces[i] = string_to_score(v)      //
//
//return conn.zadd(name, *pieces)             // 调用已有的 ZADD 方法
//// <end id="zadd-string"/>
//
//
//// 代码清单 7-9
//// <start id="ecpm_helpers"/>
//func cpc_to_ecpm(views, clicks, cpc):
//return 1000. * cpc * clicks / views
//
//func cpa_to_ecpm(views, actions, cpa):
//// 因为点击通过率是由点击次数除以展示次数计算出的，
//// 而动作的执行概率则是由动作执行次数除以点击次数计算出的，
//// 所以这两个概率相乘的结果等于动作执行次数除以展示次数。
//return 1000. * cpa * actions / views
//// <end id="ecpm_helpers"/>
//
//
//// 代码清单 7-10
//// <start id="index_ad"/>
//TO_ECPM = {
//"cpc": cpc_to_ecpm,
//"cpa": cpa_to_ecpm,
//"cpm": lambda *args:args[-1],
//}
//
//func index_ad(conn, id, locations, content, type, value):
//// 设置流水线，使得程序可以在一次通信往返里面完成整个索引操作。
//pipeline = conn.pipeline(True)
//
//for location in locations:
//// 为了进行定向操作，把广告 ID 添加到所有相关的位置集合里面。
//pipeline.sadd("idx:req:"+location, id)
//
//words = tokenize(content)
//// 对广告包含的单词进行索引。
//for word in tokenize(content):
//pipeline.zadd("idx:" + word, id, 0)
//
//// 为了评估新广告的效果，
//// 程序会使用字典来储存广告每千次展示的平均点击次数或平均动作执行次数。
//rvalue = TO_ECPM[type](
//1000, AVERAGE_PER_1K.get(type, 1), value)
//// 记录这个广告的类型。
//pipeline.hset("type:", id, type)
//// 将广告的 eCPM 添加到一个记录了所有广告的 eCPM 的有序集合里面。
//pipeline.zadd("idx:ad:value:", id, rvalue)
//// 将广告的基本价格（base value）添加到一个记录了所有广告的基本价格的有序集合里面。
//pipeline.zadd("ad:base_value:", id, value)
//// 把能够对广告进行定向的单词全部记录起来。
//pipeline.sadd("terms:" + id, *list(words))
//pipeline.execute()
//// <end id="index_ad"/>
//
//
//// 代码清单 7-11
//// <start id="target_ad"/>
//func target_ads(conn, locations, content):
//pipeline = conn.pipeline(True)
//// 根据用户传入的位置定向参数，找到所有位于该位置的广告，以及这些广告的 eCPM 。
//matched_ads, base_ecpm = match_location(pipeline, locations)
//// 基于匹配的内容计算附加值。
//words, targeted_ads = finish_scoring(
//pipeline, matched_ads, base_ecpm, content)
//
//// 获取一个 ID ，它可以用于汇报并记录这个被定向的广告。
//pipeline.incr("ads:served:")
//// 找到 eCPM 最高的广告，并获取这个广告的 ID 。
//pipeline.zrevrange("idx:" + targeted_ads, 0, 0)
//target_id, targeted_ad = pipeline.execute()[-2:]
//
//// 如果没有任何广告与目标位置相匹配，那么返回空值。
//if not targeted_ad:
//return None, None
//
//ad_id = targeted_ad[0]
//// 记录一系列定向操作的执行结果，作为学习用户行为的其中一个步骤。
//record_targeting_result(conn, target_id, ad_id, words)
//
//// 向调用者返回记录本次定向操作相关信息的 ID ，以及被选中的广告的 ID 。
//return target_id, ad_id
//// <end id="target_ad"/>
//
//
//// 代码清单 7-12
//// <start id="location_target"/>
//func match_location(pipe, locations):
//// 根据给定的位置，找出所有需要执行并集操作的集合键。
//required = ["req:" + loc for loc in locations]
//// 找出与指定地区相匹配的广告，并将它们储存到集合里面。
//matched_ads = union(pipe, required, ttl=300, _execute=False)
//// 找到储存着所有被匹配广告的集合，
//// 以及储存着所有被匹配广告的基本 eCPM 的有序集合，
//// 然后返回它们的 ID 。
//return matched_ads, zintersect(pipe,
//{matched_ads: 0, "ad:value:": 1}, _execute=False)
//// <end id="location_target"/>
//
//
//// 代码清单 7-13
//// <start id="finish_scoring"/>
//func finish_scoring(pipe, matched, base, content):
//bonus_ecpm = {}
//// 对内容进行标记化处理，以便与广告进行匹配。
//words = tokenize(content)
//for word in words:
//// 找出那些既位于定向位置之内，又拥有页面内容其中一个单词的广告。
//word_bonus = zintersect(
//pipe, {matched: 0, word: 1}, _execute=False)
//bonus_ecpm[word_bonus] = 1
//
//if bonus_ecpm:
//// 计算每个广告的最小 eCPM 附加值和最大 eCPM 附加值。
//minimum = zunion(
//pipe, bonus_ecpm, aggregate="MIN", _execute=False)
//maximum = zunion(
//pipe, bonus_ecpm, aggregate="MAX", _execute=False)
//
//// 将广告的基本价格、最小 eCPM 附加值的一半以及最大 eCPM 附加值的一半这三者相加起来。
//return words, zunion(
//pipe, {base:1, minimum:.5, maximum:.5}, _execute=False)
//// 如果页面内容中没有出现任何可匹配的单词，那么返回广告的基本 eCPM 。
//return words, base
//// <end id="finish_scoring"/>
//
//
//// 代码清单 7-14
//// <start id="record_targeting"/>
//func record_targeting_result(conn, target_id, ad_id, words):
//pipeline = conn.pipeline(True)
//
//// 找出内容与广告之间相匹配的那些单词。
//terms = conn.smembers("terms:" + ad_id)
//matched = list(words & terms)
//if matched:
//matched_key = "terms:matched:%s" % target_id
//// 如果有相匹配的单词出现，那么把它们记录起来，并设置 15 分钟的生存时间。
//pipeline.sadd(matched_key, *matched)
//pipeline.expire(matched_key, 900)
//
//// 为每种类型的广告分别记录它们的展示次数。
//type = conn.hget("type:", ad_id)
//pipeline.incr("type:%s:views:" % type)
//// 对广告以及广告包含的单词的展示信息进行记录。
//for word in matched:
//pipeline.zincrby("views:%s" % ad_id, word)
//pipeline.zincrby("views:%s" % ad_id, '')
//
//// 广告每展示 100 次，就更新一次它的 eCPM 。
//if not pipeline.execute()[-1] % 100:
//update_cpms(conn, ad_id)
//
//// <end id="record_targeting"/>
//
//
//// 代码清单 7-15
//// <start id="record_click"/>
//func record_click(conn, target_id, ad_id, action=False):
//pipeline = conn.pipeline(True)
//click_key = "clicks:%s"%ad_id
//
//match_key = "terms:matched:%s"%target_id
//
//type = conn.hget("type:", ad_id)
//// 如果这是一个按动作计费的广告，
//// 并且被匹配的单词仍然存在，
//// 那么刷新这些单词的过期时间。
//if type == "cpa":
//pipeline.expire(match_key, 900)
//if action:
//// 记录动作信息，而不是点击信息。
//click_key = "actions:%s" % ad_id
//
//if action and type == "cpa":
//// 根据广告的类型，维持一个全局的点击/动作计数器。
//pipeline.incr("type:%s:actions:" % type)
//else:
//pipeline.incr("type:%s:clicks:" % type)
//
//// 为广告以及所有被定向至该广告的单词记录下本次点击（或动作）。
//matched = list(conn.smembers(match_key))
//matched.append('')
//for word in matched:
//pipeline.zincrby(click_key, word)
//pipeline.execute()
//
//// 对广告中出现的所有单词的 eCPM 进行更新。
//update_cpms(conn, ad_id)
//// <end id="record_click"/>
//
//
//// 代码清单 7-16
//// <start id="update_cpms"/>
//func update_cpms(conn, ad_id):
//pipeline = conn.pipeline(True)
//// 获取广告的类型和价格，以及广告包含的所有单词。
//pipeline.hget("type:", ad_id)
//pipeline.zscore("ad:base_value:", ad_id)
//pipeline.smembers("terms:" + ad_id)
//type, base_value, words = pipeline.execute()
//
//// 判断广告的 eCPM 应该基于点击次数进行计算还是基于动作执行次数进行计算。
//which = "clicks"
//if type == "cpa":
//which = "actions"
//
//// 根据广告的类型，
//// 获取这类广告的展示次数和点击次数（或者动作执行次数）。
//pipeline.get("type:%s:views:" % type)
//pipeline.get("type:%s:%s" % (type, which))
//type_views, type_clicks = pipeline.execute()
//// 将广告的点击率或动作执行率重新写入到全局字典里面。
//AVERAGE_PER_1K[type] = (
//1000. * int(type_clicks or "1") / int(type_views or "1"))
//
//// 如果正在处理的是一个 CPM 广告，
//// 那么它的 eCPM 已经更新完毕，
//// 无需再做其他处理。
//if type == "cpm":
//return
//
//view_key = "views:%s" % ad_id
//click_key = "%s:%s" % (which, ad_id)
//
//to_ecpm = TO_ECPM[type]
//
//// 获取广告的展示次数，以及广告的点击次数（或者动作执行次数）。
//pipeline.zscore(view_key, '')
//pipeline.zscore(click_key, '')
//ad_views, ad_clicks = pipeline.execute()
//// 如果广告还没有被点击过，那么使用已有的 eCPM 。
//if (ad_clicks or 0) < 1:
//ad_ecpm = conn.zscore("idx:ad:value:", ad_id)
//else:
//// 计算广告的 eCPM 并更新它的价格。
//ad_ecpm = to_ecpm(ad_views or 1, ad_clicks or 0, base_value)
//pipeline.zadd("idx:ad:value:", ad_id, ad_ecpm)
//
//for word in words:
//// 获取单词的展示次数和点击次数（或者动作执行次数）。
//pipeline.zscore(view_key, word)
//pipeline.zscore(click_key, word)
//views, clicks = pipeline.execute()[-2:]
//
//// 如果广告还未被点击过，那么不对 eCPM 进行更新。
//if (clicks or 0) < 1:
//continue
//
//// 计算单词的 eCPM 。
//word_ecpm = to_ecpm(views or 1, clicks or 0, base_value)
//// 计算单词的附加值。
//bonus = word_ecpm - ad_ecpm
//// 将单词的附加值重新写入到为广告包含的每个单词分别记录附加值的有序集合里面。
//pipeline.zadd("idx:" + word, ad_id, bonus)
//pipeline.execute()
//// <end id="update_cpms"/>
//
//
//// 代码清单 7-17
//// <start id="slow_job_search"/>
//func add_job(conn, job_id, required_skills):
//// 把职位所需的技能全部添加到职位对应的集合里面。
//conn.sadd("job:" + job_id, *required_skills)
//
//func is_qualified(conn, job_id, candidate_skills):
//temp = str(uuid.uuid4())
//pipeline = conn.pipeline(True)
//// 把求职者拥有的技能全部添加到一个临时集合里面，并设置过期时间。
//pipeline.sadd(temp, *candidate_skills)
//pipeline.expire(temp, 5)
//// 找出职位所需技能当中，求职者不具备的那些技能，并将它们记录到结果集合里面。
//pipeline.sdiff("job:" + job_id, temp)
//// 如果求职者具备职位所需的全部技能，那么返回 True 。
//return not pipeline.execute()[-1]
//// <end id="slow_job_search"/>
//
//
//// 代码清单 7-18
//// <start id="job_search_index"/>
//func index_job(conn, job_id, skills):
//pipeline = conn.pipeline(True)
//for skill in skills:
//// 将职位 ID 添加到相应的技能集合里面。
//pipeline.sadd("idx:skill:" + skill, job_id)
//// 将职位所需技能的数量添加到记录了所有职位所需技能数量的有序集合里面。
//pipeline.zadd("idx:jobs:req", job_id, len(set(skills)))
//pipeline.execute()
//// <end id="job_search_index"/>
//
//
//// 代码清单 7-19
//// <start id="job_search_results"/>
//func find_jobs(conn, candidate_skills):
//// 设置好用于计算职位得分的字典。
//skills = {}
//for skill in set(candidate_skills):
//skills["skill:" + skill] = 1
//
//// 计算求职者对于每个职位的得分。
//job_scores = zunion(conn, skills)
//// 计算出求职者能够胜任以及不能够胜任的职位。
//final_result = zintersect(
//conn, {job_scores:-1, "jobs:req":1})
//
//// 返回求职者能够胜任的那些职位。
//return conn.zrangebyscore("idx:" + final_result, 0, 0)
//// <end id="job_search_results"/>
//
//// 0 is beginner, 1 is intermediate, 2 is expert
//SKILL_LEVEL_LIMIT = 2
//
//func index_job_levels(conn, job_id, skill_levels):
//total_skills = len(set(skill for skill, level in skill_levels))
//pipeline = conn.pipeline(True)
//for skill, level in skill_levels:
//level = min(level, SKILL_LEVEL_LIMIT)
//for wlevel in xrange(level, SKILL_LEVEL_LIMIT+1):
//pipeline.sadd("idx:skill:%s:%s"%(skill,wlevel), job_id)
//pipeline.zadd("idx:jobs:req", job_id, total_skills)
//pipeline.execute()
//
//func search_job_levels(conn, skill_levels):
//skills = {}
//for skill, level in skill_levels:
//level = min(level, SKILL_LEVEL_LIMIT)
//for wlevel in xrange(level, SKILL_LEVEL_LIMIT+1):
//skills["skill:%s:%s"%(skill,wlevel)] = 1
//
//job_scores = zunion(conn, skills)
//final_result = zintersect(conn, {job_scores:-1, "jobs:req":1})
//
//return conn.zrangebyscore("idx:" + final_result, 0, 0)
//
//
//func index_job_years(conn, job_id, skill_years):
//total_skills = len(set(skill for skill, level in skill_years))
//pipeline = conn.pipeline(True)
//for skill, years in skill_years:
//pipeline.zadd(
//"idx:skill:%s:years"%skill, job_id, max(years, 0))
//pipeline.sadd("idx:jobs:all", job_id)
//pipeline.zadd("idx:jobs:req", job_id, total_skills)
//
//
//func search_job_years(conn, skill_years):
//skill_years = dict(skill_years)
//pipeline = conn.pipeline(True)
//
//union = []
//for skill, years in skill_years.iteritems():
//sub_result = zintersect(pipeline,
//{"jobs:all":-years, "skill:%s:years"%skill:1}, _execute=False)
//pipeline.zremrangebyscore("idx:" + sub_result, "(0", "inf")
//union.append(
//zintersect(pipeline, {"jobs:all":1, sub_result:0}), _execute=False)
//
//job_scores = zunion(pipeline, dict((key, 1) for key in union), _execute=False)
//final_result = zintersect(pipeline, {job_scores:-1, "jobs:req":1}, _execute=False)
//
//pipeline.zrange("idx:" + final_result, 0, 0)
//return pipeline.execute()[-1]
