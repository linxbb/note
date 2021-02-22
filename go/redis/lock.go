package redis

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"reflect"
	"runtime"
	"time"
)

func redisLock(task func()) {
	//key的过期时间 单位秒
	expiration := int64(10 * 60)
	//任务未执行完 每隔keepExpiration 再给key续上过期时间expiration
	keepExpiration := int64(9 * 60)

	key := runtime.FuncForPC(reflect.ValueOf(task).Pointer()).Name()
	log.Println("runtime.FuncForPC key:", key)
	for {
		//获取任务锁
		isRedisScheduleLock := RedisScheduleLock(key, expiration)
		log.Printf("isRedisScheduleLock:%v", isRedisScheduleLock)
		if isRedisScheduleLock {
			endChan := make(chan int)
			//维持任务锁
			log.Printf("开始启动任务 key:%v,", key)
			go keepTaskExpirationTime(endChan, key, expiration, keepExpiration)

			//执行任务
			task()

			//通知维持任务锁取消
			endChan <- 1

			//删除key
			if !DelRedisScheduleLock(key) {
				panic(fmt.Sprintf("DELREDISSCHEDULELOCK ERROR %s", key))
			}
		}
		log.Printf("after %v continue check redis lock", time.Duration(expiration)*time.Second)
		time.Sleep(time.Duration(expiration) * time.Second)
	}

}

func keepTaskExpirationTime(endChan chan int, key string, expiration, keepExpiration int64) {
	for {
		select {
		case <-endChan:
			return
		case <-time.After(time.Duration(keepExpiration) * time.Second):
			if !KeepRedisScheduleLock(key, expiration) {
				//开发服会改系统时间导致redis key过期
				env := os.Getenv("NACOS_CONFIG_TAG")
				if env == "dev" {
					if !RedisScheduleLock(key, expiration) {
						panic(fmt.Sprintf("Relocking failed ERROR %s", key))
					}
				} else {
					panic(fmt.Sprintf("KEEPREDISSCHEDULELOCK ERROR %s", key))
				}
			}
		}
	}
}

// RedisScheduleLock 设置计划任务锁
func RedisScheduleLock(key string, expiration int64) bool {
	return redisScheduleLock(key, expiration)
}

// DelRedisScheduleLock 删除计划任务锁
func DelRedisScheduleLock(key string) bool {
	return delRedisScheduleLock(key)
}

// KeepRedisScheduleLock 维持计划任务锁不过期
func KeepRedisScheduleLock(key string, expiration int64) bool {
	return keepRedisScheduleLock(key, expiration)
}

var (
	KeyScheduleLock = "ag:schedule:lock:%s"
	pool            = newPool("127.0.0.1:6379")
)

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

func redisScheduleLock(key string, expiration int64) bool {
	conn := pool.Get()
	defer conn.Close()

	res, err := conn.Do("set", fmt.Sprintf(KeyScheduleLock, key), time.Now().String(), "NX", "EX", expiration)

	if err != nil && err != redis.ErrNil {
		log.Printf("schedule task %s err:%+v", key, err)
	}

	return res != nil
}

func delRedisScheduleLock(key string) bool {
	conn := pool.Get()
	defer conn.Close()

	ok, err := redis.Bool(conn.Do("del", fmt.Sprintf(KeyScheduleLock, key)))
	if err != nil {
		log.Printf("delete schedule task %s err:%+v", key, err)
	}

	return ok
}

func keepRedisScheduleLock(key string, expiration int64) bool {
	conn := pool.Get()
	defer conn.Close()

	ok, err := redis.Bool(conn.Do("expire", fmt.Sprintf(KeyScheduleLock, key), expiration))
	if err != nil {
		log.Printf("keep schedule task %s err:%+v", key, err)
	}

	return ok
}
