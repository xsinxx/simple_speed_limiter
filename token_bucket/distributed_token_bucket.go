package token_bucket

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type DistributedTokenLimiter struct {
	rate                  int64                  // 每秒生产速率
	burst                 int64                  // 桶容量
	store                 *redis.Client          // 存储容器
	tokenKey              string                 // redis key
	timestampKey          string                 // 桶刷新时间key
	rescueLock            sync.Mutex             // rescueLock
	redisAlive            uint32                 // redis健康标识
	standAloneRateLimiter *StandAloneRateLimiter // redis故障时采用进程内 令牌桶限流器
	monitorStarted        bool                   // redis监控探测任务标识
}

func NewDistributedTokenLimiter(rate, burst int64, store *redis.Client, key string) *DistributedTokenLimiter {
	tokenKey := fmt.Sprintf("simple_speed_limiter_%s", key)
	timestampKey := fmt.Sprintf("simple_speed_limiter_timestamp_%d_%s", time.Now().Unix(), key)

	return &DistributedTokenLimiter{
		rate:                  rate,
		burst:                 burst,
		store:                 store,
		tokenKey:              tokenKey,
		timestampKey:          timestampKey,
		redisAlive:            1,
		standAloneRateLimiter: NewStandAloneTokenBucket(rate, burst),
	}
}

func (d *DistributedTokenLimiter) AllowN(ctx context.Context, now time.Time, requestedNum int64) bool {
	// 判断redis是否健康, redis故障时采用进程内限流器, 兜底保障
	if atomic.LoadUint32(&d.redisAlive) == 0 {
		return d.standAloneRateLimiter.AllowN(ctx, now, requestedNum)
	}
	// 获取桶中剩余token
	remindTokenStr, err := d.store.Get(ctx, d.tokenKey).Result()
	if err != nil && err != redis.Nil {
		log.WithFields(log.Fields{"err": err}).Error("[distributedRateLimit] get remind tokens fail")
		d.startMonitor(ctx)
		return false
	}
	// 上次时间戳
	lastTimeStampStr, err := d.store.Get(ctx, d.timestampKey).Result()
	if err != nil && err != redis.Nil {
		log.WithFields(log.Fields{"err": err}).Error("[distributedRateLimit] get last timestamp fail")
		d.startMonitor(ctx)
		return false
	}
	lastTimeStamp, _ := strconv.Atoi(lastTimeStampStr)
	remindToken, _ := strconv.Atoi(remindTokenStr)
	// 本次可获取总token数量
	totalToken := (time.Now().Unix()-int64(lastTimeStamp))*d.rate + int64(remindToken)
	// 不可超过桶的容量
	if totalToken > d.burst {
		totalToken = d.burst
	}
	allowed := totalToken >= requestedNum
	var newToken int64
	if allowed {
		newToken = totalToken - requestedNum
	} else {
		newToken = totalToken
	}
	// pipeline 更新token & timestamp
	pipeLine := d.store.Pipeline()
	pipeLine.Set(ctx, d.tokenKey, newToken, 7*24*time.Hour)
	pipeLine.Set(ctx, d.timestampKey, time.Now().Unix(), 7*24*time.Hour)
	_, err = pipeLine.Exec(ctx)
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Error("[distributedRateLimit] pipeLine exec fail")
		d.startMonitor(ctx)
		return false
	}
	return allowed
}

// 开启redis健康探测
func (d *DistributedTokenLimiter) startMonitor(ctx context.Context) {
	d.rescueLock.Lock()
	defer d.rescueLock.Unlock()
	// 防止重复开启
	if d.monitorStarted {
		return
	}

	// 设置任务和健康标识
	d.monitorStarted = true
	atomic.StoreUint32(&d.redisAlive, 0)
	// 健康探测
	go d.waitForRedis(ctx)
}

// redis健康探测定时任务
func (d *DistributedTokenLimiter) waitForRedis(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	// 健康探测成功时回调此函数
	defer func() {
		ticker.Stop()
		d.rescueLock.Lock()
		d.monitorStarted = false
		d.rescueLock.Unlock()
	}()

	for range ticker.C {
		// ping属于redis内置健康探测命令
		if d.store.Ping(ctx).Val() == "pong" {
			// 健康探测成功，设置健康标识
			atomic.StoreUint32(&d.redisAlive, 1)
			return
		}
	}
}
