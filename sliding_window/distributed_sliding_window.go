package sliding_window

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type DistributedSlidingWindowLimiter struct {
	limit                          int64                           // 窗口请求上限
	window                         int64                           // 总窗口时间大小
	unitWindow                     int64                           // 单位窗口时间大小
	unitWindowsNum                 int64                           // 单位窗口数量
	token                          string                          // redis对应的key
	store                          *redis.Client                   // Redis客户端
	rescueLock                     sync.Mutex                      // 单机限流的锁
	monitorStarted                 bool                            // redis监控探测任务标识
	redisAlive                     uint32                          // redis健康标识
	standAloneSlidingWindowLimiter *StandAloneSlidingWindowLimiter // 单机滑动窗口限流器
}

func NewDistributedSlidingWindowLimiter(store *redis.Client, limit int64, window, unitWindow int64, token string) *DistributedSlidingWindowLimiter {
	// 窗口时间必须能够被小窗口时间整除
	if window%unitWindow != 0 {
		log.WithFields(log.Fields{"err": errors.New("window cannot be split by integers")}).Error("[NewDistributedSlidingWindowLimiter]")
		return nil
	}

	return &DistributedSlidingWindowLimiter{
		limit:                          limit,
		window:                         window,
		unitWindow:                     unitWindow,
		unitWindowsNum:                 window / unitWindow,
		token:                          token,
		store:                          store,
		redisAlive:                     1,
		standAloneSlidingWindowLimiter: NewStandAloneSlidingWindowLimiter(limit, window, unitWindow),
	}
}

func (d *DistributedSlidingWindowLimiter) Allow(ctx context.Context, now time.Time, requestedNum int64) bool {
	// 判断redis是否健康, redis故障时采用进程内限流器, 兜底保障
	if atomic.LoadUint32(&d.redisAlive) == 0 {
		log.WithFields(log.Fields{}).Warn("start standalone rate limit")
		return d.standAloneSlidingWindowLimiter.Allow(ctx, now)
	}
	// 获取当前单位窗口的值
	currentUnitWindow := now.UnixMilli() / d.unitWindow * d.unitWindow
	// 获取起始单位窗口的值
	startUnitWindow := currentUnitWindow - d.unitWindow*(d.unitWindowsNum-1)
	// 获取hash中全部数据
	m, err := d.store.HGetAll(ctx, d.token).Result()
	if err != nil && err != redis.Nil {
		log.WithFields(log.Fields{"err": err}).Error("HGetAll fail")
		d.startMonitor(ctx)
		return false
	}
	log.WithFields(log.Fields{"hashMap": m}).Info("Allow")
	// 计算[startUnitWindow, currentUnitWindow]总共有多少数
	var totalNum int64
	for timestamp, count := range m {
		ts, _ := strconv.Atoi(timestamp)
		if int64(ts) < startUnitWindow {
			d.store.HDel(ctx, d.token, timestamp)
		} else {
			c, _ := strconv.Atoi(count)
			totalNum += int64(c)
		}
	}
	// 滑动窗口中数量超过上限
	if totalNum > d.limit {
		log.WithFields(log.Fields{"totalNum": totalNum, "limit": d.limit, "startUnitWindow": startUnitWindow, "currentUnitWindow": currentUnitWindow}).Info("over limit")
		return false
	}
	// pipeline 执行
	pipeline := d.store.Pipeline()
	pipeline.HIncrBy(ctx, d.token, strconv.Itoa(int(currentUnitWindow)), 1)
	pipeline.PExpire(ctx, d.token, time.Hour)
	_, err = pipeline.Exec(ctx)
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Error("pipeline exec fail")
		d.startMonitor(ctx)
		return false
	}
	return true
}

// 开启redis健康探测
func (d *DistributedSlidingWindowLimiter) startMonitor(ctx context.Context) {
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
func (d *DistributedSlidingWindowLimiter) waitForRedis(ctx context.Context) {
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
