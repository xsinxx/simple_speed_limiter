package sliding_window

import (
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type StandAloneSlidingWindowLimiter struct {
	limit          int64           // 窗口请求上限
	window         int64           // 总窗口时间大小
	unitWindow     int64           // 单位窗口时间大小
	unitWindowsNum int64           // 单位窗口数量
	mutex          sync.Mutex      // 单机限流的锁
	buckets        map[int64]int64 // timestamp:count
}

func NewStandAloneSlidingWindowLimiter(limit int64, window, unitWindow int64) *StandAloneSlidingWindowLimiter {
	return &StandAloneSlidingWindowLimiter{
		limit:          limit,
		window:         window,
		unitWindow:     unitWindow,
		unitWindowsNum: window / unitWindow,
		buckets:        map[int64]int64{},
	}
}

func (s *StandAloneSlidingWindowLimiter) Allow(ctx context.Context, now time.Time) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// 获取当前单位窗口的值
	currentUnitWindow := now.UnixMilli() / s.unitWindow * s.unitWindow
	// 获取起始单位窗口的值
	startUnitWindow := currentUnitWindow - s.unitWindow*(s.unitWindowsNum-1)
	log.WithFields(log.Fields{"currentUnitWindow": currentUnitWindow, "startUnitWindow": startUnitWindow}).Info("[StandAloneSlidingWindowLimiter] Allow")
	// 计算时间窗口内的数量
	var totalNum int64
	for timestamp, value := range s.buckets {
		if timestamp < startUnitWindow {
			delete(s.buckets, timestamp)
			continue
		}
		totalNum += value
	}
	// 超出阈值
	log.WithFields(log.Fields{"totalNum": totalNum, "limit": s.limit}).Info("[StandAloneSlidingWindowLimiter] Allow")
	if totalNum > s.limit {
		return false
	}
	// 更新桶计数
	s.buckets[currentUnitWindow]++
	return true
}
