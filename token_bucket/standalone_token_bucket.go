package token_bucket

import (
	"context"
	"sync"
	"time"
)

type StandAloneRateLimiter struct {
	rate       int64      // 每秒生产速率
	burst      int64      // 桶容量
	last       int64      // 上一次请求发生时间
	amount     int64      // 上个桶中令牌数量
	rescueLock sync.Mutex // 由于读写冲突，需要加锁
}

func NewStandAloneTokenBucket(rate int64, burst int64) *StandAloneRateLimiter {
	return &StandAloneRateLimiter{
		rate:   rate,
		burst:  burst,
		last:   time.Now().Unix(),
		amount: 0,
	}
}

func (s *StandAloneRateLimiter) AllowN(ctx context.Context, now time.Time, requestedTokenNum int64) bool {
	s.rescueLock.Lock()
	defer s.rescueLock.Unlock()

	// 距离上一次请求过去的时间, 以秒为单位
	passed := now.Unix() - s.last

	// 计算在这段时间里 令牌数量可以增加多少
	amount := s.amount + passed*s.rate

	// 如果令牌数量超过上限；我们就不继续放入那么多令牌了
	if amount > s.burst {
		amount = s.burst
	}

	// 如果令牌数量仍然小于请求数量，则说明请求应该拒绝
	if amount < requestedTokenNum {
		return false
	}

	amount -= requestedTokenNum
	s.amount = amount
	// 更新上次请求时间
	s.last = now.Unix()

	return true
}
