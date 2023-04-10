package sliding_window

import (
	"context"
	"github.com/go-redis/redis/v8"
	c "github.com/smartystreets/goconvey/convey" // 别名导入
	"testing"
	"time"
)

func TestDistributedAllow(t *testing.T) {
	c.Convey("distributed_sliding_window", t, func() {
		var (
			store = redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
			})
			limiter = NewDistributedSlidingWindowLimiter(store, 100, 1000, 10, "distributed_sliding_window")
		)
		if limiter != nil {
			actual := limiter.Allow(context.Background(), time.Now(), 5)
			c.So(actual, c.ShouldBeTrue)
		}
	})
}
