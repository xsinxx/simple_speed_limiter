package token_bucket

import (
	"context"
	"github.com/go-redis/redis/v8"
	c "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestDistributedAllow(t *testing.T) {
	c.Convey("distributed_token_bucket", t, func() {
		var (
			store = redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
			})
			limiter = NewDistributedTokenLimiter(10, 100, store, "distributed_token_bucket")
		)
		actual := limiter.AllowN(context.Background(), time.Now(), 5)
		c.So(actual, c.ShouldBeTrue)
	})
}
