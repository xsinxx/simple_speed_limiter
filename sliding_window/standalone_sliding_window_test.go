package sliding_window

import (
	"context"
	c "github.com/smartystreets/goconvey/convey" // 别名导入
	"testing"
	"time"
)

func TestStandAloneAllow(t *testing.T) {
	c.Convey("standalone_sliding_window", t, func() {
		var (
			limiter = NewStandAloneSlidingWindowLimiter(100, 1000, 10)
		)
		actual := limiter.Allow(context.Background(), time.Now())
		c.So(actual, c.ShouldBeTrue)
	})
}
