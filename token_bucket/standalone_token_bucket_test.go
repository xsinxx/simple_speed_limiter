package token_bucket

import (
	"context"
	c "github.com/smartystreets/goconvey/convey" // 别名导入
	"testing"
	"time"
)

func TestStandAloneAllow(t *testing.T) {
	c.Convey("standalone_token_bucket", t, func() {
		var (
			limiter = NewStandAloneTokenBucket(1, 10)
		)
		actual := limiter.AllowN(context.Background(), time.Now(), 19)
		c.So(!actual, c.ShouldBeTrue)
	})
	// time pass 2s
	c.Convey("standalone_token_bucket", t, func() {
		var (
			limiter = NewStandAloneTokenBucket(10, 50)
		)
		time.Sleep(2 * time.Second)
		actual := limiter.AllowN(context.Background(), time.Now(), 10)
		c.So(actual, c.ShouldBeTrue)
	})
}
