package cronx

import (
	"errors"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/robfig/cron/v3"
)

// locationCron 为每个任务保存独立解析结果，直接使用时区对象，避免按名称重新加载丢失规则。
type locationCron struct {
	schedule *cron.SpecSchedule
}

func (c *locationCron) IsValid(expression string, location *time.Location, now time.Time) error {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(expression)
	if err != nil {
		return errors.Join(gocron.ErrCronJobParse, err)
	}
	// 严格字段解析未启用描述符，成功结果必为 SpecSchedule。
	c.schedule = schedule.(*cron.SpecSchedule)
	c.schedule.Location = location
	if c.Next(now).IsZero() {
		return gocron.ErrCronJobInvalid
	}
	return nil
}

func (c *locationCron) Next(lastRun time.Time) time.Time {
	// robfig 将 time.Local 视为沿用输入时间的时区，显式转换保证这一路径也使用配置时区。
	return c.schedule.Next(lastRun.In(c.schedule.Location))
}
