package redislock

import (
	"context"

	"github.com/bpcoder16/pixiu/biz/lockx"
)

// 直接逐个 SET NX PX，减少低竞争成功路径上的检查。
// 竞争或命令错误时按身份清理此前写入；清理失败返回错误，且继续清理其余 key。
const acquireManyScript = `
for i, key in ipairs(KEYS) do
    local result = redis.pcall('set', key, ARGV[1], 'NX', 'PX', ARGV[2])
    if not result or (type(result) == 'table' and result.err) then
        local cleanupError = nil
        for j = 1, i - 1 do
            local value = redis.pcall('get', KEYS[j])
            if type(value) == 'table' and value.err then
                cleanupError = cleanupError or value
            elseif value == ARGV[1] then
                local removed = redis.pcall('del', KEYS[j])
                if type(removed) == 'table' and removed.err then
                    cleanupError = cleanupError or removed
                end
            end
        end
        if cleanupError then
            local message = 'ERR redislock: cleanup failed: ' .. cleanupError.err
            if type(result) == 'table' and result.err then
                message = message .. '; acquire failed: ' .. result.err
            end
            return redis.error_reply(message)
        end
        if not result then
            return 0
        end
        return result
    end
end
return 1
`

// 单个 key 的错误不能阻止其余 key 的清理；只删除令牌匹配的占用并保留错误。
const unlockManyScript = `
local removed = 0
local firstError = nil
for _, key in ipairs(KEYS) do
    local value = redis.pcall('get', key)
    if type(value) == 'table' and value.err then
        firstError = firstError or value
    elseif value == ARGV[1] then
        local result = redis.pcall('del', key)
        if type(result) == 'table' and result.err then
            firstError = firstError or result
        else
            removed = removed + result
        end
    end
end
if firstError then
    return firstError
end
return removed
`

// TryLockMany 在单机 Redis 上原子获取全部 key，不等待、不重试、不续租。
// keys 按原值复制去重；只有一个唯一 key 时使用 SET NX PX，多个时使用一次 Lua。
// 任一被占用则整批失败，不交付部分句柄；各 key 共用本次令牌和从尝试开始计算的租期。
// 竞争失败且已获取的 key 清理成功时返回 nil、false、nil；写入或清理失败返回错误。
// 已确认成功但取消或超时时独立限时清理；不确定结果返回错误并依靠 TTL 回收。
// 不支持 Redis Cluster 跨槽获取。
func (l *Locker) TryLockMany(ctx context.Context, keys []string) (lockx.Lock, bool, error) {
	return l.tryLock(ctx, keys)
}
