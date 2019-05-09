package org.gy.framework.redis.lock;

import org.gy.framework.redis.JedisClient;
import org.gy.framework.redis.ShardedJedisClient;
import org.gy.framework.redis.lock.support.DistributedLock;
import org.gy.framework.redis.lock.support.DistributedLockFactory;
import org.gy.framework.redis.response.ResponseCode;
import org.gy.framework.redis.response.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLockAction {

    private final Logger                  logger = LoggerFactory.getLogger(DistributedLockAction.class);

    /**
     * 锁实例
     */
    private final DistributedLock         lock;
    /**
     * 业务操作接口
     */
    private final DistributedLockRunnable runnable;

    public DistributedLockAction(ShardedJedisClient shardedJedisClient, String lockKey, DistributedLockRunnable runnable) {
        this.lock = DistributedLockFactory.getShardedJedisLock(shardedJedisClient, lockKey);
        this.runnable = runnable;
    }

    public DistributedLockAction(JedisClient jedisClient, String lockKey, DistributedLockRunnable runnable) {
        this.lock = DistributedLockFactory.getJedisLock(jedisClient, lockKey);
        this.runnable = runnable;
    }

    /**
     * 功能描述: 业务执行，包含加锁、释放锁(非阻塞锁，仅尝试一次获取锁)
     * 
     * @param expireTimeMillis expireTime 锁失效时间，单位：毫秒
     * @return
     */
    public Result execute(long expireTimeMillis) {
        return execute(0, 0, expireTimeMillis);
    }

    /**
     * 功能描述:业务执行，包含加锁、释放锁(阻塞锁，一直阻塞)
     * 
     * @param sleepTimeMillis 睡眠重试时间，单位：毫秒，防止一直消耗CPU
     * @param expireTimeMillis 锁失效时间，单位：毫秒
     * @return
     */
    public Result execute(long sleepTimeMillis,
                          long expireTimeMillis) {
        return execute(-1, sleepTimeMillis, expireTimeMillis);
    }

    /**
     * 功能描述:业务执行，包含加锁、释放锁(阻塞锁，自定义阻塞时间)
     * 
     * @param waitTimeMillis 等待超时时间，单位：毫秒
     * @param sleepTimeMillis 睡眠重试时间，单位：毫秒，防止一直消耗CPU
     * @param expireTimeMillis 锁失效时间，单位：毫秒
     * @return
     */
    public Result execute(long waitTimeMillis,
                          long sleepTimeMillis,
                          long expireTimeMillis) {
        Result result = new Result();
        boolean lockFlag = false;
        try {
            lockFlag = lock.tryLock(waitTimeMillis, sleepTimeMillis, expireTimeMillis);
            if (!lockFlag) {
                result.wrapResult(ResponseCode.E9901);
                return result;
            }
            result = runnable.run();
        } catch (Exception e) {
            logger.error("execute exception:" + e.getMessage(), e);
            result.wrapResult(ResponseCode.E9902);
        } finally {
            if (lockFlag) {
                try {
                    lock.unlock();
                } catch (Exception e) {
                    logger.error("release exception:" + e.getMessage(), e);
                }
            }
        }
        return result;
    }

}
