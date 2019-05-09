package org.gy.framework.redis.lock.support;

/**
 * 功能描述：
 * 
 */
public abstract class AbstractJedisDistributedLock implements DistributedLock {

    protected static final String LOCK_SUCCESS         = "OK";
    protected static final String SET_IF_NOT_EXIST     = "NX";
    protected static final String SET_WITH_EXPIRE_TIME = "PX";
    protected static final Long   RELEASE_SUCCESS      = 1L;
    protected static final String SCRIPT               = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
    protected static final String DEFAULT_LOCK_PREFIX  = "_LOCK_:";

    public abstract boolean innerLockHandler(long expireTimeMillis);

    public abstract boolean innerReleaseHandler();

    @Override
    public boolean tryLock(long expireTimeMillis) throws InterruptedException {
        return tryLock(0, 0, expireTimeMillis);
    }

    @Override
    public boolean tryLock(long waitTimeMillis,
                           long sleepTimeMillis,
                           long expireTimeMillis) throws InterruptedException {
        long current = System.currentTimeMillis();
        if (waitTimeMillis == 0) {
            return innerLockHandler(expireTimeMillis);
        }
        while (checkCondition(current, waitTimeMillis)) {
            boolean doLock = innerLockHandler(expireTimeMillis);
            if (doLock) {
                return true;
            }
            // 针对有超时的场景，如果此时耗费时间加上sleepTime已经超时，直接返回失败即可，不需要sleep，可提升效率
            if (waitTimeMillis > 0) {
                long time = (System.currentTimeMillis() - current) + sleepTimeMillis - waitTimeMillis;
                if (time > 0) {
                    return false;
                }
            }
            wrapBlockTime(sleepTimeMillis);
        }
        return false;
    }

    @Override
    public void lock(long sleepTimeMillis,
                     long expireTimeMillis) throws InterruptedException {
        tryLock(-1, sleepTimeMillis, expireTimeMillis);
    }

    @Override
    public void unlock() {
        innerReleaseHandler();
    }

    private boolean checkCondition(long startTime,
                                   long waitTimeMillis) {
        if (waitTimeMillis < 0) {
            return true;
        }
        return (System.currentTimeMillis() - startTime) <= waitTimeMillis;
    }

    private void wrapBlockTime(long sleepTimeMillis) throws InterruptedException {
        // 防止一直消耗 CPU
        Thread.sleep(sleepTimeMillis);
    }
}
