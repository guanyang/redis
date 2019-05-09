package org.gy.framework.redis.lock.support;

/**
 * 功能描述：
 * 
 */
public interface DistributedLock {

    /**
     * 功能描述: 非阻塞锁，仅尝试一次获取锁
     * 
     * @param expireTime 锁失效时间，单位：毫秒
     * @return 加锁是否成功，true成功，false不成功
     */
    boolean tryLock(long expireTimeMillis) throws InterruptedException;

    /**
     * 功能描述: 阻塞锁，自定义阻塞时间
     * 
     * @param waitTimeMillis 等待超时时间，单位：毫秒
     * @param sleepTimeMillis 睡眠重试时间，单位：毫秒，防止一直消耗CPU
     * @param expireTimeMillis 锁失效时间，单位：毫秒
     * @return 加锁是否成功，true成功，false不成功
     */
    boolean tryLock(long waitTimeMillis,
                    long sleepTimeMillis,
                    long expireTimeMillis) throws InterruptedException;

    /**
     * 功能描述: 阻塞锁，一直阻塞
     * 
     * @param sleepTimeMillis 睡眠重试时间，单位：毫秒，防止一直消耗CPU
     * @param expireTimeMillis 锁失效时间，单位：毫秒
     */
    void lock(long sleepTimeMillis,
              long expireTimeMillis) throws InterruptedException;

    /**
     * 功能描述:释放锁
     * 
     */
    void unlock();

}
