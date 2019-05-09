package org.gy.framework.redis.lock.support;

import org.gy.framework.redis.JedisClient;
import org.gy.framework.redis.ShardedJedisClient;
import org.gy.framework.redis.impl.ShardedJedisClientImpl;

/**
 * 功能描述：
 * 
 */
public class DistributedLockFactory {

    private DistributedLockFactory() {
    }

    public static DistributedLock getShardedJedisLock(ShardedJedisClient shardedJedisClient,
                                                      String lockKey) {
        return new ShardedJedisDistributedLock(shardedJedisClient, lockKey);
    }

    public static DistributedLock getJedisLock(JedisClient jedisClient,
                                               String lockKey) {
        return new JedisDistributedLock(jedisClient, lockKey);
    }

    public static void main(String[] args) {
        final ShardedJedisClient shardedJedisClient = new ShardedJedisClientImpl("conf/redis-config-demo.xml");
        final String lockKey = "gy:lock:test";
        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {

                @Override
                public void run() {
                    doBiz(shardedJedisClient, lockKey);
                }
            }).start();
        }
    }

    /**
     * 功能描述:
     * 
     * @param shardedJedisClient
     * @param lockKey
     */
    private static void doBiz(ShardedJedisClient shardedJedisClient,
                              String lockKey) {
        DistributedLock lock = DistributedLockFactory.getShardedJedisLock(shardedJedisClient, lockKey);
        boolean flag = false;
        try {
            flag = lock.tryLock(5000, 100, 10000);
            System.out.println(Thread.currentThread().getId() + "获取锁：" + flag);
            if (flag) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (flag) {
                System.out.println(Thread.currentThread().getId() + "释放锁");
            }
            lock.unlock();
        }
    }
}
