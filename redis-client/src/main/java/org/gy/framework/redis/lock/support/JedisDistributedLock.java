package org.gy.framework.redis.lock.support;

import java.util.Collections;
import java.util.UUID;

import org.gy.framework.redis.JedisAction;
import org.gy.framework.redis.JedisClient;

import redis.clients.jedis.Jedis;

/**
 * 功能描述：
 * 
 */
public class JedisDistributedLock extends AbstractJedisDistributedLock implements DistributedLock {

    private final JedisClient jedisClient;

    private final String      lockKey;

    private final String      requestId;

    private volatile boolean  locked = false;

    public JedisDistributedLock(JedisClient jedisClient, String lockKey) {
        this(jedisClient, lockKey, UUID.randomUUID().toString());
    }

    public JedisDistributedLock(JedisClient jedisClient, String lockKey, String requestId) {
        this.jedisClient = jedisClient;
        this.lockKey = DEFAULT_LOCK_PREFIX + lockKey;
        this.requestId = requestId;
    }

    @Override
    public boolean innerLockHandler(final long expireTimeMillis) {
        String result = jedisClient.execute(new JedisAction<String>() {
            @Override
            public String doAction(Jedis jedis) {
                // 从 Redis 2.6.12 版本开始， SET 在设置操作成功完成时，才返回 OK
                return jedis.set(lockKey, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTimeMillis);
            }
        });
        if (LOCK_SUCCESS.equals(result)) {
            locked = true;
            return true;
        }
        return false;
    }

    @Override
    public boolean innerReleaseHandler() {
        if (locked) {
            Object result = jedisClient.execute(new JedisAction<Object>() {
                @Override
                public Object doAction(Jedis jedis) {
                    // 获取redis中的值，验证是否与之前设置的值相等，如果相等，则删除，避免删除掉其他线程的锁
                    // 采用lua脚本，保证原子性操作
                    return jedis.eval(SCRIPT, Collections.singletonList(lockKey), Collections.singletonList(requestId));
                }
            });
            if (RELEASE_SUCCESS.equals(result)) {
                locked = false;
                return true;
            }
        }
        return false;
    }

}
