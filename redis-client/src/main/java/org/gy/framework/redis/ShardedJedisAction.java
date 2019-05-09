package org.gy.framework.redis;

import redis.clients.jedis.ShardedJedis;

public interface ShardedJedisAction<T> {
    
    public T doAction(ShardedJedis shardedJedis);
}
