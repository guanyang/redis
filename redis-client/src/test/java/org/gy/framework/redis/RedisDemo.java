package org.gy.framework.redis;

import org.gy.framework.redis.impl.ShardedJedisClientImpl;

import redis.clients.jedis.ShardedJedis;

public class RedisDemo {

    public static void main(String[] args) {
        ShardedJedisClient client = new ShardedJedisClientImpl("redis.xml");

        final String key = "GY:DEMO:CACHE:TEST";

        String result = client.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                shardedJedis.setex(key, 1800, "test");
                return shardedJedis.get(key);
            }
        });
        System.out.println(result);
    }

}
