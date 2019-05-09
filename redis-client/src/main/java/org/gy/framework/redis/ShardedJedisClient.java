package org.gy.framework.redis;


public interface ShardedJedisClient {

    public void destroy();

    public <T> T execute(ShardedJedisAction<T> action);

}
