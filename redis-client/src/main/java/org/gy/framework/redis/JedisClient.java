package org.gy.framework.redis;


public interface JedisClient {

    public void destroy();

    public <T> T execute(JedisAction<T> action);

}
