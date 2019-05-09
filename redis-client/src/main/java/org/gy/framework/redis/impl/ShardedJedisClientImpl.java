package org.gy.framework.redis.impl;

import org.gy.framework.redis.ShardedJedisAction;
import org.gy.framework.redis.ShardedJedisClient;
import org.gy.framework.redis.exception.RedisClientException;
import org.gy.framework.redis.support.DiscardShardException;
import org.gy.framework.redis.support.SmartShardedJedisPool;
import org.gy.framework.redis.support.WarningService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class ShardedJedisClientImpl extends AbstractClient implements ShardedJedisClient {

    private static final Logger logger = LoggerFactory.getLogger(ShardedJedisClientImpl.class);

    public ShardedJedisClientImpl(String configFile) {
        super(configFile);
    }

    public ShardedJedisClientImpl(String configPath, WarningService warningService) {
        super(configPath, warningService);
    }

    @Override
    public void destroy() {
        super.destroy();
    }

    @Override
    public <T> T execute(ShardedJedisAction<T> action) {
        T result;
        try {
            result = executeAction(action);
        } catch (Exception e) {
            throw new RedisClientException(e);
        }
        return result;
    }

    private <T> T executeAction(ShardedJedisAction<T> action) {
        SmartShardedJedisPool pool = getSmartShardedJedisPool();
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = pool.getResource();
            return action.doAction(shardedJedis);
        } catch (DiscardShardException e) {
            logger.warn("ExecuteAction DiscardShardException.", e);
            return null;
        } catch (JedisConnectionException jex) {
            if (shardedJedis != null) {
                try {
                    pool.returnBrokenResource(shardedJedis);
                } catch (Exception ex) {
                    logger.warn("Can not return broken resource.", ex);
                }
                shardedJedis = null;
            }
            throw jex;
        } finally {
            if (shardedJedis != null) {
                try {
                    pool.returnResource(shardedJedis);
                } catch (Exception ex) {
                    logger.warn("Can not return resource.", ex);
                }
            }
        }
    }
}
