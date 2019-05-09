package org.gy.framework.redis.impl;

import java.util.ArrayList;
import java.util.List;

import org.gy.framework.redis.JedisAction;
import org.gy.framework.redis.JedisClient;
import org.gy.framework.redis.exception.RedisClientException;
import org.gy.framework.redis.support.SmartJedisPool;
import org.gy.framework.redis.support.SmartShardedJedisPool;
import org.gy.framework.redis.support.WarningService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisClientImpl extends AbstractClient implements JedisClient {

    private static final Logger logger = LoggerFactory.getLogger(JedisClientImpl.class);

    private String              shardName;

    public JedisClientImpl(String configPath) {
        super(configPath);
        init();
    }

    public JedisClientImpl(String configPath, WarningService warningService) {
        super(configPath, warningService);
        init();
    }

    public JedisClientImpl(String configPath, String shardName) {
        super(configPath);
        init(shardName);
    }

    public JedisClientImpl(String configPath, String shardName, WarningService warningService) {
        super(configPath, warningService);
        init(shardName);
    }

    private void init() {
        SmartShardedJedisPool smartShardedJedisPool = getSmartShardedJedisPool();
        List<String> shardNames = new ArrayList<>(smartShardedJedisPool.getShardNames());
        if (shardNames.size() != 1) {
            throw new IllegalArgumentException("config contain multi shard,must specify one shard name.");
        }
    }

    private void init(String shardName) {
        this.shardName = shardName;
    }

    @Override
    public void destroy() {
        SmartShardedJedisPool smartShardedJedisPool = getSmartShardedJedisPool();
        List<String> shardNames = new ArrayList<>(smartShardedJedisPool.getShardNames());
        if (shardNames.size() == 1) {
            super.destroy();
        }
    }

    @Override
    public <T> T execute(JedisAction<T> action) {
        T result;
        try {
            result = executeAction(action);
        } catch (Exception e) {
            throw new RedisClientException(e);
        }
        return result;
    }

    private <T> T executeAction(JedisAction<T> action) {
        SmartJedisPool smartJedisPool;
        if (shardName != null) {
            SmartShardedJedisPool smartShardedJedisPool = getSmartShardedJedisPool();
            smartJedisPool = smartShardedJedisPool.getJedisPool(shardName);
            if (smartJedisPool == null) {
                throw new RedisClientException("can not get smartJedisPool , please check shardName");
            }
        } else {
            SmartShardedJedisPool smartShardedJedisPool = getSmartShardedJedisPool();
            List<String> shardNames = new ArrayList<>(smartShardedJedisPool.getShardNames());
            if (shardNames.size() != 1) {
                throw new IllegalArgumentException("config contain multi shard,must specify one shard name.");
            }
            smartJedisPool = smartShardedJedisPool.getJedisPool(shardNames.get(0));
        }
        Jedis jedis = null;
        try {
            jedis = smartJedisPool.getResource();
            return action.doAction(jedis);
        } catch (JedisConnectionException jex) {
            if (jedis != null) {
                try {
                    smartJedisPool.returnBrokenResource(jedis);
                } catch (Exception ex) {
                    logger.warn("Can not return broken resource.", ex);
                }
                jedis = null;
            }
            throw jex;
        } finally {
            if (jedis != null) {
                try {
                    smartJedisPool.returnResource(jedis);
                } catch (Exception ex) {
                    logger.warn("Can not return resource.", ex);
                }
            }
        }
    }
}
