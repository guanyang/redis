package org.gy.framework.redis.support;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;

public class PoolStatusUtil {

    private static final Logger     logger = LoggerFactory.getLogger(PoolStatusUtil.class);

    private static SimpleDateFormat df     = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String getPoolStatus(SmartShardedJedisPool smartShardedJedisPool) {
        String poolStatus = df.format(new Date());
        poolStatus = poolStatus + "  SmartShardedJedisPool:" + getStatus(smartShardedJedisPool);
        List<String> shardNames = smartShardedJedisPool.getShardNames();
        for (String shardName : shardNames) {
            SmartJedisPool smartJedisPool = smartShardedJedisPool.getJedisPool(shardName);
            poolStatus = poolStatus + "  SmartJedisPool_" + shardName + ":" + getStatus(smartJedisPool);
            List<JedisPool> pools = smartJedisPool.getAvailableJedisPools();
            for (JedisPool pool : pools) {
                poolStatus = poolStatus + "  " + pool + ":" + getStatus(pool);
            }
        }
        return poolStatus;
    }

    public static void setMaxTotal(SmartShardedJedisPool smartShardedJedisPool,
                                   String maxTotal) {
        if (null != maxTotal && !"".equals(maxTotal.trim())) {
            List<String> shardNames = smartShardedJedisPool.getShardNames();
            setMaxTotal(smartShardedJedisPool, shardNames.size() * 2 * Integer.valueOf(maxTotal));
            for (String shardName : shardNames) {
                SmartJedisPool smartJedisPool = smartShardedJedisPool.getJedisPool(shardName);
                List<JedisPool> pools = smartJedisPool.getAvailableJedisPools();
                setMaxTotal(smartJedisPool, Integer.valueOf(maxTotal) * 2);
                for (JedisPool pool : pools) {
                    setMaxTotal(pool, Integer.valueOf(maxTotal));
                }
            }
        }
    }

    public static void setMaxIdle(SmartShardedJedisPool smartShardedJedisPool,
                                  String maxIdle) {
        if (null != maxIdle && !"".equals(maxIdle.trim())) {
            List<String> shardNames = smartShardedJedisPool.getShardNames();
            setMaxIdle(smartShardedJedisPool, shardNames.size() * 2 * Integer.valueOf(maxIdle));
            for (String shardName : shardNames) {
                SmartJedisPool smartJedisPool = smartShardedJedisPool.getJedisPool(shardName);
                List<JedisPool> pools = smartJedisPool.getAvailableJedisPools();
                setMaxIdle(smartJedisPool, Integer.valueOf(maxIdle) * 2);
                for (JedisPool pool : pools) {
                    setMaxIdle(pool, Integer.valueOf(maxIdle));
                }
            }
        }
    }

    public static void setMinIdle(SmartShardedJedisPool smartShardedJedisPool,
                                  String minIdle) {
        if (null != minIdle && !"".equals(minIdle.trim())) {
            List<String> shardNames = smartShardedJedisPool.getShardNames();
            setMinIdle(smartShardedJedisPool, shardNames.size() * 2 * Integer.valueOf(minIdle));
            for (String shardName : shardNames) {
                SmartJedisPool smartJedisPool = smartShardedJedisPool.getJedisPool(shardName);
                List<JedisPool> pools = smartJedisPool.getAvailableJedisPools();
                setMinIdle(smartJedisPool, Integer.valueOf(minIdle) * 2);
                for (JedisPool pool : pools) {
                    setMinIdle(pool, Integer.valueOf(minIdle));
                }
            }
        }
    }

    public static void setMaxWait(SmartShardedJedisPool smartShardedJedisPool,
                                  String maxWaitMillis) {
        if (null != maxWaitMillis && !"".equals(maxWaitMillis.trim())) {
            List<String> shardNames = smartShardedJedisPool.getShardNames();
            setMaxWaitMillis(smartShardedJedisPool, Long.valueOf(maxWaitMillis));
            for (String shardName : shardNames) {
                SmartJedisPool smartJedisPool = smartShardedJedisPool.getJedisPool(shardName);
                List<JedisPool> pools = smartJedisPool.getAvailableJedisPools();
                setMaxWaitMillis(smartJedisPool, Long.valueOf(maxWaitMillis));
                for (JedisPool pool : pools) {
                    setMaxWaitMillis(pool, Long.valueOf(maxWaitMillis));
                }
            }
        }
    }

    public static void setBlockWhenExhausted(SmartShardedJedisPool smartShardedJedisPool,
                                             boolean block) {
        List<String> shardNames = smartShardedJedisPool.getShardNames();
        setBlock(smartShardedJedisPool, block);
        for (String shardName : shardNames) {
            SmartJedisPool smartJedisPool = smartShardedJedisPool.getJedisPool(shardName);
            List<JedisPool> pools = smartJedisPool.getAvailableJedisPools();
            setBlock(smartJedisPool, block);
            for (JedisPool pool : pools) {
                setBlock(pool, block);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private static void setMaxTotal(Pool pool,
                                    int maxTotal) {
        GenericObjectPool internalPool;
        try {
            internalPool = getInternalPool(pool);
            internalPool.setMaxTotal(maxTotal);
        } catch (SecurityException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage(), e);
        } catch (NoSuchFieldException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @SuppressWarnings("rawtypes")
    private static void setMaxIdle(Pool pool,
                                   int maxIdle) {
        GenericObjectPool internalPool;
        try {
            internalPool = getInternalPool(pool);
            internalPool.setMaxIdle(maxIdle);
        } catch (SecurityException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage(), e);
        } catch (NoSuchFieldException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @SuppressWarnings("rawtypes")
    private static void setMinIdle(Pool pool,
                                   int minIdle) {
        GenericObjectPool internalPool;
        try {
            internalPool = getInternalPool(pool);
            internalPool.setMinIdle(minIdle);
        } catch (SecurityException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage(), e);
        } catch (NoSuchFieldException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @SuppressWarnings("rawtypes")
    private static void setMaxWaitMillis(Pool pool,
                                         long maxWaitMillis) {
        GenericObjectPool internalPool;
        try {
            internalPool = getInternalPool(pool);
            internalPool.setMaxWaitMillis(maxWaitMillis);
        } catch (SecurityException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage(), e);
        } catch (NoSuchFieldException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @SuppressWarnings("rawtypes")
    private static void setBlock(Pool pool,
                                 boolean block) {
        GenericObjectPool internalPool;
        try {
            internalPool = getInternalPool(pool);
            internalPool.setBlockWhenExhausted(block);
        } catch (SecurityException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage(), e);
        } catch (NoSuchFieldException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @SuppressWarnings("rawtypes")
    private static String getStatus(Pool pool) {
        GenericObjectPool internalPool;
        String result = null;
        try {
            internalPool = getInternalPool(pool);
            int numActive = internalPool.getNumActive();
            int numIdle = internalPool.getNumIdle();
            int numMaxTotal = internalPool.getMaxTotal();
            int numMaxIdle = internalPool.getMaxIdle();
            result = "[active=" + numActive + "/" + numMaxTotal + ", idle=" + numIdle + "/" + numMaxIdle + "]";
        } catch (SecurityException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage(), e);
        } catch (NoSuchFieldException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }
        return result;
    }

    @SuppressWarnings("rawtypes")
    private static GenericObjectPool getInternalPool(Pool pool) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Class<?> father = pool.getClass().getSuperclass();
        if (father != Pool.class) {
            father = father.getSuperclass();
        }
        Field f = father.getDeclaredField("internalPool");
        f.setAccessible(true);
        return (GenericObjectPool) f.get(pool);
    }
}
