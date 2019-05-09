package org.gy.framework.redis.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Client;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Slowlog;

public class SmartJedis extends Jedis {

    protected static final String COMBINESIGN = "___COMBINE___";

    private static final Logger   logger      = LoggerFactory.getLogger(SmartJedis.class);

    private SmartJedisPool        smartJedisPool;

    // smartJedis使用过程中，从JedisPool中借出来的Jedis资源
    private List<JedisResource>   resources   = new ArrayList<JedisResource>();

    private static final Random   RANDOM      = new Random();

    private Integer               dbIndex     = null;

    public SmartJedis(SmartJedisPool smartJedisPool) {
        this.smartJedisPool = smartJedisPool;
    }

    /**
     * 获取master jedis resource
     * 
     * @return master jedis resource
     */
    protected JedisResource getMasterJedisResource() {
        for (JedisResource resource : resources) {
            if (resource.isMaster) {
                return resource;
            }
        }
        JedisPool masterJedisPool = smartJedisPool.getMasterJedisPool();
        if (masterJedisPool == null) {
            throw new JedisConnectionException("Can not get master jedis pool.");
        }
        Jedis masterJedis = masterJedisPool.getResource();
        JedisResource masterResource = new JedisResource();
        masterResource.isMaster = true;
        masterResource.jedisPool = masterJedisPool;
        masterResource.jedis = masterJedis;
        resources.add(masterResource);
        if (dbIndex != null) {
            masterJedis.select(dbIndex);
        }
        return masterResource;
    }

    /**
     * 随机选择pool，并获取一个jedis
     * 
     * @return jedisResource
     */
    protected JedisResource getRandomJedisResource() {
        if (!resources.isEmpty()) {
            int size = resources.size();
            return resources.get(RANDOM.nextInt(size));
        }
        JedisPool jedisPool = null;
        List<JedisPool> jedisPools = smartJedisPool.getAvailableJedisPools();
        if (jedisPools != null && !jedisPools.isEmpty()) {
            int size = jedisPools.size();
            jedisPool = jedisPools.get(RANDOM.nextInt(size));
        }
        if (jedisPool == null) {
            logger.error("Can not get available jedis pool.");
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        JedisResource resource = new JedisResource();
        resource.isMaster = (jedisPool == smartJedisPool.getMasterJedisPool());
        resource.jedisPool = jedisPool;
        resource.jedis = jedis;
        resources.add(resource);
        if (dbIndex != null) {
            jedis.select(dbIndex);
        }
        return resource;
    }

    /**
     * 归还smartJedis使用过程中，从JedisPool借出来的Jedis资源。如果这些借出来的资源发生过JedisConnectionException，
     * 那么将对这些资源进行测试，确实连接出现问题的(broken)Jedis进行销毁（returnBrokenResource）
     * 注：为什么不在使用过程中，准确记录哪个jedis发生JedisConnectionException?因为，如果要记录，那么不仅是jedis，
     * Transaction、pipeline所有的API都需要改写，工作量大，也比较复杂。所以，采取这种简单的方案，应该对性能也不会有大的影响， 因为发生JedisConnectionException毕竟是少数。
     * 
     * @param isBroken 是否发生过JedisConnectionException
     */
    protected void cleanResources(boolean isBroken) {
        for (JedisResource resource : resources) {
            boolean isResourceBroken = false;
            if (isBroken) {
                try {
                    if (!resource.jedis.isConnected() || !resource.jedis.ping().equals("PONG")) {
                        isResourceBroken = true;
                    }
                } catch (final Exception e) {
                    isResourceBroken = true;
                }
            }
            if (isResourceBroken) {
                try {
                    smartJedisPool.increaseUnavailableCounter(resource.jedisPool);
                    resource.jedisPool.returnBrokenResource(resource.jedis);
                } catch (Exception ex) {
                    logger.warn("Can not return broken resource.", ex);
                }
            } else {
                try {
                    resource.jedisPool.returnResource(resource.jedis);
                    smartJedisPool.clearUnavailableCounter(resource.jedisPool);
                } catch (Exception ex) {
                    logger.warn("Can not return resource.", ex);
                }
            }
        }
        resources.clear();
        dbIndex = null;
    }

    protected class JedisResource {

        protected Jedis     jedis;

        protected JedisPool jedisPool;

        protected boolean   isMaster;

    }

    protected enum RW {
        R, W
    }

    private static interface Action<T> {

        public T doAction(Jedis jedis);

    }

    /**
     * 根据读写类型和本次操作的keys选择合适的jedis resource
     * 
     * @param rw 读写类型
     * @param keys 键
     * @return jedis resource
     */
    protected JedisResource chooseJedisResource(RW rw,
                                                String... keys) {
        JedisResource jedisResource = null;
        if (rw.equals(RW.R)) {
            Boolean flag = ReadInstructor.getCanReadSlaveFlag();
            jedisResource = (flag != null) ? ((flag) ? getRandomJedisResource() : getMasterJedisResource()) : (smartJedisPool.getRwPolicy().canReadSlave(keys) ? getRandomJedisResource() : getMasterJedisResource());
        }
        if (jedisResource == null) {
            jedisResource = getMasterJedisResource();
        }
        return jedisResource;
    }

    /**
     * 标注keys刚被修改过（发生过写操作）
     * 
     * @param keys keys
     */
    protected void keyHasBeenWritten(String... keys) {
        if (keys != null) {
            for (String key : keys) {
                smartJedisPool.getRwPolicy().keyHasBeenWritten(key);
            }
        }
    }

    private <T> T execute(Action<T> action,
                          RW rw,
                          String... keys) {
        JedisResource jedisResource = chooseJedisResource(rw, keys);
        long beginDate = System.currentTimeMillis();
        try {
            T result = action.doAction(jedisResource.jedis);
            if (rw.equals(RW.W)) {
                keyHasBeenWritten(keys);
            }
            return result;
        } catch (JedisException ex) {
            logger.error("Jedis exception occur when execute on jedis from " + jedisResource.jedisPool, ex);
            throw ex;
        } finally {
            long useTime = System.currentTimeMillis() - beginDate;
            if (useTime > smartJedisPool.execTimeThreshold) {
                if (keys != null && keys.length > 10) {
                    keys = Arrays.copyOf(keys, 10);
                }
                logger.warn("Method execution time is too long :{}ms, {}, at {}, keys:{}", useTime, new Throwable().getStackTrace()[1], jedisResource.jedisPool, Arrays.toString(keys));
            }
        }
    }

    private <T> T executeOnMaster(Action<T> action,
                                  String... keys) {
        JedisResource jedisResource = getMasterJedisResource();
        long beginDate = System.currentTimeMillis();
        try {
            return action.doAction(jedisResource.jedis);
        } catch (JedisException ex) {
            logger.error("Jedis exception occur when execute on jedis from " + jedisResource.jedisPool, ex);
            throw ex;
        } finally {
            long useTime = System.currentTimeMillis() - beginDate;
            if (useTime > smartJedisPool.execTimeThreshold) {
                if (keys != null && keys.length > 10) {
                    keys = Arrays.copyOf(keys, 10);
                }
                logger.warn("Method execution time is too long :{}ms, {}, at {}, keys:{}", useTime, new Throwable().getStackTrace()[1], jedisResource.jedisPool, Arrays.toString(keys));
            }
        }
    }

    @Override
    public String watch(final String... keys) {
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.watch(keys);
            }
        }, keys);
    }

    @Override
    public String watch(final byte[]... keys) {
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.watch(keys);
            }
        });
    }

    @Override
    public String unwatch() {
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.unwatch();
            }
        });
    }

    @Override
    public Transaction multi() {
        return executeOnMaster(new Action<Transaction>() {
            @Override
            public Transaction doAction(Jedis jedis) {
                return jedis.multi();
            }
        });
    }

    @Override
    public List<Object> multi(TransactionBlock jedisTransaction) {
        throw new UnsupportedOperationException();
    }

    protected void checkIsInMulti() {
        for (JedisResource resource : resources) {
            if (resource.jedis.getClient().isInMulti()) {
                throw new JedisDataException("Cannot use Jedis when in Multi. Please use JedisTransaction instead.");
            }
        }
    }

    @Override
    public String echo(final String message) {
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.echo(message);
            }
        });
    }

    @Override
    public byte[] echo(final byte[] message) {
        return executeOnMaster(new Action<byte[]>() {
            @Override
            public byte[] doAction(Jedis jedis) {
                return jedis.echo(message);
            }
        });
    }

    @Override
    public String ping() {
        checkIsInMulti();
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.ping();
            }
        });
    }

    @Override
    public Pipeline pipelined() {
        return new SmartPipeline(this);
    }

    @Deprecated
    @Override
    public List<Object> pipelined(PipelineBlock jedisPipeline) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String select(final int index) {
        checkIsInMulti();
        String result = null;
        for (JedisResource resource : resources) {
            result = resource.jedis.select(index);
        }
        dbIndex = index;
        return result;
    }

    @Override
    public Long getDB() {
        return dbIndex == null ? 0 : Long.parseLong(String.valueOf(dbIndex));
    }

    @Override
    public String auth(final String password) {
        checkIsInMulti();
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.auth(password);
            }
        });
    }

    @Override
    public String quit() {
        checkIsInMulti();
        String result = null;
        JedisException ex = null;
        for (JedisResource resource : resources) {
            try {
                result = resource.jedis.quit();
            } catch (JedisException e) {
                ex = e;
            }
        }
        if (ex != null) {
            throw ex;
        }
        return result;
    }

    // String PubSub Commands
    @Override
    public Long publish(final String channel,
                        final String message) {
        checkIsInMulti();
        return executeOnMaster(new Action<Long>() {
            @Override
            public Long doAction(Jedis jedis) {
                return jedis.publish(channel, message);
            }
        });
    }

    @Override
    public void subscribe(final JedisPubSub jedisPubSub,
                          final String... channels) {
        executeOnMaster(new Action<Object>() {
            @Override
            public Object doAction(Jedis jedis) {
                jedis.subscribe(jedisPubSub, channels);
                return null;
            }
        });
    }

    @Override
    public void psubscribe(final JedisPubSub jedisPubSub,
                           final String... patterns) {
        checkIsInMulti();
        executeOnMaster(new Action<Object>() {
            @Override
            public Object doAction(Jedis jedis) {
                jedis.psubscribe(jedisPubSub, patterns);
                return null;
            }
        });
    }

    @Override
    public List<String> pubsubChannels(final String pattern) {
        checkIsInMulti();
        return executeOnMaster(new Action<List<String>>() {
            @Override
            public List<String> doAction(Jedis jedis) {
                return jedis.pubsubChannels(pattern);
            }
        });
    }

    @Override
    public Long pubsubNumPat() {
        checkIsInMulti();
        return executeOnMaster(new Action<Long>() {
            @Override
            public Long doAction(Jedis jedis) {
                return jedis.pubsubNumPat();
            }
        });
    }

    @Override
    public Map<String, String> pubsubNumSub(final String... channels) {
        checkIsInMulti();
        return executeOnMaster(new Action<Map<String, String>>() {
            @Override
            public Map<String, String> doAction(Jedis jedis) {
                return jedis.pubsubNumSub(channels);
            }
        });
    }

    @Override
    public Long publish(final byte[] channel,
                        final byte[] message) {
        return executeOnMaster(new Action<Long>() {
            @Override
            public Long doAction(Jedis jedis) {
                return jedis.publish(channel, message);
            }
        });
    }

    @Override
    public void subscribe(final BinaryJedisPubSub jedisPubSub,
                          final byte[]... channels) {
        executeOnMaster(new Action<Object>() {
            @Override
            public Object doAction(Jedis jedis) {
                jedis.subscribe(jedisPubSub, channels);
                return null;
            }
        });
    }

    @Override
    public void psubscribe(final BinaryJedisPubSub jedisPubSub,
                           final byte[]... patterns) {
        executeOnMaster(new Action<Object>() {
            @Override
            public Object doAction(Jedis jedis) {
                jedis.psubscribe(jedisPubSub, patterns);
                return null;
            }
        });
    }

    @Override
    public Client getClient() {
        throw new UnsupportedOperationException();
    }

    // String JedisCommands and MultiKeyCommands
    @Override
    public String set(final String key,
                      final String value) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.set(key, value);
            }
        }, RW.W, key);
    }

    @Override
    public String get(final String key) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.get(key);
            }
        }, RW.R, key);
    }

    @Override
    public String set(final String key,
                      final String value,
                      final String nxxx,
                      final String expx,
                      final long time) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.set(key, value, nxxx, expx, time);
            }
        }, RW.W, key);
    }

    @Override
    public Boolean exists(final String key) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.exists(key);
            }
        }, RW.R, key);
    }

    @Override
    public Long del(final String... keys) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.del(keys);
            }
        }, RW.W, keys);
    }

    @Override
    public Long del(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.del(key);
            }
        }, RW.W, key);
    }

    @Override
    public String type(final String key) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.type(key);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> keys(final String pattern) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.keys(pattern);
            }
        }, RW.R);
    }

    @Override
    public String randomKey() {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.randomKey();
            }
        }, RW.R);
    }

    @Override
    public String rename(final String oldkey,
                         final String newkey) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.rename(oldkey, newkey);
            }
        }, RW.W, oldkey, newkey);
    }

    @Override
    public Long renamenx(final String oldkey,
                         final String newkey) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.renamenx(oldkey, newkey);
            }
        }, RW.W, oldkey, newkey);
    }

    @Override
    public Long expire(final String key,
                       final int seconds) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.expire(key, seconds);
            }
        }, RW.W, key);
    }

    @Override
    public Long expireAt(final String key,
                         final long unixTime) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.expireAt(key, unixTime);
            }
        }, RW.W, key);
    }

    @Override
    public Long ttl(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.ttl(key);
            }
        }, RW.R, key);
    }

    @Override
    public Long persist(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.persist(key);
            }
        }, RW.W, key);
    }

    @Override
    public Long move(final String key,
                     final int dbIndex) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.move(key, dbIndex);
            }
        }, RW.W, key);
    }

    @Override
    public byte[] dump(final String key) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.dump(key);
            }
        }, RW.W, key);
    }

    @Override
    public String restore(final String key,
                          final int ttl,
                          final byte[] serializedValue) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.restore(key, ttl, serializedValue);
            }
        }, RW.W, key);
    }

    @Override
    public String getSet(final String key,
                         final String value) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.getSet(key, value);
            }
        }, RW.W, key);
    }

    @Override
    public List<String> mget(final String... keys) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.mget(keys);
            }
        }, RW.R, keys);
    }

    @Override
    public Long setnx(final String key,
                      final String value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.setnx(key, value);
            }
        }, RW.W, key);
    }

    @Override
    public String setex(final String key,
                        final int seconds,
                        final String value) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.setex(key, seconds, value);
            }
        }, RW.W, key);
    }

    @Override
    public String mset(final String... keysvalues) {
        checkIsInMulti();
        String[] keys = null;
        if (keysvalues != null && keysvalues.length > 0) {
            int keyCount = keysvalues.length / 2;
            keys = new String[keyCount];
            for (int i = 0; i < keyCount; i++) {
                keys[i] = keysvalues[2 * i];
            }
        }
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.mset(keysvalues);
            }
        }, RW.W, keys);
    }

    @Override
    public Long msetnx(final String... keysvalues) {
        checkIsInMulti();
        String[] keys = null;
        if (keysvalues != null && keysvalues.length > 0) {
            int keyCount = keysvalues.length / 2;
            keys = new String[keyCount];
            for (int i = 0; i < keyCount; i++) {
                keys[i] = keysvalues[2 * i];
            }
        }
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.msetnx(keysvalues);
            }
        }, RW.W, keys);
    }

    @Override
    public Long decrBy(final String key,
                       final long integer) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.decrBy(key, integer);
            }
        }, RW.W, key);
    }

    @Override
    public Long decr(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.decr(key);
            }
        }, RW.W, key);
    }

    @Override
    public Long incrBy(final String key,
                       final long integer) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.incrBy(key, integer);
            }
        }, RW.W, key);
    }

    @Override
    public Long incr(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.incr(key);
            }
        }, RW.W, key);
    }

    @Override
    public Long append(final String key,
                       final String value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.append(key, value);
            }
        }, RW.W, key);
    }

    @Override
    public String substr(final String key,
                         final int start,
                         final int end) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.substr(key, start, end);
            }
        }, RW.R, key);
    }

    @Override
    public Long hset(final String key,
                     final String field,
                     final String value) {
        checkIsInMulti();
        String[] comKeys = combineKeyFiled(RW.W, key, field);
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.hset(key, field, value);
            }
        }, RW.W, comKeys);
    }

    @Override
    public String hget(final String key,
                       final String field) {
        checkIsInMulti();
        String[] comKeys = combineKeyFiled(RW.R, key, field);
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.hget(key, field);
            }
        }, RW.R, comKeys);
    }

    @Override
    public Long hsetnx(final String key,
                       final String field,
                       final String value) {
        checkIsInMulti();
        String[] comKeys = combineKeyFiled(RW.W, key, field);
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.hsetnx(key, field, value);
            }
        }, RW.W, comKeys);
    }

    @Override
    public String hmset(final String key,
                        final Map<String, String> hash) {
        checkIsInMulti();
        List<String> list = new ArrayList<String>();
        list.addAll(hash.keySet());
        String[] fields = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            fields[i] = list.get(i);
        }
        String[] comKeys = combineKeyFiled(RW.W, key, fields);
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.hmset(key, hash);
            }
        }, RW.W, comKeys);
    }

    @Override
    public List<String> hmget(final String key,
                              final String... fields) {
        checkIsInMulti();
        String[] comKeys = combineKeyFiled(RW.R, key, fields);
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.hmget(key, fields);
            }
        }, RW.R, comKeys);
    }

    @Override
    public Long hincrBy(final String key,
                        final String field,
                        final long value) {
        checkIsInMulti();
        String[] comKeys = combineKeyFiled(RW.W, key, field);
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.hincrBy(key, field, value);
            }
        }, RW.W, comKeys);
    }

    @Override
    public Boolean hexists(final String key,
                           final String field) {
        checkIsInMulti();
        String[] comKeys = combineKeyFiled(RW.R, key, field);
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.hexists(key, field);
            }
        }, RW.R, comKeys);
    }

    @Override
    public Long hdel(final String key,
                     final String... fields) {
        checkIsInMulti();
        String[] comKeys = combineKeyFiled(RW.W, key, fields);
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.hdel(key, fields);
            }
        }, RW.W, comKeys);
    }

    @Override
    public Long hlen(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.hlen(key);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> hkeys(final String key) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.hkeys(key);
            }
        }, RW.R, key);
    }

    @Override
    public List<String> hvals(final String key) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.hvals(key);
            }
        }, RW.R, key);
    }

    @Override
    public Map<String, String> hgetAll(final String key) {
        checkIsInMulti();
        return execute(new Action<Map<String, String>>() {
            public Map<String, String> doAction(Jedis jedis) {
                return jedis.hgetAll(key);
            }
        }, RW.R, key);
    }

    @Override
    public Long rpush(final String key,
                      final String... values) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.rpush(key, values);
            }
        }, RW.W, key);
    }

    @Override
    public Long lpush(final String key,
                      final String... values) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.lpush(key, values);
            }
        }, RW.W, key);
    }

    @Override
    public Long llen(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.llen(key);
            }
        }, RW.R, key);
    }

    @Override
    public List<String> lrange(final String key,
                               final long start,
                               final long end) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.lrange(key, start, end);
            }
        }, RW.R, key);
    }

    @Override
    public String lindex(final String key,
                         final long index) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.lindex(key, index);
            }
        }, RW.R, key);
    }

    @Override
    public String lset(final String key,
                       final long index,
                       final String value) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.lset(key, index, value);
            }
        }, RW.W, key);
    }

    @Override
    public Long lrem(final String key,
                     final long count,
                     final String value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.lrem(key, count, value);
            }
        }, RW.W, key);
    }

    @Override
    public String lpop(final String key) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.lpop(key);
            }
        }, RW.W, key);
    }

    @Override
    public String rpop(final String key) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.rpop(key);
            }
        }, RW.W, key);
    }

    @Override
    public String ltrim(final String key,
                        final long start,
                        final long end) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.ltrim(key, start, end);
            }
        }, RW.W, key);
    }

    @Override
    public Boolean setbit(final String key,
                          final long offset,
                          final boolean value) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.setbit(key, offset, value);
            }
        }, RW.W, key);
    }

    @Override
    public Boolean setbit(final String key,
                          final long offset,
                          final String value) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.setbit(key, offset, value);
            }
        }, RW.W, key);
    }

    @Override
    public Boolean getbit(final String key,
                          final long offset) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.getbit(key, offset);
            }
        }, RW.R, key);
    }

    @Override
    public Long setrange(final String key,
                         final long offset,
                         final String value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.setrange(key, offset, value);
            }
        }, RW.W, key);
    }

    @Override
    public String getrange(final String key,
                           final long startOffset,
                           final long endOffset) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.getrange(key, startOffset, endOffset);
            }
        }, RW.R, key);
    }

    @Override
    public Long strlen(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.strlen(key);
            }
        }, RW.R, key);
    }

    @Override
    public Long sadd(final String key,
                     final String... members) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sadd(key, members);
            }
        }, RW.W, key);
    }

    @Override
    public Set<String> smembers(final String key) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.smembers(key);
            }
        }, RW.R, key);
    }

    @Override
    public Long srem(final String key,
                     final String... members) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.srem(key, members);
            }
        }, RW.W, key);
    }

    @Override
    public String spop(final String key) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.spop(key);
            }
        }, RW.W, key);
    }

    @Override
    public Long scard(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.scard(key);
            }
        }, RW.R, key);
    }

    @Override
    public Boolean sismember(final String key,
                             final String member) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.sismember(key, member);
            }
        }, RW.R, key);
    }

    @Override
    public String srandmember(final String key) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.srandmember(key);
            }
        }, RW.R, key);
    }

    @Override
    public List<String> srandmember(final String key,
                                    final int count) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.srandmember(key, count);
            }
        }, RW.R, key);
    }

    @Override
    public Long zadd(final String key,
                     final double score,
                     final String member) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zadd(key, score, member);
            }
        }, RW.W, key);
    }

    @Override
    public Long zadd(final String key,
                     final Map<String, Double> scoreMembers) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zadd(key, scoreMembers);
            }
        }, RW.W, key);
    }

    @Override
    public Set<String> zrange(final String key,
                              final long start,
                              final long end) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.zrange(key, start, end);
            }
        }, RW.R, key);
    }

    @Override
    public Long zrem(final String key,
                     final String... members) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zrem(key, members);
            }
        }, RW.W, key);
    }

    @Override
    public Double zincrby(final String key,
                          final double score,
                          final String member) {
        checkIsInMulti();
        return execute(new Action<Double>() {
            public Double doAction(Jedis jedis) {
                return jedis.zincrby(key, score, member);
            }
        }, RW.W, key);
    }

    @Override
    public Long zrank(final String key,
                      final String member) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zrank(key, member);
            }
        }, RW.R, key);
    }

    @Override
    public Long zrevrank(final String key,
                         final String member) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zrevrank(key, member);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> zrevrange(final String key,
                                 final long start,
                                 final long end) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.zrevrange(key, start, end);
            }
        }, RW.R, key);
    }

    @Override
    public Set<Tuple> zrangeWithScores(final String key,
                                       final long start,
                                       final long end) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrangeWithScores(key, start, end);
            }
        }, RW.R, key);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(final String key,
                                          final long start,
                                          final long end) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrevrangeWithScores(key, start, end);
            }
        }, RW.R, key);
    }

    @Override
    public Long zcard(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zcard(key);
            }
        }, RW.R, key);
    }

    @Override
    public Double zscore(final String key,
                         final String member) {
        checkIsInMulti();
        return execute(new Action<Double>() {
            public Double doAction(Jedis jedis) {
                return jedis.zscore(key, member);
            }
        }, RW.R, key);
    }

    @Override
    public List<String> sort(final String key) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.sort(key);
            }
        }, RW.W, key);
    }

    @Override
    public List<String> sort(final String key,
                             final SortingParams sortingParameters) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.sort(key, sortingParameters);
            }
        }, RW.W, key);
    }

    @Override
    public Long zcount(final String key,
                       final double min,
                       final double max) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zcount(key, min, max);
            }
        }, RW.R, key);
    }

    @Override
    public Long zcount(final String key,
                       final String min,
                       final String max) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zcount(key, min, max);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> zrangeByScore(final String key,
                                     final double min,
                                     final double max) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> zrangeByScore(final String key,
                                     final String min,
                                     final String max) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> zrevrangeByScore(final String key,
                                        final double max,
                                        final double min) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> zrevrangeByScore(final String key,
                                        final String max,
                                        final String min) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> zrangeByScore(final String key,
                                     final double min,
                                     final double max,
                                     final int offset,
                                     final int count) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max, offset, count);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> zrangeByScore(final String key,
                                     final String min,
                                     final String max,
                                     final int offset,
                                     final int count) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max, offset, count);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> zrevrangeByScore(final String key,
                                        final double max,
                                        final double min,
                                        final int offset,
                                        final int count) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min, offset, count);
            }
        }, RW.R, key);
    }

    @Override
    public Set<String> zrevrangeByScore(final String key,
                                        final String max,
                                        final String min,
                                        final int offset,
                                        final int count) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min, offset, count);
            }
        }, RW.R, key);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key,
                                              final double min,
                                              final double max) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max);
            }
        }, RW.R, key);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key,
                                              final String min,
                                              final String max) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max);
            }
        }, RW.R, key);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key,
                                              final double min,
                                              final double max,
                                              final int offset,
                                              final int count) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }, RW.R, key);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key,
                                              final String min,
                                              final String max,
                                              final int offset,
                                              final int count) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }, RW.R, key);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key,
                                                 final String max,
                                                 final String min) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min);
            }
        }, RW.R, key);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key,
                                                 final double max,
                                                 final double min) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min);
            }
        }, RW.R, key);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key,
                                                 final double max,
                                                 final double min,
                                                 final int offset,
                                                 final int count) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }, RW.R, key);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key,
                                                 final String max,
                                                 final String min,
                                                 final int offset,
                                                 final int count) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }, RW.R, key);
    }

    @Override
    public Long zremrangeByRank(final String key,
                                final long start,
                                final long end) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zremrangeByRank(key, start, end);
            }
        }, RW.W, key);
    }

    @Override
    public Long zremrangeByScore(final String key,
                                 final double start,
                                 final double end) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zremrangeByScore(key, start, end);
            }
        }, RW.W, key);
    }

    @Override
    public Long zremrangeByScore(final String key,
                                 final String start,
                                 final String end) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zremrangeByScore(key, start, end);
            }
        }, RW.W, key);
    }

    @Override
    public Long linsert(final String key,
                        final LIST_POSITION where,
                        final String pivot,
                        final String value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.linsert(key, where, pivot, value);
            }
        }, RW.W, key);
    }

    @Override
    public Long lpushx(final String key,
                       final String... strings) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.lpushx(key, strings);
            }
        }, RW.W, key);
    }

    @Override
    public Long rpushx(final String key,
                       final String... strings) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.rpushx(key, strings);
            }
        }, RW.W, key);
    }

    @Override
    public List<String> blpop(final String key) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.blpop(key);
            }
        }, RW.W, key);
    }

    @Override
    public List<String> brpop(final String key) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.brpop(key);
            }
        }, RW.W, key);
    }

    @Override
    public Long bitcount(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.bitcount(key);
            }
        }, RW.R, key);
    }

    @Override
    public Long bitcount(final String key,
                         final long start,
                         final long end) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.bitcount(key, start, end);
            }
        }, RW.R, key);
    }

    @Deprecated
    @Override
    public ScanResult<Entry<String, String>> hscan(final String key,
                                                   final int cursor) {
        checkIsInMulti();
        return execute(new Action<ScanResult<Entry<String, String>>>() {
            public ScanResult<Entry<String, String>> doAction(Jedis jedis) {
                return jedis.hscan(key, cursor);
            }
        }, RW.R, key);
    }

    @Deprecated
    @Override
    public ScanResult<Entry<String, String>> hscan(final String key,
                                                   final int cursor,
                                                   final ScanParams params) {
        checkIsInMulti();
        return execute(new Action<ScanResult<Entry<String, String>>>() {
            public ScanResult<Entry<String, String>> doAction(Jedis jedis) {
                return jedis.hscan(key, cursor, params);
            }
        }, RW.R, key);
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(final String key,
                                                   final String cursor) {
        checkIsInMulti();
        return execute(new Action<ScanResult<Entry<String, String>>>() {
            public ScanResult<Entry<String, String>> doAction(Jedis jedis) {
                return jedis.hscan(key, cursor);
            }
        }, RW.R, key);
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(final String key,
                                                   final String cursor,
                                                   final ScanParams params) {
        checkIsInMulti();
        return execute(new Action<ScanResult<Entry<String, String>>>() {
            public ScanResult<Entry<String, String>> doAction(Jedis jedis) {
                return jedis.hscan(key, cursor, params);
            }
        }, RW.R, key);
    }

    @Deprecated
    @Override
    public ScanResult<String> sscan(final String key,
                                    final int cursor) {
        checkIsInMulti();
        return execute(new Action<ScanResult<String>>() {
            public ScanResult<String> doAction(Jedis jedis) {
                return jedis.sscan(key, cursor);
            }
        }, RW.R, key);
    }

    @Deprecated
    @Override
    public ScanResult<String> sscan(final String key,
                                    final int cursor,
                                    final ScanParams params) {
        checkIsInMulti();
        return execute(new Action<ScanResult<String>>() {
            public ScanResult<String> doAction(Jedis jedis) {
                return jedis.sscan(key, cursor, params);
            }
        }, RW.R, key);
    }

    @Override
    public ScanResult<String> sscan(final String key,
                                    final String cursor) {
        checkIsInMulti();
        return execute(new Action<ScanResult<String>>() {
            public ScanResult<String> doAction(Jedis jedis) {
                return jedis.sscan(key, cursor);
            }
        }, RW.R, key);
    }

    @Override
    public ScanResult<String> sscan(final String key,
                                    final String cursor,
                                    final ScanParams params) {
        checkIsInMulti();
        return execute(new Action<ScanResult<String>>() {
            public ScanResult<String> doAction(Jedis jedis) {
                return jedis.sscan(key, cursor, params);
            }
        }, RW.R, key);
    }

    @Deprecated
    @Override
    public ScanResult<Tuple> zscan(final String key,
                                   final int cursor) {
        checkIsInMulti();
        return execute(new Action<ScanResult<Tuple>>() {
            public ScanResult<Tuple> doAction(Jedis jedis) {
                return jedis.zscan(key, cursor);
            }
        }, RW.R, key);
    }

    @Deprecated
    @Override
    public ScanResult<Tuple> zscan(final String key,
                                   final int cursor,
                                   final ScanParams params) {
        checkIsInMulti();
        return execute(new Action<ScanResult<Tuple>>() {
            public ScanResult<Tuple> doAction(Jedis jedis) {
                return jedis.zscan(key, cursor, params);
            }
        }, RW.R, key);
    }

    @Override
    public ScanResult<Tuple> zscan(final String key,
                                   final String cursor) {
        checkIsInMulti();
        return execute(new Action<ScanResult<Tuple>>() {
            public ScanResult<Tuple> doAction(Jedis jedis) {
                return jedis.zscan(key, cursor);
            }
        }, RW.R, key);
    }

    @Override
    public ScanResult<Tuple> zscan(final String key,
                                   final String cursor,
                                   final ScanParams params) {
        checkIsInMulti();
        return execute(new Action<ScanResult<Tuple>>() {
            public ScanResult<Tuple> doAction(Jedis jedis) {
                return jedis.zscan(key, cursor, params);
            }
        }, RW.R, key);
    }

    @Override
    public List<String> blpop(final int timeout,
                              final String... keys) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.blpop(timeout, keys);
            }
        }, RW.W, keys);
    }

    @Override
    public List<String> brpop(final int timeout,
                              final String... keys) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.brpop(timeout, keys);
            }
        }, RW.W, keys);
    }

    @Override
    public List<String> blpop(final String... keys) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.blpop(keys);
            }
        }, RW.W, keys);
    }

    @Override
    public List<String> brpop(final String... keys) {
        checkIsInMulti();
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.brpop(keys);
            }
        }, RW.W, keys);
    }

    @Override
    public String rpoplpush(final String srcKey,
                            final String dstKey) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.rpoplpush(srcKey, dstKey);
            }
        }, RW.W, srcKey, dstKey);
    }

    @Override
    public Set<String> sdiff(final String... keys) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.sdiff(keys);
            }
        }, RW.R, keys);
    }

    @Override
    public Long sdiffstore(final String dstKey,
                           final String... keys) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sdiffstore(dstKey, keys);
            }
        }, RW.W, dstKey);
    }

    @Override
    public Set<String> sinter(final String... keys) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.sinter(keys);
            }
        }, RW.R, keys);
    }

    @Override
    public Long sinterstore(final String dstKey,
                            final String... keys) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sinterstore(dstKey, keys);
            }
        }, RW.W, dstKey);
    }

    @Override
    public Long smove(final String srcKey,
                      final String dstKey,
                      final String member) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.smove(srcKey, dstKey, member);
            }
        }, RW.W, dstKey, srcKey);
    }

    @Override
    public Long sort(final String key,
                     final SortingParams sortingParameters,
                     final String dstKey) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sort(key, sortingParameters, dstKey);
            }
        }, RW.W, dstKey);
    }

    @Override
    public Long sort(final String key,
                     final String dstKey) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sort(key, dstKey);
            }
        }, RW.W, dstKey);
    }

    @Override
    public Set<String> sunion(final String... keys) {
        checkIsInMulti();
        return execute(new Action<Set<String>>() {
            public Set<String> doAction(Jedis jedis) {
                return jedis.sunion(keys);
            }
        }, RW.R, keys);
    }

    @Override
    public Long sunionstore(final String dstKey,
                            final String... keys) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sunionstore(dstKey, keys);
            }
        }, RW.W, dstKey);
    }

    @Override
    public Long zinterstore(final String dstKey,
                            final String... sets) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zinterstore(dstKey, sets);
            }
        }, RW.W, dstKey);
    }

    @Override
    public Long zinterstore(final String dstKey,
                            final ZParams params,
                            final String... sets) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zinterstore(dstKey, params, sets);
            }
        }, RW.W, dstKey);
    }

    @Override
    public Long zunionstore(final String dstKey,
                            final String... sets) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zunionstore(dstKey, sets);
            }
        }, RW.W, dstKey);
    }

    @Override
    public Long zunionstore(final String dstKey,
                            final ZParams params,
                            final String... sets) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zunionstore(dstKey, params, sets);
            }
        }, RW.W, dstKey);
    }

    @Override
    public String brpoplpush(final String source,
                             final String destination,
                             final int timeout) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.brpoplpush(source, destination, timeout);
            }
        }, RW.W, source, destination);
    }

    @Override
    public Long bitop(final BitOP op,
                      final String dstKey,
                      final String... srcKeys) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.bitop(op, dstKey, srcKeys);
            }
        }, RW.W, dstKey);
    }

    @Deprecated
    @Override
    public ScanResult<String> scan(final int cursor) {
        checkIsInMulti();
        return execute(new Action<ScanResult<String>>() {
            public ScanResult<String> doAction(Jedis jedis) {
                return jedis.scan(cursor);
            }
        }, RW.R);
    }

    @Deprecated
    @Override
    public ScanResult<String> scan(final int cursor,
                                   final ScanParams params) {
        checkIsInMulti();
        return execute(new Action<ScanResult<String>>() {
            public ScanResult<String> doAction(Jedis jedis) {
                return jedis.scan(cursor, params);
            }
        }, RW.R);
    }

    @Override
    public ScanResult<String> scan(final String cursor,
                                   final ScanParams params) {
        checkIsInMulti();
        return execute(new Action<ScanResult<String>>() {
            public ScanResult<String> doAction(Jedis jedis) {
                return jedis.scan(cursor, params);
            }
        }, RW.R);
    }

    @Override
    public ScanResult<String> scan(final String cursor) {
        checkIsInMulti();
        return execute(new Action<ScanResult<String>>() {
            public ScanResult<String> doAction(Jedis jedis) {
                return jedis.scan(cursor);
            }
        }, RW.R);
    }

    @Override
    public Long pexpire(final String key,
                        final int milliseconds) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.pexpire(key, milliseconds);
            }
        }, RW.W, key);
    }

    @Override
    public Long pexpireAt(final String key,
                          final long millisecondsTimestamp) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.pexpireAt(key, millisecondsTimestamp);
            }
        }, RW.W, key);
    }

    @Override
    public Long pttl(final String key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.pttl(key);
            }
        }, RW.R, key);
    }

    @Override
    public Double incrByFloat(final String key,
                              final double increment) {
        checkIsInMulti();
        return execute(new Action<Double>() {
            public Double doAction(Jedis jedis) {
                return jedis.incrByFloat(key, increment);
            }
        }, RW.W, key);
    }

    @Override
    public String psetex(final String key,
                         final int milliseconds,
                         final String value) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.psetex(key, milliseconds, value);
            }
        }, RW.W, key);
    }

    @Override
    public String set(final String key,
                      final String value,
                      final String nxxx) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.set(key, value, nxxx);
            }
        }, RW.W, key);
    }

    @Override
    public String set(final String key,
                      final String value,
                      final String nxxx,
                      final String expx,
                      final int time) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.set(key, value, nxxx, expx, time);
            }
        }, RW.W, key);
    }

    @Override
    public String migrate(final String host,
                          final int port,
                          final String key,
                          final int destinationDb,
                          final int timeout) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.migrate(key, timeout, key, timeout, timeout);
            }
        }, RW.W, key);
    }

    @Override
    public Double hincrByFloat(final String key,
                               final String field,
                               final double increment) {
        checkIsInMulti();
        String[] comKeys = combineKeyFiled(RW.W, key, field);
        return execute(new Action<Double>() {
            public Double doAction(Jedis jedis) {
                return jedis.hincrByFloat(key, field, increment);
            }
        }, RW.W, comKeys);
    }

    private String[] combineKeyFiled(RW rw,
                                     String key,
                                     String... fields) {
        int count = fields.length;
        String[] comKeys;
        if (rw.equals(RW.R)) {
            comKeys = new String[count];
            for (int i = 0; i < count; i++) {
                comKeys[i] = key + COMBINESIGN + fields[i];
            }
        } else {
            comKeys = new String[count + 1];
            for (int i = 0; i < count; i++) {
                comKeys[i] = key + COMBINESIGN + fields[i];
            }
            comKeys[count] = key;
        }
        return comKeys;
    }

    @Override
    public String set(final byte[] key,
                      final byte[] value) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.set(key, value);
            }
        }, RW.W);
    }

    @Override
    public byte[] get(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.get(key);
            }
        }, RW.R);
    }

    @Override
    public String set(final byte[] key,
                      final byte[] value,
                      final byte[] nxxx,
                      final byte[] expx,
                      final long time) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.set(key, value, nxxx, expx, time);
            }
        }, RW.W);
    }

    @Override
    public Boolean exists(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.exists(key);
            }
        }, RW.R);
    }

    @Override
    public Long del(final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.del(keys);
            }
        }, RW.W);
    }

    @Override
    public Long del(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.del(key);
            }
        }, RW.W);
    }

    @Override
    public String type(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.type(key);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> keys(final byte[] pattern) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.keys(pattern);
            }
        }, RW.R);
    }

    @Override
    public byte[] randomBinaryKey() {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.randomBinaryKey();
            }
        }, RW.R);
    }

    @Override
    public String rename(final byte[] oldkey,
                         final byte[] newkey) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.rename(oldkey, newkey);
            }
        }, RW.W);
    }

    @Override
    public Long renamenx(final byte[] oldkey,
                         final byte[] newkey) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.renamenx(oldkey, newkey);
            }
        }, RW.W);
    }

    @Override
    public Long expire(final byte[] key,
                       final int seconds) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.expire(key, seconds);
            }
        }, RW.W);
    }

    @Override
    public Long expireAt(final byte[] key,
                         final long unixTime) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.expireAt(key, unixTime);
            }
        }, RW.W);
    }

    @Override
    public Long ttl(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.ttl(key);
            }
        }, RW.R);
    }

    @Override
    public Long persist(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.persist(key);
            }
        }, RW.W);
    }

    @Override
    public byte[] dump(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.dump(key);
            }
        }, RW.W);
    }

    @Override
    public String restore(final byte[] key,
                          final int ttl,
                          final byte[] serializedValue) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.restore(key, ttl, serializedValue);
            }
        }, RW.W);
    }

    @Override
    public Long move(final byte[] key,
                     final int dbIndex) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.move(key, dbIndex);
            }
        }, RW.W);
    }

    @Override
    public byte[] getSet(final byte[] key,
                         final byte[] value) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.getSet(key, value);
            }
        }, RW.W);
    }

    @Override
    public List<byte[]> mget(final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.mget(keys);
            }
        }, RW.R);
    }

    @Override
    public Long setnx(final byte[] key,
                      final byte[] value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.setnx(key, value);
            }
        }, RW.W);
    }

    @Override
    public String setex(final byte[] key,
                        final int seconds,
                        final byte[] value) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.setex(key, seconds, value);
            }
        }, RW.W);
    }

    @Override
    public String mset(final byte[]... keysvalues) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.mset(keysvalues);
            }
        }, RW.W);
    }

    @Override
    public Long msetnx(final byte[]... keysvalues) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.msetnx(keysvalues);
            }
        }, RW.W);
    }

    @Override
    public Long decrBy(final byte[] key,
                       final long integer) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.decrBy(key, integer);
            }
        }, RW.W);
    }

    @Override
    public Long decr(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.decr(key);
            }
        }, RW.W);
    }

    @Override
    public Long incrBy(final byte[] key,
                       final long integer) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.incrBy(key, integer);
            }
        }, RW.W);
    }

    @Override
    public Long incr(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.incr(key);
            }
        }, RW.W);
    }

    @Override
    public Long append(final byte[] key,
                       final byte[] value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.append(key, value);
            }
        }, RW.W);
    }

    @Override
    public byte[] substr(final byte[] key,
                         final int start,
                         final int end) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.substr(key, start, end);
            }
        }, RW.R);
    }

    @Override
    public Long hset(final byte[] key,
                     final byte[] field,
                     final byte[] value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.hset(key, field, value);
            }
        }, RW.W);
    }

    @Override
    public byte[] hget(final byte[] key,
                       final byte[] field) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.hget(key, field);
            }
        }, RW.R);
    }

    @Override
    public Long hsetnx(final byte[] key,
                       final byte[] field,
                       final byte[] value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.hsetnx(key, field, value);
            }
        }, RW.W);
    }

    @Override
    public String hmset(final byte[] key,
                        final Map<byte[], byte[]> hash) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.hmset(key, hash);
            }
        }, RW.W);
    }

    @Override
    public List<byte[]> hmget(final byte[] key,
                              final byte[]... fields) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.hmget(key, fields);
            }
        }, RW.R);
    }

    @Override
    public Long hincrBy(final byte[] key,
                        final byte[] field,
                        final long value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.hincrBy(key, field, value);
            }
        }, RW.W);
    }

    @Override
    public Boolean hexists(final byte[] key,
                           final byte[] field) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.hexists(key, field);
            }
        }, RW.R);
    }

    @Override
    public Long hdel(final byte[] key,
                     final byte[]... fields) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.hdel(key, fields);
            }
        }, RW.W);
    }

    @Override
    public Long hlen(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.hlen(key);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> hkeys(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.hkeys(key);
            }
        }, RW.R);
    }

    @Override
    public List<byte[]> hvals(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.hvals(key);
            }
        }, RW.R);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Map<byte[], byte[]>>() {
            public Map<byte[], byte[]> doAction(Jedis jedis) {
                return jedis.hgetAll(key);
            }
        }, RW.R);
    }

    @Override
    public Long rpush(final byte[] key,
                      final byte[]... values) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.rpush(key, values);
            }
        }, RW.W);
    }

    @Override
    public Long lpush(final byte[] key,
                      final byte[]... values) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.lpush(key, values);
            }
        }, RW.W);
    }

    @Override
    public Long llen(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.llen(key);
            }
        }, RW.R);
    }

    @Override
    public List<byte[]> lrange(final byte[] key,
                               final long start,
                               final long end) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.lrange(key, start, end);
            }
        }, RW.R);
    }

    @Override
    public byte[] lindex(final byte[] key,
                         final long index) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.lindex(key, index);
            }
        }, RW.R);
    }

    @Override
    public String lset(final byte[] key,
                       final long index,
                       final byte[] value) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.lset(key, index, value);
            }
        }, RW.W);
    }

    @Override
    public Long lrem(final byte[] key,
                     final long count,
                     final byte[] value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.lrem(key, count, value);
            }
        }, RW.W);
    }

    @Override
    public byte[] lpop(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.lpop(key);
            }
        }, RW.W);
    }

    @Override
    public byte[] rpop(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.rpop(key);
            }
        }, RW.W);
    }

    @Override
    public String ltrim(final byte[] key,
                        final long start,
                        final long end) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.ltrim(key, start, end);
            }
        }, RW.W);
    }

    @Override
    public Boolean setbit(final byte[] key,
                          final long offset,
                          final boolean value) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.setbit(key, offset, value);
            }
        }, RW.W);
    }

    @Override
    public Boolean setbit(final byte[] key,
                          final long offset,
                          final byte[] value) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.setbit(key, offset, value);
            }
        }, RW.W);
    }

    @Override
    public Boolean getbit(final byte[] key,
                          final long offset) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.getbit(key, offset);
            }
        }, RW.R);
    }

    @Override
    public Long setrange(final byte[] key,
                         final long offset,
                         final byte[] value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.setrange(key, offset, value);
            }
        }, RW.W);
    }

    @Override
    public byte[] getrange(final byte[] key,
                           final long startOffset,
                           final long endOffset) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.getrange(key, startOffset, endOffset);
            }
        }, RW.R);
    }

    @Override
    public Long strlen(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.strlen(key);
            }
        }, RW.R);
    }

    @Override
    public Long sadd(final byte[] key,
                     final byte[]... members) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sadd(key, members);
            }
        }, RW.W);
    }

    @Override
    public Set<byte[]> smembers(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.smembers(key);
            }
        }, RW.R);
    }

    @Override
    public Long srem(final byte[] key,
                     final byte[]... members) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.srem(key, members);
            }
        }, RW.W);
    }

    @Override
    public byte[] spop(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.spop(key);
            }
        }, RW.W);
    }

    @Override
    public Long scard(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.scard(key);
            }
        }, RW.R);
    }

    @Override
    public Boolean sismember(final byte[] key,
                             final byte[] member) {
        checkIsInMulti();
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.sismember(key, member);
            }
        }, RW.R);
    }

    @Override
    public byte[] srandmember(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.srandmember(key);
            }
        }, RW.R);
    }

    @Override
    public List<byte[]> srandmember(final byte[] key,
                                    final int count) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.srandmember(key, count);
            }
        }, RW.R);
    }

    @Override
    public Long zadd(final byte[] key,
                     final double score,
                     final byte[] member) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zadd(key, score, member);
            }
        }, RW.W);
    }

    @Override
    public Long zadd(final byte[] key,
                     final Map<byte[], Double> scoreMembers) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zadd(key, scoreMembers);
            }
        }, RW.W);
    }

    @Override
    public Set<byte[]> zrange(final byte[] key,
                              final long start,
                              final long end) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.zrange(key, start, end);
            }
        }, RW.R);
    }

    @Override
    public Long zrem(final byte[] key,
                     final byte[]... members) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zrem(key, members);
            }
        }, RW.W);
    }

    @Override
    public Double zincrby(final byte[] key,
                          final double score,
                          final byte[] member) {
        checkIsInMulti();
        return execute(new Action<Double>() {
            public Double doAction(Jedis jedis) {
                return jedis.zincrby(key, score, member);
            }
        }, RW.W);
    }

    @Override
    public Long zrank(final byte[] key,
                      final byte[] member) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zrank(key, member);
            }
        }, RW.R);
    }

    @Override
    public Long zrevrank(final byte[] key,
                         final byte[] member) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zrevrank(key, member);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> zrevrange(final byte[] key,
                                 final long start,
                                 final long end) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.zrevrange(key, start, end);
            }
        }, RW.R);
    }

    @Override
    public Set<Tuple> zrangeWithScores(final byte[] key,
                                       final long start,
                                       final long end) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrangeWithScores(key, start, end);
            }
        }, RW.R);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(final byte[] key,
                                          final long start,
                                          final long end) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrevrangeWithScores(key, start, end);
            }
        }, RW.R);
    }

    @Override
    public Long zcard(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zcard(key);
            }
        }, RW.R);
    }

    @Override
    public Double zscore(final byte[] key,
                         final byte[] member) {
        checkIsInMulti();
        return execute(new Action<Double>() {
            public Double doAction(Jedis jedis) {
                return jedis.zscore(key, member);
            }
        }, RW.R);
    }

    @Override
    public List<byte[]> sort(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.sort(key);
            }
        }, RW.W);
    }

    @Override
    public List<byte[]> sort(final byte[] key,
                             final SortingParams sortingParameters) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.sort(key, sortingParameters);
            }
        }, RW.W);
    }

    @Override
    public Long zcount(final byte[] key,
                       final double min,
                       final double max) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zcount(key, min, max);
            }
        }, RW.R);
    }

    @Override
    public Long zcount(final byte[] key,
                       final byte[] min,
                       final byte[] max) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zcount(key, min, max);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> zrangeByScore(final byte[] key,
                                     final double min,
                                     final double max) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> zrangeByScore(final byte[] key,
                                     final byte[] min,
                                     final byte[] max) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key,
                                        final double max,
                                        final double min) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key,
                                        final byte[] max,
                                        final byte[] min) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> zrangeByScore(final byte[] key,
                                     final double min,
                                     final double max,
                                     final int offset,
                                     final int count) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max, offset, count);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> zrangeByScore(final byte[] key,
                                     final byte[] min,
                                     final byte[] max,
                                     final int offset,
                                     final int count) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max, offset, count);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key,
                                        final double max,
                                        final double min,
                                        final int offset,
                                        final int count) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min, offset, count);
            }
        }, RW.R);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key,
                                        final byte[] max,
                                        final byte[] min,
                                        final int offset,
                                        final int count) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min, offset, count);
            }
        }, RW.R);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key,
                                              final double min,
                                              final double max) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max);
            }
        }, RW.R);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key,
                                              final byte[] min,
                                              final byte[] max) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max);
            }
        }, RW.R);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key,
                                              final double min,
                                              final double max,
                                              final int offset,
                                              final int count) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }, RW.R);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key,
                                              final byte[] min,
                                              final byte[] max,
                                              final int offset,
                                              final int count) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }, RW.R);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key,
                                                 final byte[] max,
                                                 final byte[] min) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min);
            }
        }, RW.R);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key,
                                                 final double max,
                                                 final double min) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min);
            }
        }, RW.R);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key,
                                                 final double max,
                                                 final double min,
                                                 final int offset,
                                                 final int count) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }, RW.R);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key,
                                                 final byte[] max,
                                                 final byte[] min,
                                                 final int offset,
                                                 final int count) {
        checkIsInMulti();
        return execute(new Action<Set<Tuple>>() {
            public Set<Tuple> doAction(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }, RW.R);
    }

    @Override
    public Long zremrangeByRank(final byte[] key,
                                final long start,
                                final long end) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zremrangeByRank(key, start, end);
            }
        }, RW.W);
    }

    @Override
    public Long zremrangeByScore(final byte[] key,
                                 final double start,
                                 final double end) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zremrangeByScore(key, start, end);
            }
        }, RW.W);
    }

    @Override
    public Long zremrangeByScore(final byte[] key,
                                 final byte[] start,
                                 final byte[] end) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zremrangeByScore(key, start, end);
            }
        }, RW.W);
    }

    @Override
    public Long linsert(final byte[] key,
                        final LIST_POSITION where,
                        final byte[] pivot,
                        final byte[] value) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.linsert(key, where, pivot, value);
            }
        }, RW.W);
    }

    @Override
    public Long lpushx(final byte[] key,
                       final byte[]... strings) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.lpushx(key, strings);
            }
        }, RW.W);
    }

    @Override
    public Long rpushx(final byte[] key,
                       final byte[]... strings) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.rpushx(key, strings);
            }
        }, RW.W);
    }

    @Override
    public List<byte[]> blpop(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.blpop(key);
            }
        }, RW.W);
    }

    @Override
    public List<byte[]> brpop(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.brpop(key);
            }
        }, RW.W);
    }

    @Override
    public Long bitcount(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.bitcount(key);
            }
        }, RW.R);
    }

    @Override
    public Long bitcount(final byte[] key,
                         final long start,
                         final long end) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.bitcount(key, start, end);
            }
        }, RW.R);
    }

    @Override
    public List<byte[]> blpop(final int timeout,
                              final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.blpop(timeout, keys);
            }
        }, RW.W);
    }

    @Override
    public List<byte[]> brpop(final int timeout,
                              final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.brpop(timeout, keys);
            }
        }, RW.W);
    }

    @Override
    public List<byte[]> blpop(final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.blpop(keys);
            }
        }, RW.W);
    }

    @Override
    public List<byte[]> brpop(final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<List<byte[]>>() {
            public List<byte[]> doAction(Jedis jedis) {
                return jedis.brpop(keys);
            }
        }, RW.W);
    }

    @Override
    public byte[] rpoplpush(final byte[] srcKey,
                            final byte[] dstKey) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.rpoplpush(srcKey, dstKey);
            }
        }, RW.W);
    }

    @Override
    public Set<byte[]> sdiff(final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.sdiff(keys);
            }
        }, RW.R);
    }

    @Override
    public Long sdiffstore(final byte[] dstKey,
                           final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sdiffstore(dstKey, keys);
            }
        }, RW.W);
    }

    @Override
    public Set<byte[]> sinter(final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.sinter(keys);
            }
        }, RW.R);
    }

    @Override
    public Long sinterstore(final byte[] dstKey,
                            final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sinterstore(dstKey, keys);
            }
        }, RW.W);
    }

    @Override
    public Long smove(final byte[] srcKey,
                      final byte[] dstKey,
                      final byte[] member) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.smove(srcKey, dstKey, member);
            }
        }, RW.W);
    }

    @Override
    public Long sort(final byte[] key,
                     final SortingParams sortingParameters,
                     final byte[] dstKey) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sort(key, sortingParameters, dstKey);
            }
        }, RW.W);
    }

    @Override
    public Long sort(final byte[] key,
                     final byte[] dstKey) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sort(key, dstKey);
            }
        }, RW.W);
    }

    @Override
    public Set<byte[]> sunion(final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<Set<byte[]>>() {
            public Set<byte[]> doAction(Jedis jedis) {
                return jedis.sunion(keys);
            }
        }, RW.R);
    }

    @Override
    public Long sunionstore(final byte[] dstKey,
                            final byte[]... keys) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.sunionstore(dstKey, keys);
            }
        }, RW.W);
    }

    @Override
    public Long zinterstore(final byte[] dstKey,
                            final byte[]... sets) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zinterstore(dstKey, sets);
            }
        }, RW.W);
    }

    @Override
    public Long zinterstore(final byte[] dstKey,
                            final ZParams params,
                            final byte[]... sets) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zinterstore(dstKey, params, sets);
            }
        }, RW.W);
    }

    @Override
    public Long zunionstore(final byte[] dstKey,
                            final byte[]... sets) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zunionstore(dstKey, sets);
            }
        }, RW.W);
    }

    @Override
    public Long zunionstore(final byte[] dstKey,
                            final ZParams params,
                            final byte[]... sets) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.zunionstore(dstKey, params, sets);
            }
        }, RW.W);
    }

    @Override
    public byte[] brpoplpush(final byte[] source,
                             final byte[] destination,
                             final int timeout) {
        checkIsInMulti();
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.brpoplpush(source, destination, timeout);
            }
        }, RW.W);
    }

    @Override
    public Long bitop(final BitOP op,
                      final byte[] dstKey,
                      final byte[]... srcKeys) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.bitop(op, dstKey, srcKeys);
            }
        }, RW.W);
    }

    @Override
    public Long pexpire(final byte[] key,
                        final int milliseconds) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.pexpire(key, milliseconds);
            }
        }, RW.W);
    }

    @Override
    public Long pexpireAt(final byte[] key,
                          final long millisecondsTimestamp) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.pexpireAt(key, millisecondsTimestamp);
            }
        }, RW.W);
    }

    @Override
    public Long pttl(final byte[] key) {
        checkIsInMulti();
        return execute(new Action<Long>() {
            public Long doAction(Jedis jedis) {
                return jedis.pttl(key);
            }
        }, RW.R);
    }

    @Override
    public Double incrByFloat(final byte[] key,
                              final double increment) {
        checkIsInMulti();
        return execute(new Action<Double>() {
            public Double doAction(Jedis jedis) {
                return jedis.incrByFloat(key, increment);
            }
        }, RW.W);
    }

    @Override
    public String psetex(final byte[] key,
                         final int milliseconds,
                         final byte[] value) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.psetex(key, milliseconds, value);
            }
        }, RW.W);
    }

    @Override
    public String set(final byte[] key,
                      final byte[] value,
                      final byte[] nxxx) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.set(key, value, nxxx);
            }
        }, RW.W);
    }

    @Override
    public String set(final byte[] key,
                      final byte[] value,
                      final byte[] nxxx,
                      final byte[] expx,
                      final int time) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.set(key, value, nxxx, expx, time);
            }
        }, RW.W);
    }

    @Override
    public String migrate(final byte[] host,
                          final int port,
                          final byte[] key,
                          final int destinationDb,
                          final int timeout) {
        checkIsInMulti();
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.migrate(host, port, key, destinationDb, timeout);
            }
        }, RW.W);
    }

    @Override
    public Double hincrByFloat(final byte[] key,
                               final byte[] field,
                               final double increment) {
        checkIsInMulti();
        return execute(new Action<Double>() {
            public Double doAction(Jedis jedis) {
                return jedis.hincrByFloat(key, field, increment);
            }
        }, RW.W);
    }

    @Override
    public Object eval(final String script,
                       final int keyCount,
                       final String... params) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.eval(script, keyCount, params);
            }
        }, RW.W);
    }

    @Override
    public Object eval(final String script,
                       final List<String> keys,
                       final List<String> args) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.eval(script, keys, args);
            }
        }, RW.W);
    }

    @Override
    public Object eval(final String script) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.eval(script);
            }
        }, RW.W);
    }

    @Override
    public Object evalsha(final String script) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.evalsha(script);
            }
        }, RW.W);
    }

    @Override
    public Object evalsha(final String sha1,
                          final List<String> keys,
                          final List<String> args) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.evalsha(sha1, keys, args);
            }
        }, RW.W);
    }

    @Override
    public Object evalsha(final String sha1,
                          final int keyCount,
                          final String... params) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.evalsha(sha1, keyCount, params);
            }
        }, RW.W);
    }

    @Override
    public Boolean scriptExists(final String sha1) {
        return execute(new Action<Boolean>() {
            public Boolean doAction(Jedis jedis) {
                return jedis.scriptExists(sha1);
            }
        }, RW.W);
    }

    @Override
    public List<Boolean> scriptExists(final String... sha1) {
        return execute(new Action<List<Boolean>>() {
            public List<Boolean> doAction(Jedis jedis) {
                return jedis.scriptExists(sha1);
            }
        }, RW.W);
    }

    @Override
    public String scriptLoad(final String script) {
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.scriptLoad(script);
            }
        }, RW.W);
    }

    @Override
    public Object eval(final byte[] script,
                       final byte[] keyCount,
                       final byte[]... params) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.eval(script, keyCount, params);
            }
        }, RW.W);
    }

    @Override
    public Object eval(final byte[] script,
                       final int keyCount,
                       final byte[]... params) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.eval(script, keyCount, params);
            }
        }, RW.W);
    }

    @Override
    public Object eval(final byte[] script,
                       final List<byte[]> keys,
                       final List<byte[]> args) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.eval(script, keys, args);
            }
        }, RW.W);
    }

    @Override
    public Object eval(final byte[] script) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.eval(script);
            }
        }, RW.W);
    }

    @Override
    public Object evalsha(final byte[] script) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.evalsha(script);
            }
        }, RW.W);
    }

    @Override
    public Object evalsha(final byte[] sha1,
                          final List<byte[]> keys,
                          final List<byte[]> args) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.evalsha(sha1, keys, args);
            }
        }, RW.W);
    }

    @Override
    public Object evalsha(final byte[] sha1,
                          final int keyCount,
                          final byte[]... params) {
        return execute(new Action<Object>() {
            public Object doAction(Jedis jedis) {
                return jedis.evalsha(sha1, keyCount, params);
            }
        }, RW.W);
    }

    @Override
    public List<Long> scriptExists(final byte[]... sha1) {
        return execute(new Action<List<Long>>() {
            public List<Long> doAction(Jedis jedis) {
                return jedis.scriptExists(sha1);
            }
        }, RW.W);
    }

    @Override
    public byte[] scriptLoad(final byte[] script) {
        return execute(new Action<byte[]>() {
            public byte[] doAction(Jedis jedis) {
                return jedis.scriptLoad(script);
            }
        }, RW.W);
    }

    @Override
    public String scriptFlush() {
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.scriptFlush();
            }
        }, RW.W);
    }

    @Override
    public String scriptKill() {
        return execute(new Action<String>() {
            public String doAction(Jedis jedis) {
                return jedis.scriptKill();
            }
        }, RW.W);
    }

    @Override
    public List<String> configGet(String pattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String configSet(String parameter,
                            String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String slowlogReset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long slowlogLen() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Slowlog> slowlogGet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Slowlog> slowlogGet(long entries) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long objectRefcount(String string) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String objectEncoding(String string) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long objectIdletime(String string) {
        throw new UnsupportedOperationException();
    }

    public List<byte[]> configGet(byte[] pattern) {
        throw new UnsupportedOperationException();
    }

    public byte[] configSet(byte[] parameter,
                            byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<byte[]> slowlogGetBinary() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<byte[]> slowlogGetBinary(long entries) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long objectRefcount(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] objectEncoding(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long objectIdletime(byte[] key) {
        throw new UnsupportedOperationException();
    }

    // BasicCommands
    @Override
    public String flushDB() {
        checkIsInMulti();
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.flushDB();
            }
        });
    }

    @Override
    public Long dbSize() {
        checkIsInMulti();
        return executeOnMaster(new Action<Long>() {
            @Override
            public Long doAction(Jedis jedis) {
                return jedis.dbSize();
            }
        });
    }

    @Override
    public String flushAll() {
        checkIsInMulti();
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.flushAll();
            }
        });
    }

    @Override
    public String save() {
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.save();
            }
        });
    }

    @Override
    public String bgsave() {
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.bgsave();
            }
        });
    }

    @Override
    public String bgrewriteaof() {
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.bgrewriteaof();
            }
        });
    }

    @Override
    public Long lastsave() {
        return executeOnMaster(new Action<Long>() {
            @Override
            public Long doAction(Jedis jedis) {
                return jedis.lastsave();
            }
        });
    }

    @Override
    public String shutdown() {
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.shutdown();
            }
        });
    }

    @Override
    public String info() {
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.info();
            }
        });
    }

    @Override
    public String info(final String section) {
        return executeOnMaster(new Action<String>() {
            @Override
            public String doAction(Jedis jedis) {
                return jedis.info(section);
            }
        });
    }

    @Override
    public String slaveof(String host,
                          int port) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String slaveofNoOne() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String debug(DebugParams params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String configResetStat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long waitReplicas(int replicas,
                             long timeout) {
        throw new UnsupportedOperationException();
    }

    // SentinelCommands and ClusterCommands Unsupported
    @Override
    public List<Map<String, String>> sentinelMasters() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> sentinelGetMasterAddrByName(String masterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sentinelReset(String pattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Map<String, String>> sentinelSlaves(String masterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String sentinelFailover(String masterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String sentinelMonitor(String masterName,
                                  String ip,
                                  int port,
                                  int quorum) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String sentinelRemove(String masterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String sentinelSet(String masterName,
                              Map<String, String> parameterMap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clusterNodes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clusterMeet(final String ip,
                              final int port) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clusterAddSlots(final int... slots) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clusterDelSlots(final int... slots) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clusterInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> clusterGetKeysInSlot(final int slot,
                                             final int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clusterSetSlotNode(final int slot,
                                     final String nodeId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clusterSetSlotMigrating(final int slot,
                                          final String nodeId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clusterSetSlotImporting(final int slot,
                                          final String nodeId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String asking() {
        throw new UnsupportedOperationException();
    }

    // server Commands Unsupported
    @Override
    public String clientKill(final String client) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clientSetname(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clientKill(final byte[] client) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clientGetname() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clientList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String clientSetname(final byte[] name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void connect() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void disconnect() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isConnected() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetState() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> time() {
        return execute(new Action<List<String>>() {
            public List<String> doAction(Jedis jedis) {
                return jedis.time();
            }
        }, RW.W);
    }
}
