package org.gy.framework.redis.support;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryShardedJedis;
import redis.clients.jedis.Builder;
import redis.clients.jedis.Client;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisDataException;

public class SmartShardedJedisPipeline extends ShardedJedisPipeline {

    private SmartShardedJedis              smartShardedJedis;

    private Map<SmartJedis, SmartPipeline> map           = new HashMap<SmartJedis, SmartPipeline>();

    private Queue<SmartPipeline>           pipelines     = new LinkedList<SmartPipeline>();

    private ShardSelector                  shardSelector = new DefaultShardSelector();

    public SmartShardedJedisPipeline(SmartShardedJedis smartShardedJedis) {
        this.smartShardedJedis = smartShardedJedis;
    }

    public SmartShardedJedisPipeline(SmartShardedJedis smartShardedJedis, ShardSelector shardSelector) {
        this.smartShardedJedis = smartShardedJedis;
        this.shardSelector = shardSelector;
    }

    public interface ShardSelector {

        public String selectShard(SmartShardedJedis ssj,
                                  String key);

        public String selectShard(SmartShardedJedis ssj,
                                  byte[] key);
    }

    public static class DefaultShardSelector implements ShardSelector {
        @Override
        public String selectShard(SmartShardedJedis ssj,
                                  String key) {
            JedisShardInfo shardInfo = ssj.getShardInfo(key);
            return shardInfo.getName();
        }

        @Override
        public String selectShard(SmartShardedJedis ssj,
                                  byte[] key) {
            JedisShardInfo shardInfo = ssj.getShardInfo(key);
            return shardInfo.getName();
        }
    }

    @Override
    public void sync() {
        // flush所有shardJedis下的所有master/slaves Jedis socket buffer
        for (SmartPipeline smartPipeline : map.values()) {
            smartPipeline.flush();
        }
        for (SmartPipeline smartPipeline : map.values()) {
            smartPipeline.generateResponses();
        }
    }

    @Override
    public List<Object> syncAndReturnAll() {
        // flush所有shardJedis下的所有master/slaves Jedis socket buffer
        for (SmartPipeline smartPipeline : map.values()) {
            smartPipeline.flush();
        }
        // smartPipeline和response队列的mapping
        Map<SmartPipeline, Queue<Response<?>>> temp = new HashMap<>();
        for (SmartPipeline smartPipeline : map.values()) {
            List<Response<?>> responses = smartPipeline.generateResponses();
            Queue<Response<?>> queue = new LinkedList<>(responses);
            temp.put(smartPipeline, queue);
        }
        List<Object> formatted = new ArrayList<>();
        // 根据执行顺序，取各response队列中的结果
        for (SmartPipeline pipeline : pipelines) {
            Queue<Response<?>> queue = temp.get(pipeline);
            try {
                formatted.add(queue.poll().get());
            } catch (JedisDataException e) {
                formatted.add(e);
            }
        }
        return formatted;
    }

    @Override
    public List<Object> getResults() {
        // flush all client socket buffer.
        for (SmartPipeline smartPipeline : map.values()) {
            smartPipeline.flush();
        }
        Map<SmartPipeline, Queue<Object>> temp = new HashMap<>();
        for (SmartPipeline smartPipeline : map.values()) {
            List<Object> objects = smartPipeline.getResults();
            Queue<Object> queue = new LinkedList<>(objects);
            temp.put(smartPipeline, queue);
        }
        List<Object> results = new ArrayList<>();
        for (SmartPipeline pipeline : pipelines) {
            Queue<Object> queue = temp.get(pipeline);
            results.add(queue.poll());
        }
        return results;
    }

    @Override
    public void setShardedJedis(BinaryShardedJedis jedis) {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    protected Client getClient(String key) {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    protected Client getClient(byte[] key) {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    protected void clean() {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    protected Response<?> generateResponse(Object data) {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    protected <T> Response<T> getResponse(Builder<T> builder) {
        throw new IllegalStateException("This method should not be invoked.");
    }

    private SmartPipeline getSmartPipeline(String key) {
        String shardName = shardSelector.selectShard(smartShardedJedis, key);
        return getSmartPipelineByShardName(shardName);
    }

    private SmartPipeline getSmartPipeline(byte[] key) {
        String shardName = shardSelector.selectShard(smartShardedJedis, key);
        return getSmartPipelineByShardName(shardName);
    }

    public SmartPipeline getSmartPipeline(JedisShardInfo shardInfo) {
        return getSmartPipelineByShardName(shardInfo.getName());
    }

    public SmartPipeline getSmartPipelineByShardName(String shardName) {
        SmartJedis smartJedis = null;
        try {
            smartJedis = smartShardedJedis.getSmartShard(shardName);
        } catch (DiscardShardException e) {
            return new DiscartPipeline(smartJedis);
        }
        SmartPipeline pipeline = map.get(smartJedis);
        if (pipeline == null) {
            pipeline = (SmartPipeline) smartJedis.pipelined();
            map.put(smartJedis, pipeline);
        }
        return pipeline;
    }

    /**
     * 在指定shard上执行mget
     * 
     * @param shardInfo shard
     * @param keys keys
     * @return response
     */
    protected Response<List<String>> mgetOnShard(JedisShardInfo shardInfo,
                                                 String... keys) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.mget(keys);
    }

    /**
     * 在指定shard上执行mget
     * 
     * @param shardInfo shard
     * @param keys keys
     * @return response
     */
    protected Response<List<byte[]>> mgetOnShard(JedisShardInfo shardInfo,
                                                 byte[]... keys) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.mget(keys);
    }

    /**
     * 在指定shard上执行mset
     * 
     * @param shardInfo shard
     * @param keysvalues keysvalues
     * @return response
     */
    protected Response<String> msetOnShard(JedisShardInfo shardInfo,
                                           String... keysvalues) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.mset(keysvalues);
    }

    /**
     * 在指定shard上执行mset
     * 
     * @param shardInfo shard
     * @param keysvalues keysvalues
     * @return response
     */
    protected Response<String> msetOnShard(JedisShardInfo shardInfo,
                                           byte[]... keysvalues) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.mset(keysvalues);
    }

    /**
     * 在指定shard上执行del
     * 
     * @param shardInfo shard
     * @param keys keys
     * @return response
     */
    protected Response<Long> delOnShard(JedisShardInfo shardInfo,
                                        String... keys) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.del(keys);
    }

    /**
     * 在指定shard上执行del
     * 
     * @param shardInfo shard
     * @param keys keys
     * @return response
     */
    protected Response<Long> delOnShard(JedisShardInfo shardInfo,
                                        byte[]... keys) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.del(keys);
    }

    /**
     * 在指定shard上执行exists
     * 
     * @param shardInfo shard
     * @param keys keys
     * @return response
     */
    protected Response<Long> existsOnShard(JedisShardInfo shardInfo,
                                           String... keys) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.exists(keys);
    }

    /**
     * 在指定shard上执行exists
     * 
     * @param shardInfo shard
     * @param keys keys
     * @return response
     */
    protected Response<Long> existsOnShard(JedisShardInfo shardInfo,
                                           byte[]... keys) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.exists(keys);
    }
    
    /**
     * 在指定shard上执行pfcount
     * 
     * @param shardInfo shard
     * @param keys keys
     * @return response
     */
    protected Response<Long> pfcountOnShard(JedisShardInfo shardInfo,
                                           String... keys) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.pfcount(keys);
    }

    /**
     * 在指定shard上执行exists
     * 
     * @param shardInfo shard
     * @param keys keys
     * @return response
     */
    protected Response<Long> pfcountOnShard(JedisShardInfo shardInfo,
                                           byte[]... keys) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.pfcount(keys);
    }

    /**
     * 在指定shard上执行keys
     * 
     * @param shardInfo shard
     * @param pattern pattern
     * @return response
     */
    protected Response<Set<String>> keysOnShard(JedisShardInfo shardInfo,
                                                String pattern) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.keys(pattern);
    }

    /**
     * 在指定shard上执行keys
     * 
     * @param shardInfo shard
     * @param pattern pattern
     * @return response
     */
    protected Response<Set<byte[]>> keysOnShard(JedisShardInfo shardInfo,
                                                byte[] pattern) {
        SmartPipeline pipeline = getSmartPipeline(shardInfo);
        pipelines.add(pipeline);
        return pipeline.keys(pattern);
    }

    @Override
    public Response<String> get(String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.get(key);
    }

    @Override
    public Response<String> set(final String key,
                                final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.set(key, value);
    }

    // override PipelineBase
    @Override
    public Response<Long> append(final String key,
                                 final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.append(key, value);
    }

    @Override
    public Response<Long> append(final byte[] key,
                                 final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.append(key, value);
    }

    @Override
    public Response<List<String>> blpop(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.blpop(key);
    }

    @Override
    public Response<List<String>> brpop(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.brpop(key);
    }

    @Override
    public Response<List<byte[]>> blpop(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.blpop(key);
    }

    @Override
    public Response<List<byte[]>> brpop(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.brpop(key);
    }

    @Override
    public Response<Long> decr(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.decr(key);
    }

    @Override
    public Response<Long> decr(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.decr(key);
    }

    @Override
    public Response<Long> decrBy(final String key,
                                 final long integer) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.decrBy(key, integer);
    }

    @Override
    public Response<Long> decrBy(final byte[] key,
                                 final long integer) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.decrBy(key, integer);
    }

    @Override
    public Response<Long> del(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.del(key);
    }

    @Override
    public Response<Long> del(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.del(key);
    }

    @Override
    public Response<String> echo(final String string) {
        SmartPipeline pipeline = getSmartPipeline(string);
        pipelines.add(pipeline);
        return pipeline.echo(string);
    }

    @Override
    public Response<byte[]> echo(final byte[] string) {
        SmartPipeline pipeline = getSmartPipeline(string);
        pipelines.add(pipeline);
        return pipeline.echo(string);
    }

    @Override
    public Response<Boolean> exists(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.exists(key);
    }

    @Override
    public Response<Boolean> exists(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.exists(key);
    }

    @Override
    public Response<Long> expire(final String key,
                                 final int seconds) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.expire(key, seconds);
    }

    @Override
    public Response<Long> expire(final byte[] key,
                                 final int seconds) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.expire(key, seconds);
    }

    @Override
    public Response<Long> expireAt(final String key,
                                   final long unixTime) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.expireAt(key, unixTime);
    }

    @Override
    public Response<Long> expireAt(final byte[] key,
                                   final long unixTime) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.expireAt(key, unixTime);
    }

    @Override
    public Response<byte[]> get(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.get(key);
    }

    @Override
    public Response<Boolean> getbit(final String key,
                                    final long offset) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.getbit(key, offset);
    }

    @Override
    public Response<Boolean> getbit(final byte[] key,
                                    final long offset) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.getbit(key, offset);
    }

    @Override
    public Response<String> getrange(final String key,
                                     final long startOffset,
                                     final long endOffset) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.getrange(key, startOffset, endOffset);
    }

    @Override
    public Response<String> getSet(final String key,
                                   final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.getSet(key, value);
    }

    @Override
    public Response<byte[]> getSet(final byte[] key,
                                   final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.getSet(key, value);
    }

    @Override
    public Response<Long> getrange(final byte[] key,
                                   final long startOffset,
                                   final long endOffset) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.getrange(key, startOffset, endOffset);
    }

    @Override
    public Response<Long> hdel(final String key,
                               final String... field) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hdel(key, field);
    }

    @Override
    public Response<Long> hdel(final byte[] key,
                               final byte[]... field) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hdel(key, field);
    }

    @Override
    public Response<Boolean> hexists(final String key,
                                     final String field) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hexists(key, field);
    }

    @Override
    public Response<Boolean> hexists(final byte[] key,
                                     final byte[] field) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hexists(key, field);
    }

    @Override
    public Response<String> hget(final String key,
                                 final String field) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hget(key, field);
    }

    @Override
    public Response<byte[]> hget(final byte[] key,
                                 final byte[] field) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hget(key, field);
    }

    @Override
    public Response<Map<String, String>> hgetAll(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hgetAll(key);
    }

    @Override
    public Response<Map<byte[], byte[]>> hgetAll(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hgetAll(key);
    }

    @Override
    public Response<Long> hincrBy(final String key,
                                  final String field,
                                  final long value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hincrBy(key, field, value);
    }

    @Override
    public Response<Long> hincrBy(final byte[] key,
                                  final byte[] field,
                                  final long value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hincrBy(key, field, value);
    }

    @Override
    public Response<Set<String>> hkeys(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hkeys(key);
    }

    @Override
    public Response<Set<byte[]>> hkeys(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hkeys(key);
    }

    @Override
    public Response<Long> hlen(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hlen(key);
    }

    @Override
    public Response<Long> hlen(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hlen(key);
    }

    @Override
    public Response<List<String>> hmget(final String key,
                                        final String... fields) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hmget(key, fields);
    }

    @Override
    public Response<List<byte[]>> hmget(final byte[] key,
                                        final byte[]... fields) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hmget(key, fields);
    }

    @Override
    public Response<String> hmset(final String key,
                                  final Map<String, String> hash) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hmset(key, hash);
    }

    @Override
    public Response<String> hmset(final byte[] key,
                                  final Map<byte[], byte[]> hash) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hmset(key, hash);
    }

    @Override
    public Response<Long> hset(final String key,
                               final String field,
                               final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hset(key, field, value);
    }

    @Override
    public Response<Long> hset(final byte[] key,
                               final byte[] field,
                               final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hset(key, field, value);
    }

    @Override
    public Response<Long> hsetnx(final String key,
                                 final String field,
                                 final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hsetnx(key, field, value);
    }

    @Override
    public Response<Long> hsetnx(final byte[] key,
                                 final byte[] field,
                                 final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hsetnx(key, field, value);
    }

    @Override
    public Response<List<String>> hvals(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hvals(key);
    }

    @Override
    public Response<List<byte[]>> hvals(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hvals(key);
    }

    @Override
    public Response<Long> incr(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.incr(key);
    }

    @Override
    public Response<Long> incr(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.incr(key);
    }

    @Override
    public Response<Long> incrBy(final String key,
                                 final long integer) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.incrBy(key, integer);
    }

    @Override
    public Response<Long> incrBy(final byte[] key,
                                 final long integer) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.incrBy(key, integer);
    }

    @Override
    public Response<String> lindex(final String key,
                                   final long index) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lindex(key, index);
    }

    @Override
    public Response<byte[]> lindex(final byte[] key,
                                   final long index) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lindex(key, index);
    }

    @Override
    public Response<Long> linsert(final String key,
                                  final LIST_POSITION where,
                                  final String pivot,
                                  final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.linsert(key, where, pivot, value);
    }

    @Override
    public Response<Long> linsert(final byte[] key,
                                  final LIST_POSITION where,
                                  final byte[] pivot,
                                  final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.linsert(key, where, pivot, value);
    }

    @Override
    public Response<Long> llen(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.llen(key);
    }

    @Override
    public Response<Long> llen(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.llen(key);
    }

    @Override
    public Response<String> lpop(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lpop(key);
    }

    @Override
    public Response<byte[]> lpop(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lpop(key);
    }

    @Override
    public Response<Long> lpush(final String key,
                                final String... string) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lpush(key, string);
    }

    @Override
    public Response<Long> lpush(final byte[] key,
                                final byte[]... string) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lpush(key, string);
    }

    @Override
    public Response<Long> lpushx(final String key,
                                 final String... string) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lpushx(key, string);
    }

    @Override
    public Response<Long> lpushx(final byte[] key,
                                 final byte[]... bytes) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lpushx(key, bytes);
    }

    @Override
    public Response<List<String>> lrange(final String key,
                                         final long start,
                                         final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lrange(key, start, end);
    }

    @Override
    public Response<List<byte[]>> lrange(final byte[] key,
                                         final long start,
                                         final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lrange(key, start, end);
    }

    @Override
    public Response<Long> lrem(final String key,
                               final long count,
                               final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lrem(key, count, value);
    }

    @Override
    public Response<Long> lrem(final byte[] key,
                               final long count,
                               final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lrem(key, count, value);
    }

    @Override
    public Response<String> lset(final String key,
                                 final long index,
                                 final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lset(key, index, value);
    }

    @Override
    public Response<String> lset(final byte[] key,
                                 final long index,
                                 final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.lset(key, index, value);
    }

    @Override
    public Response<String> ltrim(final String key,
                                  final long start,
                                  final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.ltrim(key, start, end);
    }

    @Override
    public Response<String> ltrim(final byte[] key,
                                  final long start,
                                  final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.ltrim(key, start, end);
    }

    @Override
    public Response<Long> move(final String key,
                               final int dbIndex) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.move(key, dbIndex);
    }

    @Override
    public Response<Long> move(final byte[] key,
                               final int dbIndex) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.move(key, dbIndex);
    }

    @Override
    public Response<Long> persist(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.persist(key);
    }

    @Override
    public Response<Long> persist(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.persist(key);
    }

    @Override
    public Response<String> rpop(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.rpop(key);
    }

    @Override
    public Response<byte[]> rpop(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.rpop(key);
    }

    @Override
    public Response<Long> rpush(final String key,
                                final String... string) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.rpush(key, string);
    }

    @Override
    public Response<Long> rpush(final byte[] key,
                                final byte[]... string) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.rpush(key, string);
    }

    @Override
    public Response<Long> rpushx(final String key,
                                 final String... string) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.rpushx(key, string);
    }

    @Override
    public Response<Long> rpushx(final byte[] key,
                                 final byte[]... string) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.rpushx(key, string);
    }

    @Override
    public Response<Long> sadd(final String key,
                               final String... member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.sadd(key, member);
    }

    @Override
    public Response<Long> sadd(final byte[] key,
                               final byte[]... member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.sadd(key, member);
    }

    @Override
    public Response<Long> scard(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.scard(key);
    }

    @Override
    public Response<Long> scard(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.scard(key);
    }

    @Override
    public Response<String> set(final byte[] key,
                                final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.set(key, value);
    }

    @Override
    public Response<Boolean> setbit(final String key,
                                    final long offset,
                                    final boolean value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.setbit(key, offset, value);
    }

    @Override
    public Response<Boolean> setbit(final byte[] key,
                                    final long offset,
                                    final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.setbit(key, offset, value);
    }

    @Override
    public Response<String> setex(final String key,
                                  final int seconds,
                                  final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.setex(key, seconds, value);
    }

    @Override
    public Response<String> setex(final byte[] key,
                                  final int seconds,
                                  final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.setex(key, seconds, value);
    }

    @Override
    public Response<Long> setnx(final String key,
                                final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.setnx(key, value);
    }

    @Override
    public Response<Long> setnx(final byte[] key,
                                final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.setnx(key, value);
    }

    @Override
    public Response<Long> setrange(final String key,
                                   final long offset,
                                   final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.setrange(key, offset, value);
    }

    @Override
    public Response<Long> setrange(final byte[] key,
                                   final long offset,
                                   final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.setrange(key, offset, value);
    }

    @Override
    public Response<Boolean> sismember(final String key,
                                       final String member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.sismember(key, member);
    }

    @Override
    public Response<Boolean> sismember(final byte[] key,
                                       final byte[] member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.sismember(key, member);
    }

    @Override
    public Response<Set<String>> smembers(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.smembers(key);
    }

    @Override
    public Response<Set<byte[]>> smembers(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.smembers(key);
    }

    @Override
    public Response<List<String>> sort(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.sort(key);
    }

    @Override
    public Response<List<byte[]>> sort(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.sort(key);
    }

    @Override
    public Response<List<String>> sort(final String key,
                                       final SortingParams sortingParameters) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.sort(key, sortingParameters);
    }

    @Override
    public Response<List<byte[]>> sort(final byte[] key,
                                       final SortingParams sortingParameters) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.sort(key, sortingParameters);
    }

    @Override
    public Response<String> spop(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.spop(key);
    }

    @Override
    public Response<byte[]> spop(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.spop(key);
    }

    @Override
    public Response<String> srandmember(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.srandmember(key);
    }

    @Override
    public Response<List<String>> srandmember(final String key,
                                              final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.srandmember(key, count);
    }

    @Override
    public Response<byte[]> srandmember(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.srandmember(key);
    }

    @Override
    public Response<List<byte[]>> srandmember(final byte[] key,
                                              final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.srandmember(key, count);
    }

    @Override
    public Response<Long> srem(final String key,
                               final String... member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.srem(key, member);
    }

    @Override
    public Response<Long> srem(final byte[] key,
                               final byte[]... member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.srem(key, member);
    }

    @Override
    public Response<Long> strlen(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.strlen(key);
    }

    @Override
    public Response<Long> strlen(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.strlen(key);
    }

    @Override
    public Response<String> substr(final String key,
                                   final int start,
                                   final int end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.substr(key, start, end);
    }

    @Override
    public Response<String> substr(final byte[] key,
                                   final int start,
                                   final int end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.substr(key, start, end);
    }

    @Override
    public Response<Long> ttl(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.ttl(key);
    }

    @Override
    public Response<Long> ttl(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.ttl(key);
    }

    @Override
    public Response<String> type(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.type(key);
    }

    @Override
    public Response<String> type(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.type(key);
    }

    @Override
    public Response<Long> zadd(final String key,
                               final double score,
                               final String member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zadd(key, score, member);
    }

    @Override
    public Response<Long> zadd(final String key,
                               final Map<String, Double> scoreMembers) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zadd(key, scoreMembers);
    }

    @Override
    public Response<Long> zadd(final byte[] key,
                               final double score,
                               final byte[] member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zadd(key, score, member);
    }

    @Override
    public Response<Long> zcard(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zcard(key);
    }

    @Override
    public Response<Long> zcard(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zcard(key);
    }

    @Override
    public Response<Long> zcount(final String key,
                                 final double min,
                                 final double max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zcount(key, min, max);
    }

    @Override
    public Response<Long> zcount(final String key,
                                 final String min,
                                 final String max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zcount(key, min, max);
    }

    @Override
    public Response<Long> zcount(final byte[] key,
                                 final double min,
                                 final double max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zcount(key, min, max);
    }

    @Override
    public Response<Double> zincrby(final String key,
                                    final double score,
                                    final String member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zincrby(key, score, member);
    }

    @Override
    public Response<Double> zincrby(final byte[] key,
                                    final double score,
                                    final byte[] member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zincrby(key, score, member);
    }

    @Override
    public Response<Set<String>> zrange(final String key,
                                        final long start,
                                        final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrange(key, start, end);
    }

    @Override
    public Response<Set<byte[]>> zrange(final byte[] key,
                                        final long start,
                                        final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrange(key, start, end);
    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key,
                                               final double min,
                                               final double max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScore(key, min, max);
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key,
                                               final double min,
                                               final double max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScore(key, min, max);
    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key,
                                               final String min,
                                               final String max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScore(key, min, max);
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key,
                                               final byte[] min,
                                               final byte[] max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScore(key, min, max);
    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key,
                                               final double min,
                                               final double max,
                                               final int offset,
                                               final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key,
                                               final String min,
                                               final String max,
                                               final int offset,
                                               final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key,
                                               final double min,
                                               final double max,
                                               final int offset,
                                               final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key,
                                               final byte[] min,
                                               final byte[] max,
                                               final int offset,
                                               final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key,
                                                        final double min,
                                                        final double max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key,
                                                        final String min,
                                                        final String max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key,
                                                        final double min,
                                                        final double max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key,
                                                        final byte[] min,
                                                        final byte[] max) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key,
                                                        final double min,
                                                        final double max,
                                                        final int offset,
                                                        final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key,
                                                        final String min,
                                                        final String max,
                                                        final int offset,
                                                        final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key,
                                                        final double min,
                                                        final double max,
                                                        final int offset,
                                                        final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key,
                                                        final byte[] min,
                                                        final byte[] max,
                                                        final int offset,
                                                        final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key,
                                                  final double max,
                                                  final double min) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScore(key, max, min);
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key,
                                                  final double max,
                                                  final double min) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScore(key, max, min);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key,
                                                  final String max,
                                                  final String min) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScore(key, max, min);
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key,
                                                  final byte[] max,
                                                  final byte[] min) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScore(key, max, min);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key,
                                                  final double max,
                                                  final double min,
                                                  final int offset,
                                                  final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key,
                                                  final String max,
                                                  final String min,
                                                  final int offset,
                                                  final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key,
                                                  final double max,
                                                  final double min,
                                                  final int offset,
                                                  final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key,
                                                  final byte[] max,
                                                  final byte[] min,
                                                  final int offset,
                                                  final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key,
                                                           final double max,
                                                           final double min) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key,
                                                           final String max,
                                                           final String min) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key,
                                                           final double max,
                                                           final double min) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key,
                                                           final byte[] max,
                                                           final byte[] min) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key,
                                                           final double max,
                                                           final double min,
                                                           final int offset,
                                                           final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key,
                                                           final String max,
                                                           final String min,
                                                           final int offset,
                                                           final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key,
                                                           final double max,
                                                           final double min,
                                                           final int offset,
                                                           final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key,
                                                           final byte[] max,
                                                           final byte[] min,
                                                           final int offset,
                                                           final int count) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrangeWithScores(final String key,
                                                 final long start,
                                                 final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeWithScores(key, start, end);
    }

    @Override
    public Response<Set<Tuple>> zrangeWithScores(final byte[] key,
                                                 final long start,
                                                 final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrangeWithScores(key, start, end);
    }

    @Override
    public Response<Long> zrank(final String key,
                                final String member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrank(key, member);
    }

    @Override
    public Response<Long> zrank(final byte[] key,
                                final byte[] member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrank(key, member);
    }

    @Override
    public Response<Long> zrem(final String key,
                               final String... member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrem(key, member);
    }

    @Override
    public Response<Long> zrem(final byte[] key,
                               final byte[]... member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrem(key, member);
    }

    @Override
    public Response<Long> zremrangeByRank(final String key,
                                          final long start,
                                          final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zremrangeByRank(key, start, end);
    }

    @Override
    public Response<Long> zremrangeByRank(final byte[] key,
                                          final long start,
                                          final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zremrangeByRank(key, start, end);
    }

    @Override
    public Response<Long> zremrangeByScore(final String key,
                                           final double start,
                                           final double end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zremrangeByScore(key, start, end);
    }

    @Override
    public Response<Long> zremrangeByScore(final String key,
                                           final String start,
                                           final String end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zremrangeByScore(key, start, end);
    }

    @Override
    public Response<Long> zremrangeByScore(final byte[] key,
                                           final double start,
                                           final double end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zremrangeByScore(key, start, end);
    }

    @Override
    public Response<Long> zremrangeByScore(final byte[] key,
                                           final byte[] start,
                                           final byte[] end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zremrangeByScore(key, start, end);
    }

    @Override
    public Response<Set<String>> zrevrange(final String key,
                                           final long start,
                                           final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrange(key, start, end);
    }

    @Override
    public Response<Set<byte[]>> zrevrange(final byte[] key,
                                           final long start,
                                           final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrange(key, start, end);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeWithScores(final String key,
                                                    final long start,
                                                    final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeWithScores(key, start, end);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeWithScores(final byte[] key,
                                                    final long start,
                                                    final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrangeWithScores(key, start, end);
    }

    @Override
    public Response<Long> zrevrank(final String key,
                                   final String member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrank(key, member);
    }

    @Override
    public Response<Long> zrevrank(final byte[] key,
                                   final byte[] member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zrevrank(key, member);
    }

    @Override
    public Response<Double> zscore(final String key,
                                   final String member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zscore(key, member);
    }

    @Override
    public Response<Double> zscore(final byte[] key,
                                   final byte[] member) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.zscore(key, member);
    }

    @Override
    public Response<Long> bitcount(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.bitcount(key);
    }

    @Override
    public Response<Long> bitcount(final String key,
                                   final long start,
                                   final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.bitcount(key, start, end);
    }

    @Override
    public Response<Long> bitcount(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.bitcount(key);
    }

    @Override
    public Response<Long> bitcount(final byte[] key,
                                   final long start,
                                   final long end) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.bitcount(key, start, end);
    }

    @Override
    public Response<byte[]> dump(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.dump(key);
    }

    @Override
    public Response<byte[]> dump(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.dump(key);
    }

    @Override
    public Response<String> migrate(final String host,
                                    final int port,
                                    final String key,
                                    final int destinationDb,
                                    final int timeout) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.migrate(host, port, key, destinationDb, timeout);
    }

    @Override
    public Response<String> migrate(final byte[] host,
                                    final int port,
                                    final byte[] key,
                                    final int destinationDb,
                                    final int timeout) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.migrate(host, port, key, destinationDb, timeout);
    }

    @Override
    public Response<Long> objectRefcount(String key) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<Long> objectRefcount(byte[] key) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> objectEncoding(String key) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<byte[]> objectEncoding(byte[] key) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<Long> objectIdletime(String key) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<Long> objectIdletime(byte[] key) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<Long> pexpire(final String key,
                                  final int milliseconds) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.pexpire(key, milliseconds);
    }

    @Override
    public Response<Long> pexpire(final byte[] key,
                                  final int milliseconds) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.pexpire(key, milliseconds);
    }

    @Override
    public Response<Long> pexpireAt(final String key,
                                    final long millisecondsTimestamp) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.pexpireAt(key, millisecondsTimestamp);
    }

    @Override
    public Response<Long> pexpireAt(final byte[] key,
                                    final long millisecondsTimestamp) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.pexpireAt(key, millisecondsTimestamp);
    }

    @Override
    public Response<Long> pttl(final String key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.pttl(key);
    }

    @Override
    public Response<Long> pttl(final byte[] key) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.pttl(key);
    }

    @Override
    public Response<String> restore(final String key,
                                    final int ttl,
                                    final byte[] serializedValue) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.restore(key, ttl, serializedValue);
    }

    @Override
    public Response<String> restore(final byte[] key,
                                    final int ttl,
                                    final byte[] serializedValue) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.restore(key, ttl, serializedValue);
    }

    @Override
    public Response<Double> incrByFloat(final String key,
                                        final double increment) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.incrByFloat(key, increment);
    }

    @Override
    public Response<Double> incrByFloat(final byte[] key,
                                        final double increment) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.incrByFloat(key, increment);
    }

    @Override
    public Response<String> psetex(final String key,
                                   final int milliseconds,
                                   final String value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.psetex(key, milliseconds, value);
    }

    @Override
    public Response<String> psetex(final byte[] key,
                                   final int milliseconds,
                                   final byte[] value) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.psetex(key, milliseconds, value);
    }

    @Override
    public Response<String> set(final String key,
                                final String value,
                                final String nxxx) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.set(key, value, nxxx);
    }

    @Override
    public Response<String> set(final byte[] key,
                                final byte[] value,
                                final byte[] nxxx) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.set(key, value, nxxx);
    }

    @Override
    public Response<String> set(final String key,
                                final String value,
                                final String nxxx,
                                final String expx,
                                final int time) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.set(key, value, nxxx, expx, time);
    }

    @Override
    public Response<String> set(final byte[] key,
                                final byte[] value,
                                final byte[] nxxx,
                                final byte[] expx,
                                final int time) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.set(key, value, nxxx, expx, time);
    }

    @Override
    public Response<Double> hincrByFloat(final String key,
                                         final String field,
                                         final double increment) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hincrByFloat(key, field, increment);
    }

    @Override
    public Response<Double> hincrByFloat(final byte[] key,
                                         final byte[] field,
                                         final double increment) {
        SmartPipeline pipeline = getSmartPipeline(key);
        pipelines.add(pipeline);
        return pipeline.hincrByFloat(key, field, increment);
    }

    @Override
    public Response<String> eval(String script) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> eval(String script,
                                 List<String> keys,
                                 List<String> args) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> eval(String script,
                                 int numKeys,
                                 String[] argv) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> evalsha(String script) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> evalsha(String sha1,
                                    List<String> keys,
                                    List<String> args) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> evalsha(String sha1,
                                    int numKeys,
                                    String[] argv) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }
}
