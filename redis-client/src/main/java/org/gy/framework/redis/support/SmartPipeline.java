package org.gy.framework.redis.support;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.gy.framework.redis.support.SmartJedis.RW;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Builder;
import redis.clients.jedis.Client;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

public class SmartPipeline extends Pipeline {

    private static final Logger                       logger    = LoggerFactory.getLogger(SmartPipeline.class);

    private static Method                             CLIENT_FLUSH_METHOD;

    static {
        try {
            CLIENT_FLUSH_METHOD = ReflectionUtils.findMethod(Client.class, "flush");
            ReflectionUtils.makeAccessible(CLIENT_FLUSH_METHOD);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private SmartJedis                                smartJedis;

    // JedisResource及其生成的pipeline的对应关系
    private Map<SmartJedis.JedisResource, MyPipeline> map       = new HashMap<SmartJedis.JedisResource, MyPipeline>();

    // 按照调用顺序记录，每次pipeline调用，都会记录是在哪个MyPipeline上调用
    private Queue<MyPipeline>                         pipelines = new LinkedList<MyPipeline>();

    public SmartPipeline(SmartJedis smartJedis) {
        this.smartJedis = smartJedis;
    }

    /**
     * Make protected field and method public.
     */
    protected class MyPipeline extends Pipeline {

        private String jedisPool;

        public MyPipeline(String jedisPool) {
            this.jedisPool = jedisPool;
        }

        public Response<?> generateResponse(Object data) {
            return super.generateResponse(data);
        }

        public Client getClient() {
            return client;
        }

        public String toString() {
            return "pipeline of " + jedisPool;
        }
    }

    protected static abstract class Action<T> {

        Class<T> type;

        public Action() {
            this.type = (Class<T>) getClass();
        }

        public abstract T doAction(Pipeline pipeline);

    }

    /**
     * 根据读写、键决定，本次command是在master还是slave的pipeline上执行
     * 
     * @param action action
     * @param rw 读写
     * @param keys 键
     * @param <T> 返回类型
     * @return 执行结果
     */
    protected <T> T execute(Action<T> action,
                            SmartJedis.RW rw,
                            String... keys) {
        MyPipeline pipeline = getPipeline(rw, keys);
        pipelines.add(pipeline);
        try {
            T result = action.doAction(pipeline);
            if (rw.equals(SmartJedis.RW.W)) {
                smartJedis.keyHasBeenWritten(keys);
            }
            return result;
        } catch (JedisException ex) {
            logger.error("Jedis exception occur when execute on " + pipeline, ex);
            throw ex;
        }
    }

    /**
     * 在master的pipeline上执行
     * 
     * @param action action
     * @param <T> 返回类型
     * @return 执行结果
     */
    protected <T> T executeOnMaster(Action<T> action) {
        MyPipeline pipeline = getPipeline(SmartJedis.RW.W);
        pipelines.add(pipeline);
        try {
            return action.doAction(pipeline);
        } catch (JedisException ex) {
            logger.error("Jedis exception occur when execute on " + pipeline, ex);
            throw ex;
        }
    }

    protected MyPipeline getPipeline(SmartJedis.RW rw,
                                     String... key) {
        SmartJedis.JedisResource jedisResource = smartJedis.chooseJedisResource(rw, key);
        MyPipeline pipeline = map.get(jedisResource);
        if (pipeline == null) {
            pipeline = new MyPipeline(jedisResource.jedisPool.toString());
            pipeline.setClient(jedisResource.jedis.getClient());
            map.put(jedisResource, pipeline);
        }
        return pipeline;
    }

    /**
     * flush所有JedisResource的socket buffer
     */
    protected void flush() {
        // flush all client socket buffer.
        for (SmartJedis.JedisResource resource : map.keySet()) {
            try {
                Client client = resource.jedis.getClient();
                ReflectionUtils.invokeMethod(CLIENT_FLUSH_METHOD, client);
            } catch (JedisException ex) {
                logger.error("Jedis exception occur when execute on client of " + resource.jedisPool, ex);
                throw ex;
            }
        }
    }

    protected List<Object> getResults() {
        // generate response according execute order.
        List<Object> results = new ArrayList<Object>();
        for (MyPipeline pipeline : pipelines) {
            Object object;
            try {
                object = pipeline.getClient().getOne();
            } catch (JedisDataException e) {
                object = e;
            }
            results.add(object);
        }
        return results;
    }

    /**
     * 根据执行顺序(pipelines)，依次从各MyPipeline对应的Client socket读取响应
     * 
     * @return 响应列表
     */
    protected List<Response<?>> generateResponses() {
        // generate response according execute order.
        List<Response<?>> responses = new ArrayList<Response<?>>();
        for (MyPipeline pipeline : pipelines) {
            Object object;
            try {
                object = pipeline.getClient().getOne();
            } catch (JedisDataException e) {
                logger.error("JedisDataException occur on " + pipeline, e);
                object = e;
            } catch (JedisException ex) {
                logger.error("JedisException occur on " + pipeline, ex);
                throw ex;
            }
            responses.add(pipeline.generateResponse(object));
        }
        return responses;
    }

    @Override
    public void sync() {
        flush();
        generateResponses();
    }

    @Override
    public List<Object> syncAndReturnAll() {
        flush();
        List<Response<?>> responses = generateResponses();
        List<Object> formatted = new ArrayList<Object>();
        for (Response<?> response : responses) {
            try {
                formatted.add(response.get());
            } catch (JedisDataException e) {
                formatted.add(e);
            }
        }
        return formatted;
    }

    @Override
    protected Response<?> generateResponse(Object data) {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    protected void clean() {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    protected <T> Response<T> getResponse(Builder<T> builder) {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    public void setClient(Client client) {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    protected Client getClient(byte[] key) {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    protected Client getClient(String key) {
        throw new IllegalStateException("This method should not be invoked.");
    }

    @Override
    public Response<String> discard() {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<List<Object>> exec() {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> multi() {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> get(final String key) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.get(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<String> set(final String key,
                                final String value) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.set(key, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<List<String>> mget(final String... keys) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.mget(keys);
            }
        }, SmartJedis.RW.R, keys);
    }

    @Override
    public Response<String> mset(final String... keysvalues) {
        String[] keys = null;
        if (keysvalues != null && keysvalues.length > 0) {
            int keyCount = keysvalues.length / 2;
            keys = new String[keyCount];
            for (int i = 0; i < keyCount; i++) {
                keys[i] = keysvalues[2 * i];
            }
        }
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.mset(keysvalues);
            }
        }, SmartJedis.RW.W, keys);
    }

    // override MultiKeyPipelineBase
    @Override
    public Response<List<String>> brpop(final String... args) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.brpop(args);
            }
        }, SmartJedis.RW.W, args);
    }

    @Override
    public Response<List<String>> brpop(final int timeout,
                                        final String... keys) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.brpop(timeout, keys);
            }
        }, SmartJedis.RW.W, keys);
    }

    @Override
    public Response<List<String>> blpop(final String... args) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.blpop(args);
            }
        }, SmartJedis.RW.W, args);
    }

    @Override
    public Response<List<String>> blpop(final int timeout,
                                        final String... keys) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.blpop(timeout, keys);
            }
        }, SmartJedis.RW.W, keys);
    }

    @Override
    public Response<Map<String, String>> blpopMap(final int timeout,
                                                  final String... keys) {
        return execute(new Action<Response<Map<String, String>>>() {
            public Response<Map<String, String>> doAction(Pipeline pipeline) {
                return pipeline.blpopMap(timeout, keys);
            }
        }, SmartJedis.RW.W, keys);
    }

    @Override
    public Response<List<byte[]>> brpop(final byte[]... args) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.brpop(args);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<List<String>> brpop(final int timeout,
                                        final byte[]... keys) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.brpop(timeout, keys);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Map<String, String>> brpopMap(final int timeout,
                                                  final String... keys) {
        return execute(new Action<Response<Map<String, String>>>() {
            public Response<Map<String, String>> doAction(Pipeline pipeline) {
                return pipeline.brpopMap(timeout, keys);
            }
        }, SmartJedis.RW.W, keys);
    }

    @Override
    public Response<List<byte[]>> blpop(final byte[]... args) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.blpop(args);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<List<String>> blpop(final int timeout,
                                        final byte[]... keys) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.blpop(timeout, keys);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> del(final String... keys) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.del(keys);
            }
        }, SmartJedis.RW.W, keys);
    }

    @Override
    public Response<Long> del(final byte[]... keys) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.del(keys);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Set<String>> keys(final String pattern) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.keys(pattern);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<byte[]>> keys(final byte[] pattern) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.keys(pattern);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<List<byte[]>> mget(final byte[]... keys) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.mget(keys);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<String> mset(final byte[]... keysvalues) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.mset(keysvalues);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> msetnx(final String... keysvalues) {
        String[] keys = null;
        if (keysvalues != null && keysvalues.length > 0) {
            int keyCount = keysvalues.length / 2;
            keys = new String[keyCount];
            for (int i = 0; i < keyCount; i++) {
                keys[i] = keysvalues[2 * i];
            }
        }
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.msetnx(keysvalues);
            }
        }, SmartJedis.RW.W, keys);
    }

    @Override
    public Response<Long> msetnx(final byte[]... keysvalues) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.msetnx(keysvalues);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> rename(final String oldkey,
                                   final String newkey) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.rename(oldkey, newkey);
            }
        }, SmartJedis.RW.W, oldkey, newkey);
    }

    @Override
    public Response<String> rename(final byte[] oldkey,
                                   final byte[] newkey) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.rename(oldkey, newkey);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> renamenx(final String oldkey,
                                   final String newkey) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.renamenx(oldkey, newkey);
            }
        }, SmartJedis.RW.W, oldkey, newkey);
    }

    @Override
    public Response<Long> renamenx(final byte[] oldkey,
                                   final byte[] newkey) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.renamenx(oldkey, newkey);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> rpoplpush(final String srckey,
                                      final String dstkey) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.rpoplpush(srckey, dstkey);
            }
        }, SmartJedis.RW.W, srckey, dstkey);
    }

    @Override
    public Response<byte[]> rpoplpush(final byte[] srckey,
                                      final byte[] dstkey) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.rpoplpush(srckey, dstkey);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Set<String>> sdiff(final String... keys) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.sdiff(keys);
            }
        }, SmartJedis.RW.R, keys);
    }

    @Override
    public Response<Set<byte[]>> sdiff(final byte[]... keys) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.sdiff(keys);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> sdiffstore(final String dstkey,
                                     final String... keys) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sdiffstore(dstkey, keys);
            }
        }, SmartJedis.RW.W, dstkey);
    }

    @Override
    public Response<Long> sdiffstore(final byte[] dstkey,
                                     final byte[]... keys) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sdiffstore(dstkey, keys);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Set<String>> sinter(final String... keys) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.sinter(keys);
            }
        }, SmartJedis.RW.R, keys);
    }

    @Override
    public Response<Set<byte[]>> sinter(final byte[]... keys) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.sinter(keys);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> sinterstore(final String dstkey,
                                      final String... keys) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sinterstore(dstkey, keys);
            }
        }, SmartJedis.RW.W, dstkey);
    }

    @Override
    public Response<Long> sinterstore(final byte[] dstkey,
                                      final byte[]... keys) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sinterstore(dstkey, keys);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> smove(final String srckey,
                                final String dstkey,
                                final String member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.smove(srckey, dstkey, member);
            }
        }, SmartJedis.RW.W, srckey, dstkey);
    }

    @Override
    public Response<Long> smove(final byte[] srckey,
                                final byte[] dstkey,
                                final byte[] member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.smove(srckey, dstkey, member);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> sort(final String key,
                               final SortingParams sortingParameters,
                               final String dstkey) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sort(key, sortingParameters, dstkey);
            }
        }, SmartJedis.RW.W, dstkey);
    }

    @Override
    public Response<Long> sort(final byte[] key,
                               final SortingParams sortingParameters,
                               final byte[] dstkey) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sort(key, sortingParameters, dstkey);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> sort(final String key,
                               final String dstkey) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sort(key, dstkey);
            }
        }, SmartJedis.RW.W, dstkey);
    }

    @Override
    public Response<Long> sort(final byte[] key,
                               final byte[] dstkey) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sort(key, dstkey);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Set<String>> sunion(final String... keys) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.sunion(keys);
            }
        }, SmartJedis.RW.R, keys);
    }

    @Override
    public Response<Set<byte[]>> sunion(final byte[]... keys) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.sunion(keys);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> sunionstore(final String dstkey,
                                      final String... keys) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sunionstore(dstkey, keys);
            }
        }, SmartJedis.RW.W, dstkey);
    }

    @Override
    public Response<Long> sunionstore(final byte[] dstkey,
                                      final byte[]... keys) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sunionstore(dstkey, keys);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> watch(final String... keys) {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.watch(keys);
            }
        });
    }

    @Override
    public Response<String> watch(final byte[]... keys) {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.watch(keys);
            }
        });
    }

    @Override
    public Response<Long> zinterstore(final String dstkey,
                                      final String... sets) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zinterstore(dstkey);
            }
        }, SmartJedis.RW.W, dstkey);
    }

    @Override
    public Response<Long> zinterstore(final byte[] dstkey,
                                      final byte[]... sets) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zinterstore(dstkey);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> zinterstore(final String dstkey,
                                      final ZParams params,
                                      final String... sets) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zinterstore(dstkey, params, sets);
            }
        }, SmartJedis.RW.W, dstkey);
    }

    @Override
    public Response<Long> zinterstore(final byte[] dstkey,
                                      final ZParams params,
                                      final byte[]... sets) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zinterstore(dstkey, params, sets);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> zunionstore(final String dstkey,
                                      final String... sets) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zunionstore(dstkey, sets);
            }
        }, SmartJedis.RW.W, dstkey);
    }

    @Override
    public Response<Long> zunionstore(final byte[] dstkey,
                                      final byte[]... sets) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zunionstore(dstkey, sets);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> zunionstore(final String dstkey,
                                      final ZParams params,
                                      final String... sets) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zunionstore(dstkey, params, sets);
            }
        }, SmartJedis.RW.W, dstkey);
    }

    @Override
    public Response<Long> zunionstore(final byte[] dstkey,
                                      final ZParams params,
                                      final byte[]... sets) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zunionstore(dstkey, params, sets);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> bgrewriteaof() {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.bgrewriteaof();
            }
        });
    }

    @Override
    public Response<String> bgsave() {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.bgsave();
            }
        });
    }

    @Override
    public Response<List<String>> configGet(String pattern) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> configSet(String parameter,
                                      String value) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> brpoplpush(final String source,
                                       final String destination,
                                       final int timeout) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.brpoplpush(source, destination, timeout);
            }
        }, SmartJedis.RW.W, source, destination);
    }

    @Override
    public Response<byte[]> brpoplpush(final byte[] source,
                                       final byte[] destination,
                                       final int timeout) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.brpoplpush(source, destination, timeout);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> configResetStat() {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> save() {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.save();
            }
        });
    }

    @Override
    public Response<Long> lastsave() {
        return executeOnMaster(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.lastsave();
            }
        });
    }

    @Override
    public Response<Long> publish(final String channel,
                                  final String message) {
        return executeOnMaster(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.publish(channel, message);
            }
        });
    }

    @Override
    public Response<Long> publish(final byte[] channel,
                                  final byte[] message) {
        return executeOnMaster(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.publish(channel, message);
            }
        });
    }

    @Override
    public Response<String> randomKey() {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.randomKey();
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<byte[]> randomKeyBinary() {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.randomKeyBinary();
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<String> flushDB() {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.flushDB();
            }
        });
    }

    @Override
    public Response<String> flushAll() {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.flushAll();
            }
        });
    }

    @Override
    public Response<String> info() {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.info();
            }
        });
    }

    @Override
    public Response<Long> dbSize() {
        return executeOnMaster(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.dbSize();
            }
        });
    }

    @Override
    public Response<String> shutdown() {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.shutdown();
            }
        });
    }

    @Override
    public Response<String> ping() {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.ping();
            }
        });
    }

    @Override
    public Response<String> select(final int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Long> bitop(final BitOP op,
                                final byte[] destKey,
                                final byte[]... srcKeys) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.bitop(op, destKey, srcKeys);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> bitop(final BitOP op,
                                final String destKey,
                                final String... srcKeys) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.bitop(op, destKey, srcKeys);
            }
        }, SmartJedis.RW.W, destKey);
    }

    @Override
    public Response<String> clusterNodes() {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> clusterMeet(final String ip,
                                        final int port) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> clusterAddSlots(final int... slots) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> clusterDelSlots(final int... slots) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> clusterInfo() {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<List<String>> clusterGetKeysInSlot(final int slot,
                                                       final int count) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> clusterSetSlotNode(final int slot,
                                               final String nodeId) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> clusterSetSlotMigrating(final int slot,
                                                    final String nodeId) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    @Override
    public Response<String> clusterSetSlotImporting(final int slot,
                                                    final String nodeId) {
        throw new UnsupportedOperationException("Not support,maybe support in future.");
    }

    // override PipelineBase
    @Override
    public Response<Long> append(final String key,
                                 final String value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.append(key, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> append(final byte[] key,
                                 final byte[] value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.append(key, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<List<String>> blpop(final String key) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.blpop(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<List<String>> brpop(final String key) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.brpop(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<List<byte[]>> blpop(final byte[] key) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.blpop(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<List<byte[]>> brpop(final byte[] key) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.brpop(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> decr(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.decr(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> decr(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.decr(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> decrBy(final String key,
                                 final long integer) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.decrBy(key, integer);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> decrBy(final byte[] key,
                                 final long integer) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.decrBy(key, integer);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> del(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.del(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> del(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.del(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> echo(final String string) {
        return executeOnMaster(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.echo(string);
            }
        });
    }

    @Override
    public Response<byte[]> echo(final byte[] string) {
        return executeOnMaster(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.echo(string);
            }
        });
    }

    @Override
    public Response<Boolean> exists(final String key) {
        return execute(new Action<Response<Boolean>>() {
            public Response<Boolean> doAction(Pipeline pipeline) {
                return pipeline.exists(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Boolean> exists(final byte[] key) {
        return execute(new Action<Response<Boolean>>() {
            public Response<Boolean> doAction(Pipeline pipeline) {
                return pipeline.exists(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> expire(final String key,
                                 final int seconds) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.expire(key, seconds);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> expire(final byte[] key,
                                 final int seconds) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.expire(key, seconds);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> expireAt(final String key,
                                   final long unixTime) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.expireAt(key, unixTime);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> expireAt(final byte[] key,
                                   final long unixTime) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.expireAt(key, unixTime);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<byte[]> get(final byte[] key) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.get(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Boolean> getbit(final String key,
                                    final long offset) {
        return execute(new Action<Response<Boolean>>() {
            public Response<Boolean> doAction(Pipeline pipeline) {
                return pipeline.getbit(key, offset);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Boolean> getbit(final byte[] key,
                                    final long offset) {
        return execute(new Action<Response<Boolean>>() {
            public Response<Boolean> doAction(Pipeline pipeline) {
                return pipeline.getbit(key, offset);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<String> getrange(final String key,
                                     final long startOffset,
                                     final long endOffset) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.getrange(key, startOffset, endOffset);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<String> getSet(final String key,
                                   final String value) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.getSet(key, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<byte[]> getSet(final byte[] key,
                                   final byte[] value) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.getSet(key, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> getrange(final byte[] key,
                                   final long startOffset,
                                   final long endOffset) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.getrange(key, startOffset, endOffset);
            }
        }, SmartJedis.RW.R);
    }

    private String[] combineKeyFiled(RW rw,
                                     String key,
                                     String... fields) {
        int count = fields.length;
        String[] comKeys;
        if (rw.equals(RW.R)) {
            comKeys = new String[count];
            for (int i = 0; i < count; i++) {
                comKeys[i] = key + SmartJedis.COMBINESIGN + fields[i];
            }
        } else {
            comKeys = new String[count + 1];
            for (int i = 0; i < count; i++) {
                comKeys[i] = key + SmartJedis.COMBINESIGN + fields[i];
            }
            comKeys[count] = key;
        }
        return comKeys;
    }

    @Override
    public Response<Long> hdel(final String key,
                               final String... field) {
        String[] comkeys = combineKeyFiled(SmartJedis.RW.W, key, field);
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.hdel(key, field);
            }
        }, SmartJedis.RW.W, comkeys);
    }

    @Override
    public Response<Long> hdel(final byte[] key,
                               final byte[]... field) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.hdel(key, field);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Boolean> hexists(final String key,
                                     final String field) {
        String[] comkeys = combineKeyFiled(SmartJedis.RW.R, key, field);
        return execute(new Action<Response<Boolean>>() {
            public Response<Boolean> doAction(Pipeline pipeline) {
                return pipeline.hexists(key, field);
            }
        }, SmartJedis.RW.R, comkeys);
    }

    @Override
    public Response<Boolean> hexists(final byte[] key,
                                     final byte[] field) {
        return execute(new Action<Response<Boolean>>() {
            public Response<Boolean> doAction(Pipeline pipeline) {
                return pipeline.hexists(key, field);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<String> hget(final String key,
                                 final String field) {
        String[] comkeys = combineKeyFiled(SmartJedis.RW.R, key, field);
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.hget(key, field);
            }
        }, SmartJedis.RW.R, comkeys);
    }

    @Override
    public Response<byte[]> hget(final byte[] key,
                                 final byte[] field) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.hget(key, field);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Map<String, String>> hgetAll(final String key) {
        return execute(new Action<Response<Map<String, String>>>() {
            public Response<Map<String, String>> doAction(Pipeline pipeline) {
                return pipeline.hgetAll(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Map<byte[], byte[]>> hgetAll(final byte[] key) {
        return execute(new Action<Response<Map<byte[], byte[]>>>() {
            public Response<Map<byte[], byte[]>> doAction(Pipeline pipeline) {
                return pipeline.hgetAll(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> hincrBy(final String key,
                                  final String field,
                                  final long value) {
        String[] comkeys = combineKeyFiled(SmartJedis.RW.W, key, field);
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.hincrBy(key, field, value);
            }
        }, SmartJedis.RW.W, comkeys);
    }

    @Override
    public Response<Long> hincrBy(final byte[] key,
                                  final byte[] field,
                                  final long value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.hincrBy(key, field, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Set<String>> hkeys(final String key) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.hkeys(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<byte[]>> hkeys(final byte[] key) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.hkeys(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> hlen(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.hlen(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> hlen(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.hlen(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<List<String>> hmget(final String key,
                                        final String... fields) {
        String[] comKeys = combineKeyFiled(RW.R, key, fields);
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.hmget(key, fields);
            }
        }, SmartJedis.RW.R, comKeys);
    }

    @Override
    public Response<List<byte[]>> hmget(final byte[] key,
                                        final byte[]... fields) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.hmget(key, fields);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<String> hmset(final String key,
                                  final Map<String, String> hash) {
        List<String> list = new ArrayList<String>();
        list.addAll(hash.keySet());
        String[] fields = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            fields[i] = list.get(i);
        }
        String[] comKeys = combineKeyFiled(RW.W, key, fields);
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.hmset(key, hash);
            }
        }, SmartJedis.RW.W, comKeys);
    }

    @Override
    public Response<String> hmset(final byte[] key,
                                  final Map<byte[], byte[]> hash) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.hmset(key, hash);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> hset(final String key,
                               final String field,
                               final String value) {
        String[] comKeys = combineKeyFiled(RW.W, key, field);
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.hset(key, field, value);
            }
        }, SmartJedis.RW.W, comKeys);
    }

    @Override
    public Response<Long> hset(final byte[] key,
                               final byte[] field,
                               final byte[] value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.hset(key, field, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> hsetnx(final String key,
                                 final String field,
                                 final String value) {
        String[] comKeys = combineKeyFiled(RW.W, key, field);
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.hsetnx(key, field, value);
            }
        }, SmartJedis.RW.W, comKeys);
    }

    @Override
    public Response<Long> hsetnx(final byte[] key,
                                 final byte[] field,
                                 final byte[] value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.hsetnx(key, field, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<List<String>> hvals(final String key) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.hvals(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<List<byte[]>> hvals(final byte[] key) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.hvals(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> incr(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.incr(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> incr(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.incr(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> incrBy(final String key,
                                 final long integer) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.incrBy(key, integer);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> incrBy(final byte[] key,
                                 final long integer) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.incrBy(key, integer);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> lindex(final String key,
                                   final long index) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.lindex(key, index);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<byte[]> lindex(final byte[] key,
                                   final long index) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.lindex(key, index);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> linsert(final String key,
                                  final LIST_POSITION where,
                                  final String pivot,
                                  final String value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.linsert(key, where, pivot, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> linsert(final byte[] key,
                                  final LIST_POSITION where,
                                  final byte[] pivot,
                                  final byte[] value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.linsert(key, where, pivot, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> llen(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.llen(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> llen(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.llen(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<String> lpop(final String key) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.lpop(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<byte[]> lpop(final byte[] key) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.lpop(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> lpush(final String key,
                                final String... string) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.lpush(key, string);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> lpush(final byte[] key,
                                final byte[]... string) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.lpush(key, string);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> lpushx(final String key,
                                 final String... string) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.lpushx(key, string);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> lpushx(final byte[] key,
                                 final byte[]... bytes) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.lpushx(key, bytes);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<List<String>> lrange(final String key,
                                         final long start,
                                         final long end) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.lrange(key, start, end);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<List<byte[]>> lrange(final byte[] key,
                                         final long start,
                                         final long end) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.lrange(key, start, end);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> lrem(final String key,
                               final long count,
                               final String value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.lrem(key, count, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> lrem(final byte[] key,
                               final long count,
                               final byte[] value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.lrem(key, count, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> lset(final String key,
                                 final long index,
                                 final String value) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.lset(key, index, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<String> lset(final byte[] key,
                                 final long index,
                                 final byte[] value) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.lset(key, index, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> ltrim(final String key,
                                  final long start,
                                  final long end) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.ltrim(key, start, end);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<String> ltrim(final byte[] key,
                                  final long start,
                                  final long end) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.ltrim(key, start, end);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> move(final String key,
                               final int dbIndex) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.move(key, dbIndex);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> move(final byte[] key,
                               final int dbIndex) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.move(key, dbIndex);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> persist(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.persist(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> persist(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.persist(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> rpop(final String key) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.rpop(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<byte[]> rpop(final byte[] key) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.rpop(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> rpush(final String key,
                                final String... string) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.rpush(key, string);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> rpush(final byte[] key,
                                final byte[]... string) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.rpush(key, string);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> rpushx(final String key,
                                 final String... string) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.rpushx(key, string);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> rpushx(final byte[] key,
                                 final byte[]... string) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.rpushx(key, string);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> sadd(final String key,
                               final String... member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sadd(key, member);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> sadd(final byte[] key,
                               final byte[]... member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.sadd(key, member);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> scard(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.scard(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> scard(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.scard(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<String> set(final byte[] key,
                                final byte[] value) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.set(key, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Boolean> setbit(final String key,
                                    final long offset,
                                    final boolean value) {
        return execute(new Action<Response<Boolean>>() {
            public Response<Boolean> doAction(Pipeline pipeline) {
                return pipeline.setbit(key, offset, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Boolean> setbit(final byte[] key,
                                    final long offset,
                                    final byte[] value) {
        return execute(new Action<Response<Boolean>>() {
            public Response<Boolean> doAction(Pipeline pipeline) {
                return pipeline.setbit(key, offset, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> setex(final String key,
                                  final int seconds,
                                  final String value) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.setex(key, seconds, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<String> setex(final byte[] key,
                                  final int seconds,
                                  final byte[] value) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.setex(key, seconds, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> setnx(final String key,
                                final String value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.setnx(key, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> setnx(final byte[] key,
                                final byte[] value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.setnx(key, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> setrange(final String key,
                                   final long offset,
                                   final String value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.setrange(key, offset, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> setrange(final byte[] key,
                                   final long offset,
                                   final byte[] value) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.setrange(key, offset, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Boolean> sismember(final String key,
                                       final String member) {
        return execute(new Action<Response<Boolean>>() {
            public Response<Boolean> doAction(Pipeline pipeline) {
                return pipeline.sismember(key, member);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Boolean> sismember(final byte[] key,
                                       final byte[] member) {
        return execute(new Action<Response<Boolean>>() {
            public Response<Boolean> doAction(Pipeline pipeline) {
                return pipeline.sismember(key, member);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<String>> smembers(final String key) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.smembers(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<byte[]>> smembers(final byte[] key) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.smembers(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<List<String>> sort(final String key) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.sort(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<List<byte[]>> sort(final byte[] key) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.sort(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<List<String>> sort(final String key,
                                       final SortingParams sortingParameters) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.sort(key, sortingParameters);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<List<byte[]>> sort(final byte[] key,
                                       final SortingParams sortingParameters) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.sort(key, sortingParameters);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> spop(final String key) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.spop(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<byte[]> spop(final byte[] key) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.spop(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> srandmember(final String key) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.srandmember(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<List<String>> srandmember(final String key,
                                              final int count) {
        return execute(new Action<Response<List<String>>>() {
            public Response<List<String>> doAction(Pipeline pipeline) {
                return pipeline.srandmember(key, count);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<byte[]> srandmember(final byte[] key) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.srandmember(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<List<byte[]>> srandmember(final byte[] key,
                                              final int count) {
        return execute(new Action<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.srandmember(key, count);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> srem(final String key,
                               final String... member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.srem(key, member);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> srem(final byte[] key,
                               final byte[]... member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.srem(key, member);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> strlen(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.strlen(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> strlen(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.strlen(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<String> substr(final String key,
                                   final int start,
                                   final int end) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.substr(key, start, end);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<String> substr(final byte[] key,
                                   final int start,
                                   final int end) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.substr(key, start, end);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> ttl(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.ttl(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> ttl(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.ttl(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<String> type(final String key) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.type(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<String> type(final byte[] key) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.type(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> zadd(final String key,
                               final double score,
                               final String member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zadd(key, score, member);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> zadd(final String key,
                               final Map<String, Double> scoreMembers) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zadd(key, scoreMembers);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> zadd(final byte[] key,
                               final double score,
                               final byte[] member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zadd(key, score, member);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> zcard(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zcard(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> zcard(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zcard(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> zcount(final String key,
                                 final double min,
                                 final double max) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zcount(key, min, max);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> zcount(final String key,
                                 final String min,
                                 final String max) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zcount(key, min, max);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> zcount(final byte[] key,
                                 final double min,
                                 final double max) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zcount(key, min, max);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Double> zincrby(final String key,
                                    final double score,
                                    final String member) {
        return execute(new Action<Response<Double>>() {
            public Response<Double> doAction(Pipeline pipeline) {
                return pipeline.zincrby(key, score, member);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Double> zincrby(final byte[] key,
                                    final double score,
                                    final byte[] member) {
        return execute(new Action<Response<Double>>() {
            public Response<Double> doAction(Pipeline pipeline) {
                return pipeline.zincrby(key, score, member);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Set<String>> zrange(final String key,
                                        final long start,
                                        final long end) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.zrange(key, start, end);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<byte[]>> zrange(final byte[] key,
                                        final long start,
                                        final long end) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.zrange(key, start, end);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key,
                                               final double min,
                                               final double max) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScore(key, min, max);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key,
                                               final double min,
                                               final double max) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScore(key, min, max);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key,
                                               final String min,
                                               final String max) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScore(key, min, max);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key,
                                               final byte[] min,
                                               final byte[] max) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScore(key, min, max);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key,
                                               final double min,
                                               final double max,
                                               final int offset,
                                               final int count) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScore(key, min, max, offset, count);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key,
                                               final String min,
                                               final String max,
                                               final int offset,
                                               final int count) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScore(key, min, max, offset, count);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key,
                                               final double min,
                                               final double max,
                                               final int offset,
                                               final int count) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScore(key, min, max, offset, count);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key,
                                               final byte[] min,
                                               final byte[] max,
                                               final int offset,
                                               final int count) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScore(key, min, max, offset, count);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key,
                                                        final double min,
                                                        final double max) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScoreWithScores(key, min, max);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key,
                                                        final String min,
                                                        final String max) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScoreWithScores(key, min, max);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key,
                                                        final double min,
                                                        final double max) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScoreWithScores(key, min, max);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key,
                                                        final byte[] min,
                                                        final byte[] max) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScoreWithScores(key, min, max);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key,
                                                        final double min,
                                                        final double max,
                                                        final int offset,
                                                        final int count) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key,
                                                        final String min,
                                                        final String max,
                                                        final int offset,
                                                        final int count) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key,
                                                        final double min,
                                                        final double max,
                                                        final int offset,
                                                        final int count) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key,
                                                        final byte[] min,
                                                        final byte[] max,
                                                        final int offset,
                                                        final int count) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key,
                                                  final double max,
                                                  final double min) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScore(key, max, min);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key,
                                                  final double max,
                                                  final double min) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScore(key, max, min);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key,
                                                  final String max,
                                                  final String min) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScore(key, max, min);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key,
                                                  final byte[] max,
                                                  final byte[] min) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScore(key, max, min);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key,
                                                  final double max,
                                                  final double min,
                                                  final int offset,
                                                  final int count) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScore(key, max, min, offset, count);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key,
                                                  final String max,
                                                  final String min,
                                                  final int offset,
                                                  final int count) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScore(key, max, min, offset, count);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key,
                                                  final double max,
                                                  final double min,
                                                  final int offset,
                                                  final int count) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScore(key, max, min, offset, count);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key,
                                                  final byte[] max,
                                                  final byte[] min,
                                                  final int offset,
                                                  final int count) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScore(key, max, min, offset, count);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key,
                                                           final double max,
                                                           final double min) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScoreWithScores(key, max, min);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key,
                                                           final String max,
                                                           final String min) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScoreWithScores(key, max, min);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key,
                                                           final double max,
                                                           final double min) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScoreWithScores(key, max, min);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key,
                                                           final byte[] max,
                                                           final byte[] min) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScoreWithScores(key, max, min);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key,
                                                           final double max,
                                                           final double min,
                                                           final int offset,
                                                           final int count) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key,
                                                           final String max,
                                                           final String min,
                                                           final int offset,
                                                           final int count) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key,
                                                           final double max,
                                                           final double min,
                                                           final int offset,
                                                           final int count) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key,
                                                           final byte[] max,
                                                           final byte[] min,
                                                           final int offset,
                                                           final int count) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<Tuple>> zrangeWithScores(final String key,
                                                 final long start,
                                                 final long end) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrangeWithScores(key, start, end);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<Tuple>> zrangeWithScores(final byte[] key,
                                                 final long start,
                                                 final long end) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrangeWithScores(key, start, end);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> zrank(final String key,
                                final String member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zrank(key, member);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> zrank(final byte[] key,
                                final byte[] member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zrank(key, member);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> zrem(final String key,
                               final String... member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zrem(key, member);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> zrem(final byte[] key,
                               final byte[]... member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zrem(key, member);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> zremrangeByRank(final String key,
                                          final long start,
                                          final long end) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zremrangeByRank(key, start, end);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> zremrangeByRank(final byte[] key,
                                          final long start,
                                          final long end) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zremrangeByRank(key, start, end);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> zremrangeByScore(final String key,
                                           final double start,
                                           final double end) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zremrangeByScore(key, start, end);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> zremrangeByScore(final String key,
                                           final String start,
                                           final String end) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zremrangeByScore(key, start, end);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> zremrangeByScore(final byte[] key,
                                           final double start,
                                           final double end) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zremrangeByScore(key, start, end);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> zremrangeByScore(final byte[] key,
                                           final byte[] start,
                                           final byte[] end) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zremrangeByScore(key, start, end);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Set<String>> zrevrange(final String key,
                                           final long start,
                                           final long end) {
        return execute(new Action<Response<Set<String>>>() {
            public Response<Set<String>> doAction(Pipeline pipeline) {
                return pipeline.zrevrange(key, start, end);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<byte[]>> zrevrange(final byte[] key,
                                           final long start,
                                           final long end) {
        return execute(new Action<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doAction(Pipeline pipeline) {
                return pipeline.zrevrange(key, start, end);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeWithScores(final String key,
                                                    final long start,
                                                    final long end) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeWithScores(key, start, end);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeWithScores(final byte[] key,
                                                    final long start,
                                                    final long end) {
        return execute(new Action<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doAction(Pipeline pipeline) {
                return pipeline.zrevrangeWithScores(key, start, end);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> zrevrank(final String key,
                                   final String member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zrevrank(key, member);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> zrevrank(final byte[] key,
                                   final byte[] member) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.zrevrank(key, member);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Double> zscore(final String key,
                                   final String member) {
        return execute(new Action<Response<Double>>() {
            public Response<Double> doAction(Pipeline pipeline) {
                return pipeline.zscore(key, member);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Double> zscore(final byte[] key,
                                   final byte[] member) {
        return execute(new Action<Response<Double>>() {
            public Response<Double> doAction(Pipeline pipeline) {
                return pipeline.zscore(key, member);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> bitcount(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.bitcount(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> bitcount(final String key,
                                   final long start,
                                   final long end) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.bitcount(key, start, end);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> bitcount(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.bitcount(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<Long> bitcount(final byte[] key,
                                   final long start,
                                   final long end) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.bitcount(key, start, end);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<byte[]> dump(final String key) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.dump(key);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<byte[]> dump(final byte[] key) {
        return execute(new Action<Response<byte[]>>() {
            public Response<byte[]> doAction(Pipeline pipeline) {
                return pipeline.dump(key);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> migrate(final String host,
                                    final int port,
                                    final String key,
                                    final int destinationDb,
                                    final int timeout) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.migrate(host, port, key, destinationDb, timeout);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<String> migrate(final byte[] host,
                                    final int port,
                                    final byte[] key,
                                    final int destinationDb,
                                    final int timeout) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.migrate(host, port, key, destinationDb, timeout);
            }
        }, SmartJedis.RW.W);
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
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.pexpire(key, milliseconds);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> pexpire(final byte[] key,
                                  final int milliseconds) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.pexpire(key, milliseconds);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> pexpireAt(final String key,
                                    final long millisecondsTimestamp) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.pexpireAt(key, millisecondsTimestamp);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Long> pexpireAt(final byte[] key,
                                    final long millisecondsTimestamp) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.pexpireAt(key, millisecondsTimestamp);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Long> pttl(final String key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.pttl(key);
            }
        }, SmartJedis.RW.R, key);
    }

    @Override
    public Response<Long> pttl(final byte[] key) {
        return execute(new Action<Response<Long>>() {
            public Response<Long> doAction(Pipeline pipeline) {
                return pipeline.pttl(key);
            }
        }, SmartJedis.RW.R);
    }

    @Override
    public Response<String> restore(final String key,
                                    final int ttl,
                                    final byte[] serializedValue) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.restore(key, ttl, serializedValue);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<String> restore(final byte[] key,
                                    final int ttl,
                                    final byte[] serializedValue) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.restore(key, ttl, serializedValue);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Double> incrByFloat(final String key,
                                        final double increment) {
        return execute(new Action<Response<Double>>() {
            public Response<Double> doAction(Pipeline pipeline) {
                return pipeline.incrByFloat(key, increment);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Double> incrByFloat(final byte[] key,
                                        final double increment) {
        return execute(new Action<Response<Double>>() {
            public Response<Double> doAction(Pipeline pipeline) {
                return pipeline.incrByFloat(key, increment);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> psetex(final String key,
                                   final int milliseconds,
                                   final String value) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.psetex(key, milliseconds, value);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<String> psetex(final byte[] key,
                                   final int milliseconds,
                                   final byte[] value) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.psetex(key, milliseconds, value);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> set(final String key,
                                final String value,
                                final String nxxx) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.set(key, value, nxxx);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<String> set(final byte[] key,
                                final byte[] value,
                                final byte[] nxxx) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.set(key, value, nxxx);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> set(final String key,
                                final String value,
                                final String nxxx,
                                final String expx,
                                final int time) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.set(key, value, nxxx, expx, time);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<String> set(final byte[] key,
                                final byte[] value,
                                final byte[] nxxx,
                                final byte[] expx,
                                final int time) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.set(key, value, nxxx, expx, time);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<Double> hincrByFloat(final String key,
                                         final String field,
                                         final double increment) {
        return execute(new Action<Response<Double>>() {
            public Response<Double> doAction(Pipeline pipeline) {
                return pipeline.hincrByFloat(key, field, increment);
            }
        }, SmartJedis.RW.W, key);
    }

    @Override
    public Response<Double> hincrByFloat(final byte[] key,
                                         final byte[] field,
                                         final double increment) {
        return execute(new Action<Response<Double>>() {
            public Response<Double> doAction(Pipeline pipeline) {
                return pipeline.hincrByFloat(key, field, increment);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> eval(final String script) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.eval(script);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> eval(final String script,
                                 final List<String> keys,
                                 final List<String> args) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.eval(script, keys, args);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> eval(final String script,
                                 final int numKeys,
                                 final String[] argv) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.eval(script, numKeys, argv);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> evalsha(final String script) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.evalsha(script);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> evalsha(final String sha1,
                                    final List<String> keys,
                                    final List<String> args) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.evalsha(sha1, keys, args);
            }
        }, SmartJedis.RW.W);
    }

    @Override
    public Response<String> evalsha(final String sha1,
                                    final int numKeys,
                                    final String[] argv) {
        return execute(new Action<Response<String>>() {
            public Response<String> doAction(Pipeline pipeline) {
                return pipeline.evalsha(sha1, numKeys, argv);
            }
        }, SmartJedis.RW.W);
    }

}
