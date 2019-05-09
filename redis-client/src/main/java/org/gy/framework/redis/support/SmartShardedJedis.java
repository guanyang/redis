package org.gy.framework.redis.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.MultiKeyBinaryCommands;
import redis.clients.jedis.MultiKeyCommands;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.ZParams;
import redis.clients.util.Hashing;

public class SmartShardedJedis extends ShardedJedis implements MultiKeyCommands, MultiKeyBinaryCommands {

    private static final Logger                        logger    = LoggerFactory.getLogger(SmartShardedJedis.class);

    private SmartShardedJedisPool                      smartShardedJedisPool;

    private transient volatile Map<String, SmartJedis> resources = new ConcurrentHashMap<String, SmartJedis>();

    public SmartShardedJedis(SmartShardedJedisPool smartShardedJedisPool, List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
        super(shards, algo, keyTagPattern);
        this.smartShardedJedisPool = smartShardedJedisPool;
    }

    @Override
    public Jedis getShard(String key) {
        JedisShardInfo shardInfo = getShardInfo(key);
        return getSmartShard(shardInfo.getName());
    }

    @Override
    public Jedis getShard(byte[] key) {
        JedisShardInfo shardInfo = getShardInfo(key);
        return getSmartShard(shardInfo.getName());
    }

    public Jedis getShardByMod(Long number) {
        String shardName = getShardNameByMod(number);
        return getSmartShard(shardName);
    }

    public String getShardNameByMod(Long number) {
        List<String> shardNames = smartShardedJedisPool.getShardNames();
        int index = (int) (Math.abs(number) % shardNames.size());
        return shardNames.get(index);
    }

    @Override
    public Collection<Jedis> getAllShards() {
        List<Jedis> list = new ArrayList<Jedis>();
        List<String> shardNames = smartShardedJedisPool.getShardNames();
        for (String shardName : shardNames) {
            try {
                list.add(getSmartShard(shardName));
            } catch (DiscardShardException e) {
            }
        }
        return Collections.unmodifiableCollection(list);
    }

    public SmartJedis getSmartShard(String name) {
        if (smartShardedJedisPool.getRwPolicy().isDisCardShardname(name)) {
            throw new DiscardShardException("discard shardname:" + name);
        }
        SmartJedis jedis = resources.get(name);
        if (jedis != null) {
            return jedis;
        }
        SmartJedisPool pool = smartShardedJedisPool.getJedisPool(name);
        jedis = (SmartJedis) pool.getResource();
        resources.put(name, jedis);
        return jedis;
    }

    protected void returnResources() {
        for (Map.Entry<String, SmartJedis> entry : resources.entrySet()) {
            try {
                String name = entry.getKey();
                SmartJedisPool pool = smartShardedJedisPool.getJedisPool(name);
                pool.returnResource(entry.getValue());
            } catch (Exception ex) {
                logger.error("Exception:", ex);
            }
        }
        resources.clear();
    }

    /**
     * 如果SmartShardedJedis使用过程出现JedisConnectionException，那么可能是某一个SmartJedis的连接出问题。
     * 让使用到的所有SmartJedis执行检查，如果确实有问题，则销毁(参考SmartJedisPool.returnBrokenResource)
     */
    protected void returnBrokenResources() {
        for (Map.Entry<String, SmartJedis> entry : resources.entrySet()) {
            try {
                String name = entry.getKey();
                SmartJedisPool pool = smartShardedJedisPool.getJedisPool(name);
                pool.returnBrokenResource(entry.getValue());
            } catch (Exception ex) {
                logger.error("Exception:", ex);
            }
        }
        resources.clear();
    }

    @Override
    public ShardedJedisPipeline pipelined() {
        return new SmartShardedJedisPipeline(this);
    }

    public ShardedJedisPipeline pipelined(SmartShardedJedisPipeline.ShardSelector shardSelector) {
        return new SmartShardedJedisPipeline(this, shardSelector);
    }

    @Override
    public List<String> mget(final String... keys) {
        List<String> allKeyList = Arrays.asList(keys);
        List<String> allValueList = Arrays.asList(new String[allKeyList.size()]);
        Map<JedisShardInfo, List<Object[]>> preparedMap = new HashMap<JedisShardInfo, List<Object[]>>();
        for (int k = 0; k < allKeyList.size(); k++) {
            String key = allKeyList.get(k);
            JedisShardInfo info = getShardInfo(key);
            List<Object[]> list = preparedMap.get(info);
            if (list == null) {
                list = new ArrayList<Object[]>();
                preparedMap.put(info, list);
            }
            list.add(new Object[] {
                    k,
                    key
            });
        }
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        Map<List<Object[]>, Response<List<String>>> responseMap = new HashMap<List<Object[]>, Response<List<String>>>();
        for (Entry<JedisShardInfo, List<Object[]>> entry : preparedMap.entrySet()) {
            JedisShardInfo info = entry.getKey();
            List<Object[]> keyList = entry.getValue();
            String[] paramByte = null;
            if (keyList != null && keyList.size() > 0) {
                paramByte = new String[keyList.size()];
                for (int j = 0; j < keyList.size(); j++) {
                    paramByte[j] = (String) keyList.get(j)[1];
                }
            }
            Response<List<String>> listResponse = pipeline.mgetOnShard(info, paramByte);
            responseMap.put(keyList, listResponse);
        }
        pipeline.sync();
        for (Entry<List<Object[]>, Response<List<String>>> entry : responseMap.entrySet()) {
            List<Object[]> keyList = entry.getKey();
            List<String> result = entry.getValue().get();
            for (int i = 0; i < keyList.size(); i++) {
                Object[] key = keyList.get(i);
                if (result != null && result.size() > 0) {
                    allValueList.set((Integer) key[0], result.get(i));
                }
            }
        }
        return allValueList;
    }

    @Override
    public List<byte[]> mget(final byte[]... keys) {
        List<byte[]> allKeyList = Arrays.asList(keys);
        List<byte[]> allValueList = Arrays.asList(new byte[allKeyList.size()][]);
        Map<JedisShardInfo, List<Object[]>> preparedMap = new HashMap<JedisShardInfo, List<Object[]>>();
        for (int k = 0; k < allKeyList.size(); k++) {
            byte[] key = allKeyList.get(k);
            JedisShardInfo info = getShardInfo(key);
            List<Object[]> list = preparedMap.get(info);
            if (list == null) {
                list = new ArrayList<Object[]>();
                preparedMap.put(info, list);
            }
            list.add(new Object[] {
                    k,
                    key
            });
        }
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        Map<List<Object[]>, Response<List<byte[]>>> responseMap = new HashMap<List<Object[]>, Response<List<byte[]>>>();
        for (Entry<JedisShardInfo, List<Object[]>> entry : preparedMap.entrySet()) {
            JedisShardInfo info = entry.getKey();
            List<Object[]> keyList = entry.getValue();
            byte[][] paramByte = null;
            if (keyList != null && keyList.size() > 0) {
                paramByte = new byte[keyList.size()][];
                for (int j = 0; j < keyList.size(); j++) {
                    paramByte[j] = (byte[]) keyList.get(j)[1];
                }
            }
            Response<List<byte[]>> listResponse = pipeline.mgetOnShard(info, paramByte);
            responseMap.put(keyList, listResponse);
        }
        pipeline.sync();
        for (Entry<List<Object[]>, Response<List<byte[]>>> entry : responseMap.entrySet()) {
            List<Object[]> keyList = entry.getKey();
            List<byte[]> result = entry.getValue().get();
            for (int i = 0; i < keyList.size(); i++) {
                Object[] key = keyList.get(i);
                if (result != null && result.size() > 0) {
                    allValueList.set((Integer) key[0], result.get(i));
                }
            }
        }
        return allValueList;
    }

    @Override
    public String mset(final String... keysvalues) {
        Map<JedisShardInfo, Map<String, String>> preparedMap = new HashMap<JedisShardInfo, Map<String, String>>();
        for (int i = 0; i < keysvalues.length; i++) {
            JedisShardInfo info = getShardInfo(keysvalues[i]);
            if (!preparedMap.containsKey(info)) {
                preparedMap.put(info, new HashMap<String, String>());
            }
            preparedMap.get(info).put(keysvalues[i], keysvalues[++i]);
        }
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        for (Entry<JedisShardInfo, Map<String, String>> entry : preparedMap.entrySet()) {
            JedisShardInfo info = entry.getKey();
            Map<String, String> kvMap = entry.getValue();
            pipeline.msetOnShard(info, smapToArray(kvMap));
        }
        pipeline.sync();
        return "OK";
    }

    @Override
    public String mset(final byte[]... keysvalues) {
        Map<JedisShardInfo, Map<byte[], byte[]>> preparedMap = new HashMap<JedisShardInfo, Map<byte[], byte[]>>();
        for (int i = 0; i < keysvalues.length; i++) {
            JedisShardInfo info = getShardInfo(keysvalues[i]);
            if (!preparedMap.containsKey(info)) {
                preparedMap.put(info, new HashMap<byte[], byte[]>());
            }
            preparedMap.get(info).put(keysvalues[i], keysvalues[++i]);
        }
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        for (Entry<JedisShardInfo, Map<byte[], byte[]>> entry : preparedMap.entrySet()) {
            JedisShardInfo info = entry.getKey();
            Map<byte[], byte[]> kvMap = entry.getValue();
            pipeline.msetOnShard(info, bmapToArray(kvMap));
        }
        pipeline.sync();
        return "OK";
    }

    @Override
    public Long msetnx(final String... keysvalues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long msetnx(final byte[]... keysvalues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long del(final String... keys) {
        Map<JedisShardInfo, List<String>> preparedMap = new HashMap<JedisShardInfo, List<String>>();
        for (String key : keys) {
            JedisShardInfo info = getShardInfo(key);
            if (!preparedMap.containsKey(info)) {
                preparedMap.put(info, new ArrayList<String>());
            }
            preparedMap.get(info).add(key);
        }
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        List<Response<Long>> responseList = new ArrayList<Response<Long>>();
        for (Entry<JedisShardInfo, List<String>> entry : preparedMap.entrySet()) {
            JedisShardInfo info = entry.getKey();
            List<String> keyList = entry.getValue();
            responseList.add(pipeline.delOnShard(info, slistToArray(keyList)));
        }
        pipeline.sync();
        Long result = 0l;
        for (Response<Long> response : responseList) {
            result = result + response.get();
        }
        return result;
    }

    @Override
    public Long del(final byte[]... keys) {
        Map<JedisShardInfo, List<byte[]>> preparedMap = new HashMap<JedisShardInfo, List<byte[]>>();
        for (byte[] key : keys) {
            JedisShardInfo info = getShardInfo(key);
            if (!preparedMap.containsKey(info)) {
                preparedMap.put(info, new ArrayList<byte[]>());
            }
            preparedMap.get(info).add(key);
        }
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        List<Response<Long>> responseList = new ArrayList<Response<Long>>();
        for (Entry<JedisShardInfo, List<byte[]>> entry : preparedMap.entrySet()) {
            JedisShardInfo info = entry.getKey();
            List<byte[]> keyList = entry.getValue();
            responseList.add(pipeline.delOnShard(info, blistToArray(keyList)));
        }
        pipeline.sync();
        Long result = 0l;
        for (Response<Long> response : responseList) {
            result = result + response.get();
        }
        return result;
    }

    @Override
    public Set<String> keys(final String pattern) {
        Set<String> allResult = new HashSet<String>();
        Collection<JedisShardInfo> infos = getAllShardInfo();
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        List<Response<Set<String>>> responseList = new ArrayList<Response<Set<String>>>();
        for (JedisShardInfo info : infos) {
            responseList.add(pipeline.keysOnShard(info, pattern));
        }
        pipeline.sync();
        for (Response<Set<String>> response : responseList) {
            allResult.addAll(response.get());
        }
        return allResult;
    }

    @Override
    public Set<byte[]> keys(final byte[] pattern) {
        Set<byte[]> allResult = new HashSet<byte[]>();
        Collection<JedisShardInfo> infos = getAllShardInfo();
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        List<Response<Set<byte[]>>> responseList = new ArrayList<Response<Set<byte[]>>>();
        for (JedisShardInfo info : infos) {
            responseList.add(pipeline.keysOnShard(info, pattern));
        }
        pipeline.sync();
        for (Response<Set<byte[]>> response : responseList) {
            allResult.addAll(response.get());
        }
        return allResult;
    }

    private static String[] slistToArray(List<String> list) {
        String[] paramByte = null;
        if (list != null && list.size() > 0) {
            paramByte = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                paramByte[i] = list.get(i);
            }
        }
        return paramByte;
    }

    private static byte[][] blistToArray(List<byte[]> list) {
        byte[][] paramByte = null;
        if (list != null && list.size() > 0) {
            paramByte = new byte[list.size()][];
            for (int i = 0; i < list.size(); i++) {
                paramByte[i] = list.get(i);
            }
        }
        return paramByte;
    }

    private static String[] smapToArray(Map<String, String> map) {
        String[] paramByte = null;
        if (map != null && map.size() > 0) {
            paramByte = new String[map.size() * 2];
            Iterator<Entry<String, String>> it = map.entrySet().iterator();
            int index = 0;
            while (it.hasNext()) {
                Entry<String, String> entry = it.next();
                paramByte[index++] = entry.getKey();
                paramByte[index++] = entry.getValue();
            }
        }
        return paramByte;
    }

    private static byte[][] bmapToArray(Map<byte[], byte[]> map) {
        byte[][] paramByte = null;
        if (map != null && map.size() > 0) {
            paramByte = new byte[map.size() * 2][0];
            Iterator<Entry<byte[], byte[]>> it = map.entrySet().iterator();
            int index = 0;
            while (it.hasNext()) {
                Entry<byte[], byte[]> entry = it.next();
                paramByte[index++] = entry.getKey();
                paramByte[index++] = entry.getValue();
            }
        }
        return paramByte;
    }

    @Override
    public List<String> blpop(int timeout,
                              String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> brpop(int timeout,
                              String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> blpop(String... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> brpop(String... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String rename(String oldkey,
                         String newkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long renamenx(String oldkey,
                         String newkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String rpoplpush(String srcKey,
                            String dstKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> sdiff(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sdiffstore(String dstKey,
                           String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> sinter(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sinterstore(String dstKey,
                            String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long smove(String srcKey,
                      String dstKey,
                      String member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sort(String key,
                     SortingParams sortingParameters,
                     String dstKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sort(String key,
                     String dstKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> sunion(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sunionstore(String dstKey,
                            String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String watch(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String unwatch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zinterstore(String dstKey,
                            String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zinterstore(String dstKey,
                            ZParams params,
                            String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zunionstore(String dstKey,
                            String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zunionstore(String dstKey,
                            ZParams params,
                            String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String brpoplpush(String source,
                             String destination,
                             int timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long publish(String channel,
                        String message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub,
                          String... channels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub,
                           String... patterns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String randomKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long bitop(BitOP op,
                      String destKey,
                      String... srcKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<String> scan(int cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<String> scan(String cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<byte[]> blpop(int timeout,
                              byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<byte[]> brpop(int timeout,
                              byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<byte[]> blpop(byte[]... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<byte[]> brpop(byte[]... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String rename(byte[] oldkey,
                         byte[] newkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long renamenx(byte[] oldkey,
                         byte[] newkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] rpoplpush(byte[] srcKey,
                            byte[] dstKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sdiffstore(byte[] dstKey,
                           byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sinterstore(byte[] dstKey,
                            byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long smove(byte[] srcKey,
                      byte[] dstKey,
                      byte[] member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sort(byte[] key,
                     SortingParams sortingParameters,
                     byte[] dstKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sort(byte[] key,
                     byte[] dstKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sunionstore(byte[] dstKey,
                            byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String watch(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zinterstore(byte[] dstKey,
                            byte[]... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zinterstore(byte[] dstKey,
                            ZParams params,
                            byte[]... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zunionstore(byte[] dstKey,
                            byte[]... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zunionstore(byte[] dstKey,
                            ZParams params,
                            byte[]... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] brpoplpush(byte[] source,
                             byte[] destination,
                             int timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long publish(byte[] channel,
                        byte[] message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(BinaryJedisPubSub jedisPubSub,
                          byte[]... channels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub,
                           byte[]... patterns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] randomBinaryKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long bitop(BitOP op,
                      byte[] dstKey,
                      byte[]... srcKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String pfmerge(byte[] destkey,
                          byte[]... sourcekeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<String> scan(String cursor,
                                   ScanParams params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String pfmerge(String destkey,
                          String... sourcekeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long exists(String... keys) {
        Map<JedisShardInfo, List<String>> preparedMap = new HashMap<>();
        for (String key : keys) {
            JedisShardInfo info = getShardInfo(key);
            if (!preparedMap.containsKey(info)) {
                preparedMap.put(info, new ArrayList<String>());
            }
            preparedMap.get(info).add(key);
        }
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        List<Response<Long>> responseList = new ArrayList<>();
        for (Entry<JedisShardInfo, List<String>> entry : preparedMap.entrySet()) {
            JedisShardInfo info = entry.getKey();
            List<String> keyList = entry.getValue();
            responseList.add(pipeline.existsOnShard(info, slistToArray(keyList)));
        }
        pipeline.sync();
        Long result = 0l;
        for (Response<Long> response : responseList) {
            result = result + response.get();
        }
        return result;

    }

    @Override
    public Long exists(byte[]... keys) {
        Map<JedisShardInfo, List<byte[]>> preparedMap = new HashMap<>();
        for (byte[] key : keys) {
            JedisShardInfo info = getShardInfo(key);
            if (!preparedMap.containsKey(info)) {
                preparedMap.put(info, new ArrayList<byte[]>());
            }
            preparedMap.get(info).add(key);
        }
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        List<Response<Long>> responseList = new ArrayList<>();
        for (Entry<JedisShardInfo, List<byte[]>> entry : preparedMap.entrySet()) {
            JedisShardInfo info = entry.getKey();
            List<byte[]> keyList = entry.getValue();
            responseList.add(pipeline.existsOnShard(info, blistToArray(keyList)));
        }
        pipeline.sync();
        Long result = 0l;
        for (Response<Long> response : responseList) {
            result = result + response.get();
        }
        return result;
    }

    @Override
    public long pfcount(String... keys) {
        Map<JedisShardInfo, List<String>> preparedMap = new HashMap<>();
        for (String key : keys) {
            JedisShardInfo info = getShardInfo(key);
            if (!preparedMap.containsKey(info)) {
                preparedMap.put(info, new ArrayList<String>());
            }
            preparedMap.get(info).add(key);
        }
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        List<Response<Long>> responseList = new ArrayList<>();
        for (Entry<JedisShardInfo, List<String>> entry : preparedMap.entrySet()) {
            JedisShardInfo info = entry.getKey();
            List<String> keyList = entry.getValue();
            responseList.add(pipeline.pfcountOnShard(info, slistToArray(keyList)));
        }
        pipeline.sync();
        Long result = 0l;
        for (Response<Long> response : responseList) {
            result = result + response.get();
        }
        return result;
    }

    @Override
    public Long pfcount(byte[]... keys) {
        Map<JedisShardInfo, List<byte[]>> preparedMap = new HashMap<>();
        for (byte[] key : keys) {
            JedisShardInfo info = getShardInfo(key);
            if (!preparedMap.containsKey(info)) {
                preparedMap.put(info, new ArrayList<byte[]>());
            }
            preparedMap.get(info).add(key);
        }
        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) pipelined();
        List<Response<Long>> responseList = new ArrayList<>();
        for (Entry<JedisShardInfo, List<byte[]>> entry : preparedMap.entrySet()) {
            JedisShardInfo info = entry.getKey();
            List<byte[]> keyList = entry.getValue();
            responseList.add(pipeline.pfcountOnShard(info, blistToArray(keyList)));
        }
        pipeline.sync();
        Long result = 0l;
        for (Response<Long> response : responseList) {
            result = result + response.get();
        }
        return result;
    }
}
