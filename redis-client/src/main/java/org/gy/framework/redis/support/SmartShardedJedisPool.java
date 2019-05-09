package org.gy.framework.redis.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;

public class SmartShardedJedisPool extends Pool<ShardedJedis> {
    private static final Logger         logger               = LoggerFactory.getLogger(SmartShardedJedisPool.class);
    private static final int            TIMER_DELAY          = 2000;
    private Map<String, SmartJedisPool> pools                = new ConcurrentHashMap<String, SmartJedisPool>();
    protected Timer                     timer;
    private long                        lastPrintStatusTime  = 0L;
    private List<HostAndPort>           sentinelHostAndPorts = new ArrayList<HostAndPort>();
    private volatile RwPolicy           rwPolicy;

    public SmartShardedJedisPool(List<String> masters, Set<String> sentinels, final GenericObjectPoolConfig poolConfig, WarningService warningService, Long execTimeThreshold) {
        this(masters, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, warningService, execTimeThreshold);
    }

    public SmartShardedJedisPool(List<String> masters, Set<String> sentinels, final GenericObjectPoolConfig poolConfig, int timeout, WarningService warningService, Long execTimeThreshold) {
        this(masters, sentinels, poolConfig, timeout, null, warningService, execTimeThreshold);
    }

    public SmartShardedJedisPool(List<String> masters, Set<String> sentinels, final GenericObjectPoolConfig poolConfig, int timeout, final String password, WarningService warningService, Long execTimeThreshold) {
        this(masters, sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE, warningService, execTimeThreshold);
    }

    public void setMasterChangedListener(MasterChangedListener masterChangedListener) {
        for (SmartJedisPool pool : pools.values()) {
            pool.setMasterChangedListener(masterChangedListener);
        }
    }

    public SmartShardedJedisPool(List<String> masters, Set<String> sentinels, final GenericObjectPoolConfig poolConfig, int timeout, final String password, final int database, WarningService warningService, Long execTimeThreshold) {
        this(masters, sentinels, poolConfig, timeout, password, database, new SimpleRwPolicy(), warningService, execTimeThreshold);
    }

    @SuppressWarnings("unchecked")
    public SmartShardedJedisPool(List<String> masters, Set<String> sentinels, final GenericObjectPoolConfig poolConfig, int timeout, final String password, final int database, final RwPolicy rwPolicy, WarningService warningService, Long execTimeThreshold) {
        if (warningService == null) {
            // throw new IllegalArgumentException("WarningService is must.");
            logger.warn("WarningService is null.");
        }
        // create pool for each shard
        for (String master : masters) {
            Object[] result = initSentinels(sentinels, master);
            SmartJedisPool smartJedisPool = new SmartJedisPool((HostAndPort) result[0], (List<HostAndPort>) result[1], poolConfig, timeout, password, database, rwPolicy, warningService, execTimeThreshold);
            pools.put(master, smartJedisPool);
        }
        this.rwPolicy = rwPolicy;
        // start pool check timer.
        timer = new Timer("Pool check timer", true);
        timer.schedule(new TimerTask() {
            public void run() {
                try {
                    try {
                        getRwPolicy().check();
                    } catch (Throwable ex) {
                        logger.warn("Exception:", ex);
                    }
                    if (System.currentTimeMillis() - lastPrintStatusTime > 1000 * 60) {
                        lastPrintStatusTime = System.currentTimeMillis();
                        try {
                            logger.info(PoolStatusUtil.getPoolStatus(SmartShardedJedisPool.this));
                            checkDiscardShardnameInvalid();
                        } catch (Throwable ex) {
                            logger.warn("Exception:", ex);
                        }
                    }
                    for (SmartJedisPool pool : pools.values()) {
                        try {
                            pool.check();
                        } catch (Throwable ex) {
                            logger.warn("Exception:", ex);
                        }
                    }
                } catch (Throwable ex) {
                    logger.warn("Exception:", ex);
                }
            }
        }, TIMER_DELAY, TIMER_DELAY);
        // monitor master change.
        for (String sentinel : sentinels) {
            final HostAndPort hap = MyHostAndPort.toHostAndPort(Arrays.asList(sentinel.split(":")));
            sentinelHostAndPorts.add(hap);
            MasterMonitor.getInstance().monitor(hap, masterListener);
        }
        // create self pool.
        List<JedisShardInfo> shardInfoList = makeShardInfoList(masters);
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(poolConfig.getMaxIdle());
        config.setMaxTotal(Integer.MAX_VALUE);
        config.setBlockWhenExhausted(false);
        config.setMinIdle(poolConfig.getMinIdle());
        initPool(config, new MyShardedJedisFactory(this, shardInfoList, Hashing.MURMUR_HASH, null));
    }

    private void checkDiscardShardnameInvalid() {
        StringBuilder sb = new StringBuilder();
        Set<String> allshardNames = pools.keySet();
        for (String discardShardname : getRwPolicy().getDiscardShardnames()) {
            if (!allshardNames.contains(discardShardname)) {
                sb.append(discardShardname);
                sb.append(",");
            }
        }
        if (sb.length() > 0) {
            logger.warn(String.format("discard shardname %s is not usedShardName please check!", sb.substring(0, sb.length() - 1)));
        }
    }

    private MasterMonitor.MasterListener masterListener = new MasterMonitor.MasterListener() {
                                                            @Override
                                                            public void masterChanged(String masterName,
                                                                                      HostAndPort newMaster) {
                                                                SmartJedisPool smartJedisPool = getJedisPool(masterName);
                                                                if (smartJedisPool == null) {
                                                                    logger.info("Ignoring message on +switch-master for master name " + masterName + ", our master name is " + pools.keySet());
                                                                    return;
                                                                }
                                                                smartJedisPool.initJedisPools(newMaster, null);
                                                            }
                                                        };

    /**
     * get specified shard pool.
     * 
     * @param shardName shard name
     * @return pool
     */
    public SmartJedisPool getJedisPool(String shardName) {
        return pools.get(shardName);
    }

    public List<String> getShardNames() {
        List<String> keyList = new ArrayList<String>(pools.keySet());
        Collections.sort(keyList);
        return keyList;
    }

    private List<JedisShardInfo> makeShardInfoList(List<String> masters) {
        List<JedisShardInfo> shardInfos = new ArrayList<JedisShardInfo>();
        for (String master : masters) {
            JedisShardInfo jedisShardInfo = new MyJedisShardInfo(master);
            shardInfos.add(jedisShardInfo);
        }
        return shardInfos;
    }

    @Override
    public void returnResourceObject(ShardedJedis resource) {
        SmartShardedJedis shardedJedis = (SmartShardedJedis) resource;
        shardedJedis.returnResources();
        super.returnResourceObject(resource);
    }

    @Override
    protected void returnBrokenResourceObject(ShardedJedis resource) {
        SmartShardedJedis shardedJedis = (SmartShardedJedis) resource;
        shardedJedis.returnBrokenResources();
        // SmartShardedJedis itself is ok,so invoke return.. instead of returnBroken..
        super.returnResourceObject(resource);
    }

    @Override
    public void destroy() {
        for (HostAndPort hap : sentinelHostAndPorts) {
            MasterMonitor.getInstance().unMonitor(hap, masterListener);
        }
        super.destroy();
    }

    @Override
    protected void closeInternalPool() {
        for (SmartJedisPool pool : pools.values()) {
            try {
                pool.destroy();
            } catch (Exception ex) {
                logger.error("Exception:", ex);
            }
        }
        pools.clear();
        if (timer != null) {
            timer.cancel();
        }
        super.closeInternalPool();
    }

    private static class MyShardedJedisFactory extends BasePooledObjectFactory<ShardedJedis> {
        private SmartShardedJedisPool smartShardedJedisPool;
        private List<JedisShardInfo>  shards;
        private Hashing               algo;
        private Pattern               keyTagPattern;

        public MyShardedJedisFactory(SmartShardedJedisPool smartShardedJedisPool, List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
            this.smartShardedJedisPool = smartShardedJedisPool;
            this.shards = shards;
            this.algo = algo;
            this.keyTagPattern = keyTagPattern;
        }

        @Override
        public ShardedJedis create() throws Exception {
            return new SmartShardedJedis(smartShardedJedisPool, shards, algo, keyTagPattern);
        }

        @Override
        public PooledObject<ShardedJedis> wrap(ShardedJedis shardedJedis) {
            return new DefaultPooledObject<ShardedJedis>(shardedJedis);
        }
    }

    public class MyJedisShardInfo extends JedisShardInfo {
        public MyJedisShardInfo(String master) {
            super(null, master);
        }

        @Override
        public Jedis createResource() {
            // not create jedis.
            return null;
        }
    }

    private Object[] initSentinels(Set<String> sentinels,
                                   final String masterName) {
        HostAndPort master = null;
        List<Map<String, String>> slaveInfo;
        List<HostAndPort> slaves = new ArrayList<HostAndPort>();
        int i = 0;
        outer: while (true) {
            if ((i++) > 20) {
                logger.error("Has try 20 times,failed.");
                throw new RuntimeException("Has try 20 times,failed.");
            }
            logger.info("Trying to find " + masterName + " master from available Sentinels...");
            for (String sentinel : sentinels) {
                final HostAndPort hap = MyHostAndPort.toHostAndPort(Arrays.asList(sentinel.split(":")));
                logger.info("Connecting to Sentinel " + hap);
                Jedis jedis = new Jedis(hap.getHost(), hap.getPort());
                try {
                    master = MyHostAndPort.toHostAndPort(jedis.sentinelGetMasterAddrByName(masterName));
                    logger.info("Found " + masterName + " Redis master at " + master);
                    slaveInfo = jedis.sentinelSlaves(masterName);
                    logger.info("Found " + masterName + " Redis slaves at " + slaveInfo);
                    break outer;
                } catch (JedisConnectionException e) {
                    logger.warn("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
                } finally {
                    try {
                        try {
                            jedis.quit();
                        } catch (Throwable ex) {
                            // ignore
                        }
                        jedis.disconnect();
                    } catch (Throwable ex) {
                        // ignore
                    }
                }
            }
            try {
                logger.error("All sentinels down, cannot determine where is " + masterName + " master is running... sleeping 5000ms.");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (Map<String, String> map : slaveInfo) {
            slaves.add(new MyHostAndPort(map.get("ip"), Integer.valueOf(map.get("port"))));
        }
        logger.info("Redis master running at " + master + ",slaves running at " + slaves);
        return new Object[] {
                master,
                slaves
        };
    }

    public void setWarningService(WarningService warningService) {
        List<String> shards = getShardNames();
        for (String shard : shards) {
            getJedisPool(shard).warningService = warningService;
        }
    }

    public void setRwPolicy(RwPolicy rwPolicy) {
        this.rwPolicy.clearCache();
        List<String> shards = getShardNames();
        for (String shard : shards) {
            getJedisPool(shard).rwPolicy = rwPolicy;
        }
        this.rwPolicy = rwPolicy;
    }

    public RwPolicy getRwPolicy() {
        return this.rwPolicy;
    }

    public void setExecTimeThreshold(Long execTimeThreshold) {
        List<String> shards = getShardNames();
        for (String shard : shards) {
            getJedisPool(shard).execTimeThreshold = execTimeThreshold;
        }
    }

}
