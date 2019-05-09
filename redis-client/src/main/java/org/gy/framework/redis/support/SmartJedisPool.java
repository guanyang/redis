package org.gy.framework.redis.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

public class SmartJedisPool extends Pool<Jedis> {

    private static final Logger                      logger                 = LoggerFactory.getLogger(SmartJedisPool.class);

    private static final int                         POOL_UNAVAILABLE_LIMIT = 10;

    private static final String                      CHECK_KEY              = "_CHECK_KEY_";

    private static final String                      CHECK_VALUE            = "_CHECK_SIGN_";

    protected GenericObjectPoolConfig                poolConfig;

    protected int                                    timeout                = Protocol.DEFAULT_TIMEOUT;

    protected String                                 password;

    protected Long                                   execTimeThreshold      = 20l;

    protected volatile RwPolicy                      rwPolicy;

    protected WarningService                         warningService;

    protected int                                    database               = Protocol.DEFAULT_DATABASE;

    private volatile HostAndPort                     currentHostMaster;

    private MasterChangedListener                    masterChangedListener;

    private Map<HostAndPort, JedisPool>              jedisPools             = new ConcurrentHashMap<HostAndPort, JedisPool>();

    private ConcurrentHashMap<JedisPool, AtomicLong> poolUnavailableCounter = new ConcurrentHashMap<JedisPool, AtomicLong>();

    private final static int                         checkRetryNum          = 3;                                              // 可用性检查重试次数

    protected SmartJedisPool(HostAndPort currentHostMaster, List<HostAndPort> slaveHosts, GenericObjectPoolConfig poolConfig, int timeout, String password, final int database, RwPolicy rwPolicy, WarningService warningService, Long execTimeThreshold) {
        this.poolConfig = poolConfig;
        if (this.poolConfig.getBlockWhenExhausted()) {
            logger.warn("Config BlockWhenExhausted is true,deadLock may happen,modify to false.");
            this.poolConfig.setBlockWhenExhausted(false);
        }
        if (this.poolConfig.getMaxTotal() < Integer.MAX_VALUE) {
            logger.warn("Config maxTotal is a limited value,deadLock may happen,modify to Integer.MAX_VALUE.");
            this.poolConfig.setMaxTotal(Integer.MAX_VALUE);
        }
        if (this.poolConfig.getMaxWaitMillis() > 0) {
            logger.warn("BlockWhenExhausted is force set to false,so maxWait is not necessary.");
        }
        this.timeout = timeout;
        this.password = password;
        this.database = database;
        this.rwPolicy = rwPolicy;
        this.warningService = warningService;
        this.execTimeThreshold = execTimeThreshold;
        // 初始化所有JedisPool池
        initJedisPools(currentHostMaster, slaveHosts);
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(poolConfig.getMaxIdle());
        config.setMaxTotal(Integer.MAX_VALUE);
        config.setBlockWhenExhausted(false);
        config.setMinIdle(poolConfig.getMinIdle());
        // 初始化SmartJedis对象池
        initPool(config, new SmartJedisFactory(this));
    }

    public void setMasterChangedListener(MasterChangedListener masterChangedListener) {
        this.masterChangedListener = masterChangedListener;
    }

    @Override
    public void returnResourceObject(Jedis resource) {
        SmartJedis smartJedis = (SmartJedis) resource;
        smartJedis.cleanResources(false);
        super.returnResourceObject(resource);
    }

    @Override
    protected void returnBrokenResourceObject(Jedis resource) {
        SmartJedis smartJedis = (SmartJedis) resource;
        smartJedis.cleanResources(true);
        // SmartJedis自身不存在问题，因此不必销毁
        super.returnResourceObject(resource);
    }

    @Override
    protected void closeInternalPool() {
        for (JedisPool pool : jedisPools.values()) {
            try {
                pool.destroy();
            } catch (Exception ex) {
                logger.error("Exception:", ex);
            }
        }
        jedisPools.clear();
        poolUnavailableCounter.clear();
        super.closeInternalPool();
    }

    protected JedisPool getMasterJedisPool() {
        // master pool not check available.
        return jedisPools.get(currentHostMaster);
    }

    protected List<JedisPool> getSlaveJedisPools() {
        List<JedisPool> list = new ArrayList<JedisPool>();
        for (Map.Entry<HostAndPort, JedisPool> entry : jedisPools.entrySet()) {
            // when get slave pools, exclude unavailable pool.
            if (!entry.getKey().equals(currentHostMaster) && !(exceedUnavailableLimit(entry.getValue()))) {
                list.add(entry.getValue());
            }
        }
        return list;
    }

    protected List<JedisPool> getAvailableJedisPools() {
        List<JedisPool> list = new ArrayList<JedisPool>();
        for (Map.Entry<HostAndPort, JedisPool> entry : jedisPools.entrySet()) {
            if (!exceedUnavailableLimit(entry.getValue())) {
                list.add(entry.getValue());
            }
        }
        return list;
    }

    protected RwPolicy getRwPolicy() {
        return this.rwPolicy;
    }

    protected void clearUnavailableCounter(JedisPool pool) {
        poolUnavailableCounter.remove(pool);
    }

    protected void increaseUnavailableCounter(JedisPool jedisPool) {
        AtomicLong initValue = new AtomicLong(0L);
        AtomicLong counter = poolUnavailableCounter.putIfAbsent(jedisPool, initValue);
        if (counter == null) {
            counter = initValue;
        }
        if (counter.incrementAndGet() >= POOL_UNAVAILABLE_LIMIT) {
            String message = "Sedis warning:App Host:" + NetUtils.getLocalHost() + ",Current master is " + currentHostMaster + ",pool " + jedisPool + " current not available.";
            logger.warn(message);
            if (warningService != null) {
                warningService.sendWarningMessage(message);
            }
        }
    }

    /**
     * 功能描述: <br>
     * 尝试对目标redis server的可用性进行检查
     * 
     * @param hostAndPort
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    private void tryCheckUsable(HostAndPort hostAndPort) {
        JedisPool jedisPool = jedisPools.get(hostAndPort);
        for (int i = 1; i <= checkRetryNum; i++) {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                if (jedis.get(CHECK_KEY) != null) {
                    clearUnavailableCounter(jedisPool);
                } else if (hostAndPort.equals(currentHostMaster)) {
                    jedis.set(CHECK_KEY, CHECK_VALUE);
                }
                return;
            } catch (Throwable jex) {
                if (jex instanceof JedisConnectionException) {
                    if (jedis != null) {
                        try {
                            jedisPool.returnBrokenResource(jedis);
                        } catch (Exception ex) {
                            logger.warn("Can not return broken resource.", ex);
                        }
                        jedis = null;
                    }
                    increaseUnavailableCounter(jedisPool);
                }
                if (i >= checkRetryNum) {
                    logger.info("Exception occur at " + jedisPool.toString() + ".", jex);
                    return;
                } else {
                    try {
                        Thread.sleep(500L);
                    } catch (InterruptedException e) {
                    }
                }
            } finally {
                if (jedis != null) {
                    try {
                        jedisPool.returnResource(jedis);
                    } catch (Exception ex) {
                        logger.warn("Can not return resource.", ex);
                    }
                }
            }
        }
    }

    /**
     * 检查JedisPool所有redis server的可用性
     */
    protected void check() {
        for (HostAndPort hostAndPort : jedisPools.keySet()) {
            tryCheckUsable(hostAndPort);
        }
    }

    private boolean exceedUnavailableLimit(JedisPool pool) {
        AtomicLong counter = poolUnavailableCounter.get(pool);
        return counter != null && counter.get() >= POOL_UNAVAILABLE_LIMIT;
    }

    /**
     * 初始化master、slaves JedisPools
     * 
     * @param master master hostAndPort
     * @param slaveHosts slave hostAndPort list
     */
    protected synchronized void initJedisPools(HostAndPort master,
                                               List<HostAndPort> slaveHosts) {
        if (master != null) {
            if (!jedisPools.containsKey(master)) {
                jedisPools.put(master, new JedisPoolWrapper(poolConfig, master.getHost(), master.getPort(), timeout, password, database));
            }
            if (!master.equals(currentHostMaster)) {
                if (currentHostMaster != null) {
                    if (warningService != null) {
                        warningService.sendWarningMessage("Sedis warning:App Host:" + NetUtils.getLocalHost() + ",Master changed,current is " + currentHostMaster + ",new is " + master);
                    }
                    if (masterChangedListener != null) {
                        try {
                            masterChangedListener.onMasterChanged(currentHostMaster, master);
                        } catch (Throwable ex) {
                            logger.info("Exception in MasterChangedListener.", ex);
                        }
                    }
                }
                currentHostMaster = master;
                logger.info("Master JedisPool set to " + master);
            }
        }
        if (slaveHosts != null) {
            for (HostAndPort slaveHost : slaveHosts) {
                if (!jedisPools.containsKey(slaveHost)) {
                    jedisPools.put(slaveHost, new JedisPoolWrapper(poolConfig, slaveHost.getHost(), slaveHost.getPort(), timeout, password, database));
                }
            }
        }
    }

    public static class JedisPoolWrapper extends JedisPool {

        private String host;

        private int    port;

        private int    database;

        public JedisPoolWrapper(GenericObjectPoolConfig poolConfig, String host, int port, int timeout, String password, int database) {
            super(poolConfig, host, port, timeout, password, database);
            this.host = host;
            this.port = port;
            this.database = database;
        }

        public String toString() {
            return "jedisPool:{host:" + this.host + ",port:" + this.port + ",database:" + this.database + "}";
        }

        public Jedis getResource() {
            try {
                return super.getResource();
            } catch (JedisException ex) {
                logger.error("Can not get jedis from " + toString(), ex);
                throw ex;
            }
        }
    }

    private static class SmartJedisFactory extends BasePooledObjectFactory<Jedis> {

        private SmartJedisPool smartJedisPool;

        public SmartJedisFactory(SmartJedisPool smartJedisPool) {
            this.smartJedisPool = smartJedisPool;
        }

        @Override
        public Jedis create() throws Exception {
            return new SmartJedis(smartJedisPool);
        }

        @Override
        public PooledObject<Jedis> wrap(Jedis smartJedis) {
            return new DefaultPooledObject<Jedis>(smartJedis);
        }
    }

}
