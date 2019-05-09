package org.gy.framework.redis.impl;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.gy.framework.redis.exception.RedisClientException;
import org.gy.framework.redis.support.MasterChangedListener;
import org.gy.framework.redis.support.PoolStatusUtil;
import org.gy.framework.redis.support.RwPolicy;
import org.gy.framework.redis.support.SimpleRwPolicy;
import org.gy.framework.redis.support.SmartShardedJedisPool;
import org.gy.framework.redis.support.WarningService;
import org.gy.framework.redis.util.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public abstract class AbstractClient {

    private static final Map<String, SmartShardedJedisPool> smartShardedJedisPools = new ConcurrentHashMap<>();

    private static final Object                             MUX                    = new Object();

    private String                                          configPath;

    protected AbstractClient(String configPath) {
        this(configPath, null);
    }

    protected AbstractClient(String configPath, WarningService warningService) {
        this.configPath = configPath;
        if (!smartShardedJedisPools.containsKey(configPath)) {
            synchronized (MUX) {
                if (!smartShardedJedisPools.containsKey(configPath)) {
                    String redisConfig = ResourceUtils.getResourceAsString(configPath);
                    if (redisConfig == null || "".equals(redisConfig.trim())) {
                        throw new RedisClientException("can't find redis config or config content is empty.");
                    }
                    SmartShardedJedisPool pool = XMLParser.parse(redisConfig, warningService);
                    smartShardedJedisPools.put(this.configPath, pool);
                }
            }
        }
    }

    private boolean notEqual(Set<String> setA,
                             Set<String> setB) {
        List<String> listA = new ArrayList<String>(setA);
        List<String> listB = new ArrayList<String>(setB);
        return notEqual(listA, listB);
    }

    private boolean notEqual(List<String> listA,
                             List<String> listB) {
        Collections.sort(listA, new Comparator<String>() {
            public int compare(String s1,
                               String s2) {
                return s1.compareToIgnoreCase(s2);
            }
        });
        Collections.sort(listB, new Comparator<String>() {
            public int compare(String s1,
                               String s2) {
                return s1.compareToIgnoreCase(s2);
            }
        });
        StringBuilder strA = new StringBuilder();
        for (String s : listA) {
            strA.append(s.trim());
        }
        StringBuilder strB = new StringBuilder();
        for (String s : listB) {
            strB.append(s.trim());
        }
        return notEqual(strA.toString(), strB.toString());
    }

    private boolean notEqual(String strA,
                             String strB) {
        return !(strA == null || strB == null) && !strA.trim().equalsIgnoreCase(strB.trim());
    }

    public SmartShardedJedisPool getSmartShardedJedisPool() {
        return smartShardedJedisPools.get(configPath);
    }

    public void setMasterChangedListener(MasterChangedListener masterChangedListener) {
        getSmartShardedJedisPool().setMasterChangedListener(masterChangedListener);
    }

    protected void destroy() {
        synchronized (MUX) {
            SmartShardedJedisPool pool = getSmartShardedJedisPool();
            if (pool != null) {
                pool.destroy();
            }
            smartShardedJedisPools.remove(configPath);
        }
    }

    public String getPoolStatus() {
        return PoolStatusUtil.getPoolStatus(getSmartShardedJedisPool());
    }

    private static class XMLParser {
        private static Logger   logger = LoggerFactory.getLogger(XMLParser.class);

        private static XPath    path;

        private static Document doc;

        private static String getString(Object node,
                                        String expression) throws XPathExpressionException {
            return (String) path.evaluate(expression, node, XPathConstants.STRING);
        }

        private static NodeList getList(Object node,
                                        String expression) throws XPathExpressionException {
            return (NodeList) path.evaluate(expression, node, XPathConstants.NODESET);
        }

        private static Node getNode(Object node,
                                    String expression) throws XPathExpressionException {
            return (Node) path.evaluate(expression, node, XPathConstants.NODE);
        }

        public static String parse(String redisConfig,
                                   String key) {
            try {
                StringReader reader = new StringReader(redisConfig);
                InputSource is = new InputSource(reader);
                DocumentBuilder dbd = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                doc = dbd.parse(is);
                path = XPathFactory.newInstance().newXPath();
                Node rootN = getNode(doc, "config");
                if (null == rootN) {
                    throw new RedisClientException("Invalid xml format, can't find <config> root node!");
                }
                return getString(rootN, key);
            } catch (Exception ex) {
                throw new RedisClientException("Fail to parse redis configure file.", ex);
            }
        }

        public static String poolConfig(String redisConfig,
                                        String field) {
            try {
                StringReader reader = new StringReader(redisConfig);
                InputSource is = new InputSource(reader);
                DocumentBuilder dbd = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                doc = dbd.parse(is);
                path = XPathFactory.newInstance().newXPath();
                Node rootN = getNode(doc, "config");
                if (null == rootN) {
                    throw new RedisClientException("Invalid xml format, can't find <config> root node!");
                }
                Node poolConfigNode = getNode(rootN, "poolConfig");
                return getString(poolConfigNode, field);
            } catch (Exception ex) {
                throw new RedisClientException("Fail to parse redis configure file.", ex);
            }
        }

        public static Set<String> parseSentinels(String redisConfig) {
            try {
                StringReader reader = new StringReader(redisConfig);
                InputSource is = new InputSource(reader);
                DocumentBuilder dbd = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                doc = dbd.parse(is);
                path = XPathFactory.newInstance().newXPath();
                Node rootN = getNode(doc, "config");
                if (null == rootN) {
                    throw new RedisClientException("Invalid xml format, can't find <config> root node!");
                }
                Node sentinelNode = getNode(rootN, "sentinels");
                Set<String> sentinels = new HashSet<String>();
                NodeList sentinelConfigs = getList(sentinelNode, "sentinel");
                for (int i = 0; i < sentinelConfigs.getLength(); i++) {
                    Node sentinelConfig = sentinelConfigs.item(i);
                    String ip = getString(sentinelConfig, "ip");
                    String port = getString(sentinelConfig, "port");
                    sentinels.add(ip + ":" + port);
                }
                return sentinels;
            } catch (Exception ex) {
                throw new RedisClientException("Fail to parse redis configure file.", ex);
            }
        }

        public static List<String> parseMasters(String redisConfig) {
            try {
                StringReader reader = new StringReader(redisConfig);
                InputSource is = new InputSource(reader);
                DocumentBuilder dbd = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                doc = dbd.parse(is);
                path = XPathFactory.newInstance().newXPath();
                Node rootN = getNode(doc, "config");
                if (null == rootN) {
                    throw new RedisClientException("Invalid xml format, can't find <config> root node!");
                }
                List<String> masters = new ArrayList<String>();
                Node mastersNode = getNode(rootN, "shards");
                if (mastersNode != null) {
                    NodeList masterNodes = getList(mastersNode, "shardName");
                    for (int i = 0; i < masterNodes.getLength(); i++) {
                        String master = masterNodes.item(i).getTextContent();
                        masters.add(master);
                    }
                }
                return masters;
            } catch (Exception ex) {
                throw new RedisClientException("Fail to parse redis configure file.", ex);
            }
        }

        public static List<String> forceMasterKeys(String redisConfig) {
            try {
                StringReader reader = new StringReader(redisConfig);
                InputSource is = new InputSource(reader);
                DocumentBuilder dbd = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                doc = dbd.parse(is);
                path = XPathFactory.newInstance().newXPath();
                Node rootN = getNode(doc, "config");
                if (null == rootN) {
                    throw new RedisClientException("Invalid xml format, can't find <config> root node!");
                }
                List<String> forceMasterKeys = new ArrayList<String>();
                Node forceMasterKeysNode = getNode(rootN, "forceMasterkeys");
                if (forceMasterKeysNode != null) {
                    NodeList keyPatternNodes = getList(forceMasterKeysNode, "keyPattern");
                    for (int i = 0; i < keyPatternNodes.getLength(); i++) {
                        String master = keyPatternNodes.item(i).getTextContent();
                        forceMasterKeys.add(master);
                    }
                }
                return forceMasterKeys;
            } catch (Exception ex) {
                throw new RedisClientException("Fail to parse redis configure file.", ex);
            }
        }

        public static RwPolicy parseRwPolicy(String redisConfig) {
            try {
                StringReader reader = new StringReader(redisConfig);
                InputSource is = new InputSource(reader);
                DocumentBuilder dbd = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                doc = dbd.parse(is);
                path = XPathFactory.newInstance().newXPath();
                Node rootN = getNode(doc, "config");
                if (null == rootN) {
                    throw new RedisClientException("Invalid xml format, can't find <config> root node!");
                }
                RwPolicy rwPolicy = null;
                Node forceMasterKeysNode = getNode(rootN, "forceMasterkeys");
                if (forceMasterKeysNode != null) {
                    NodeList keyPatternNodes = getList(forceMasterKeysNode, "keyPattern");
                    if (keyPatternNodes != null) {
                        String[] keyPatterns = new String[keyPatternNodes.getLength()];
                        for (int i = 0; i < keyPatternNodes.getLength(); i++) {
                            String keyPattern = keyPatternNodes.item(i).getTextContent();
                            keyPatterns[i] = keyPattern;
                        }
                        rwPolicy = new SimpleRwPolicy(keyPatterns);
                    }
                }
                if (rwPolicy == null) {
                    String forceMaster = getString(rootN, "forceMaster");
                    if (null == forceMaster || "".equals(forceMaster.trim())) {
                        forceMaster = "true";
                    }
                    Boolean isForceMaster = Boolean.valueOf(forceMaster);
                    if (isForceMaster) {
                        rwPolicy = new SimpleRwPolicy(true);
                    } else {
                        rwPolicy = new SimpleRwPolicy(false);
                    }
                }
                return rwPolicy;
            } catch (Exception ex) {
                throw new RedisClientException("Fail to parse redis configure file.", ex);
            }
        }

        public static SmartShardedJedisPool parse(String redisConfig,
                                                  WarningService warningService) {
            try {
                StringReader reader = new StringReader(redisConfig);
                InputSource is = new InputSource(reader);
                DocumentBuilder dbd = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                doc = dbd.parse(is);
                path = XPathFactory.newInstance().newXPath();
                Node rootN = getNode(doc, "config");
                if (null == rootN) {
                    throw new RedisClientException("Invalid xml format, can't find <config> root node!");
                }
                String timeOut = getString(rootN, "timeOut");
                if (null == timeOut || "".equals(timeOut.trim())) {
                    timeOut = "2000";
                }
                String password = getString(rootN, "password");
                if (null == password || "".equals(password.trim())) {
                    password = null;
                }
                String dbIndex = getString(rootN, "dbIndex");
                if (null == dbIndex || "".equals(dbIndex.trim())) {
                    dbIndex = "0";
                }
                GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
                Node poolConfigNode = getNode(rootN, "poolConfig");
                if (poolConfigNode != null) {
                    poolConfig.setMaxTotal(Integer.MAX_VALUE);
                    poolConfig.setMaxWaitMillis(200);
                    poolConfig.setBlockWhenExhausted(false);
                    String maxIdle = getString(poolConfigNode, "maxIdle");
                    if (null != maxIdle && !"".equals(maxIdle.trim())) {
                        poolConfig.setMaxIdle(Integer.valueOf(maxIdle));
                    }
                    String minIdle = getString(poolConfigNode, "minIdle");
                    if (null != minIdle && !"".equals(minIdle.trim())) {
                        poolConfig.setMinIdle(Integer.valueOf(minIdle));
                    }
                    String lifo = getString(poolConfigNode, "lifo");
                    if (null != lifo && !"".equals(lifo.trim())) {
                        poolConfig.setLifo(Boolean.valueOf(lifo));
                    }
                    String minEvictableIdleTimeMillis = getString(poolConfigNode, "minEvictableIdleTimeMillis");
                    if (null != minEvictableIdleTimeMillis && !"".equals(minEvictableIdleTimeMillis.trim())) {
                        poolConfig.setMinEvictableIdleTimeMillis(Long.valueOf(minEvictableIdleTimeMillis));
                    } else {
                        poolConfig.setMinEvictableIdleTimeMillis(60000L);
                    }
                    String softMinEvictableIdleTimeMillis = getString(poolConfigNode, "softMinEvictableIdleTimeMillis");
                    if (null != softMinEvictableIdleTimeMillis && !"".equals(softMinEvictableIdleTimeMillis.trim())) {
                        poolConfig.setSoftMinEvictableIdleTimeMillis(Long.valueOf(softMinEvictableIdleTimeMillis));
                    }
                    String numTestsPerEvictionRun = getString(poolConfigNode, "numTestsPerEvictionRun");
                    if (null != numTestsPerEvictionRun && !"".equals(numTestsPerEvictionRun.trim())) {
                        poolConfig.setNumTestsPerEvictionRun(Integer.valueOf(numTestsPerEvictionRun));
                    } else {
                        poolConfig.setNumTestsPerEvictionRun(-1);
                    }
                    String evictionPolicyClassName = getString(poolConfigNode, "evictionPolicyClassName");
                    if (null != evictionPolicyClassName && !"".equals(evictionPolicyClassName.trim())) {
                        poolConfig.setEvictionPolicyClassName(evictionPolicyClassName);
                    }
                    String testOnBorrow = getString(poolConfigNode, "testOnBorrow");
                    if (null != testOnBorrow && !"".equals(testOnBorrow.trim())) {
                        // 获取连接池是否检测可用性
                        poolConfig.setTestOnBorrow(Boolean.valueOf(testOnBorrow));
                    }
                    String testOnReturn = getString(poolConfigNode, "testOnReturn");
                    if (null != testOnReturn && !"".equals(testOnReturn.trim())) {
                        // 归还时是否检测可用性
                        poolConfig.setTestOnReturn(Boolean.valueOf(testOnReturn));
                    }
                    String testWhileIdle = getString(poolConfigNode, "testWhileIdle");
                    if (null != testWhileIdle && !"".equals(testWhileIdle.trim())) {
                        // 空闲时是否检测可用性
                        poolConfig.setTestWhileIdle(Boolean.valueOf(testWhileIdle));
                    } else {
                        poolConfig.setTestWhileIdle(true);
                    }
                    String timeBetweenEvictionRunsMillis = getString(poolConfigNode, "timeBetweenEvictionRunsMillis");
                    if (null != timeBetweenEvictionRunsMillis && !"".equals(timeBetweenEvictionRunsMillis.trim())) {
                        poolConfig.setTimeBetweenEvictionRunsMillis(Long.valueOf(timeBetweenEvictionRunsMillis));
                    } else {
                        poolConfig.setTimeBetweenEvictionRunsMillis(30000L);
                    }
                    String jmxEnabled = getString(poolConfigNode, "jmxEnabled");
                    if (null != jmxEnabled && !"".equals(jmxEnabled.trim())) {
                        poolConfig.setJmxEnabled(Boolean.valueOf(jmxEnabled));
                    }
                    String jmxNamePrefix = getString(poolConfigNode, "jmxNamePrefix");
                    if (null != jmxNamePrefix && !"".equals(jmxNamePrefix.trim())) {
                        poolConfig.setJmxNamePrefix(jmxNamePrefix);
                    }
                }
                Node sentinelNode = getNode(rootN, "sentinels");
                Set<String> sentinels = new HashSet<String>();
                NodeList sentinelConfigs = getList(sentinelNode, "sentinel");
                if (sentinelConfigs.getLength() != 0 && sentinelConfigs.getLength() < 3) {
                    throw new RedisClientException("Configuration error,no less than 3 sentinels");
                }
                for (int i = 0; i < sentinelConfigs.getLength(); i++) {
                    Node sentinelConfig = sentinelConfigs.item(i);
                    String ip = getString(sentinelConfig, "ip");
                    String port = getString(sentinelConfig, "port");
                    if (null == ip || "".equals(ip.trim())) {
                        throw new RedisClientException("Configuration error,sentinel host can not be null");
                    }
                    if (null == port || "".equals(port.trim())) {
                        port = "26379";
                    }
                    sentinels.add(ip + ":" + port);
                }
                List<String> masters = new ArrayList<String>();
                Node mastersNode = getNode(rootN, "shards");
                if (mastersNode == null) {
                    throw new RedisClientException("Configuration error, <shards> can not be null in <shardConfig> ");
                }
                NodeList masterNodes = getList(mastersNode, "shardName");
                if (masterNodes.getLength() == 0) {
                    throw new RedisClientException("Configuration error, <shardName> can not be null in <shards> ");
                }
                for (int i = 0; i < masterNodes.getLength(); i++) {
                    String master = masterNodes.item(i).getTextContent();
                    if (null == master || "".equals(master.trim())) {
                        throw new RedisClientException("Configuration error,<master> can not be null in <shard>");
                    }
                    masters.add(master);
                }
                RwPolicy rwPolicy = null;
                Node forceMasterKeysNode = getNode(rootN, "forceMasterkeys");
                if (forceMasterKeysNode != null) {
                    NodeList keyPatternNodes = getList(forceMasterKeysNode, "keyPattern");
                    if (keyPatternNodes != null) {
                        String[] keyPatterns = new String[keyPatternNodes.getLength()];
                        for (int i = 0; i < keyPatternNodes.getLength(); i++) {
                            String keyPattern = keyPatternNodes.item(i).getTextContent();
                            keyPatterns[i] = keyPattern;
                        }
                        rwPolicy = new SimpleRwPolicy(keyPatterns);
                    }
                }
                if (rwPolicy == null) {
                    String forceMaster = getString(rootN, "forceMaster");
                    if (null == forceMaster || "".equals(forceMaster.trim())) {
                        forceMaster = "true";
                    }
                    Boolean isForceMaster = Boolean.valueOf(forceMaster);
                    if (isForceMaster) {
                        rwPolicy = new SimpleRwPolicy(true);
                    } else {
                        rwPolicy = new SimpleRwPolicy(false);
                    }
                }
                String disCardShardnames = getString(rootN, "disCardShardnames");
                rwPolicy.setDisCardShardnames(disCardShardnames);
                String writeKeyCacheTime = getString(rootN, "writeKeyCacheTime");
                rwPolicy.setWriteKeyCacheTime(writeKeyCacheTime);
                String execTimeThreshold = getString(rootN, "execTimeThreshold");
                if (null == execTimeThreshold || "".equals(execTimeThreshold.trim())) {
                    execTimeThreshold = "20";
                }
                return new SmartShardedJedisPool(masters, sentinels, poolConfig, Integer.valueOf(timeOut), password, Integer.valueOf(dbIndex), rwPolicy, warningService, Long.valueOf(execTimeThreshold));
            } catch (IOException e) {
                logger.error("IOException!", e);
                throw new RedisClientException("IOException!", e);
            } catch (Exception ex) {
                throw new RedisClientException("Fail to parse redis configure file.", ex);
            }
        }
    }

}
