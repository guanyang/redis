package org.gy.framework.redis.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class SimpleRwPolicy implements RwPolicy {

    private static final long   DEFAULT_WRITE_KEY_CACHE_TIME = 1000 * 60;

    private long                writeKeyCacheTime            = DEFAULT_WRITE_KEY_CACHE_TIME;

    private Map<String, Long>   cache                        = new ConcurrentHashMap<String, Long>();

    private boolean             forceMaster                  = true;

    private List<Pattern>       forceMasterKeyPatterns       = new ArrayList<Pattern>();

    private Map<String, String> disCardShardnameMaps         = new ConcurrentHashMap<String, String>();

    public SimpleRwPolicy() {
        this(true);
    }

    public SimpleRwPolicy(boolean forceMaster) {
        this.forceMaster = forceMaster;
    }

    public SimpleRwPolicy(String[] forceMasterKeyPatterns) {
        this.forceMaster = false;
        if (forceMasterKeyPatterns != null && forceMasterKeyPatterns.length != 0) {
            for (String str : forceMasterKeyPatterns) {
                Pattern pattern = Pattern.compile(str);
                this.forceMasterKeyPatterns.add(pattern);
            }
        }
    }

    @Override
    public boolean canReadSlave(String... keys) {
        if (keys == null || keys.length == 0) {
            return false;
        }
        for (String key : keys) {
            if (isForceMasterKey(key) || cache.containsKey(key)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void keyHasBeenWritten(final String key) {
        if (!isForceMasterKey(key)) {
            cache.put(key, System.currentTimeMillis());
        }
    }

    private boolean isForceMasterKey(String key) {
        if (forceMaster) {
            return true;
        }
        if (key.contains(SmartJedis.COMBINESIGN)) {
            key = key.split(SmartJedis.COMBINESIGN)[0];
        }
        if (forceMasterKeyPatterns != null && !forceMasterKeyPatterns.isEmpty()) {
            for (Pattern pattern : forceMasterKeyPatterns) {
                if (pattern.matcher(key).find()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void check() {
        for (Map.Entry<String, Long> entry : cache.entrySet()) {
            if (System.currentTimeMillis() - entry.getValue() > writeKeyCacheTime) {
                cache.remove(entry.getKey());
            }
        }
    }

    @Override
    public void clearCache() {
        if (null != cache) {
            cache.clear();
        }
    }

    @Override
    public void setDisCardShardnames(String disCardShardnames) {
        disCardShardnameMaps.clear();
        if (disCardShardnames != null) {
            for (String shardName : disCardShardnames.split(",")) {
                String shardNameStr = shardName.trim();
                if (!"".equals(shardNameStr)) {
                    disCardShardnameMaps.put(shardNameStr, shardNameStr);
                }
            }
        }
    }

    @Override
    public boolean isDisCardShardname(String shardname) {
        return disCardShardnameMaps.containsKey(shardname);
    }

    @Override
    public List<String> getDiscardShardnames() {
        return new ArrayList<String>(disCardShardnameMaps.keySet());
    }

    @Override
    public void setWriteKeyCacheTime(String writeKeyCacheTime) {
        if (writeKeyCacheTime == null || writeKeyCacheTime.trim().length() == 0) {
            this.writeKeyCacheTime = DEFAULT_WRITE_KEY_CACHE_TIME;
        } else {
            this.writeKeyCacheTime = Long.valueOf(writeKeyCacheTime);
        }
    }

}
