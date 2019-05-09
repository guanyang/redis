package org.gy.framework.redis.support;

import java.util.List;

public interface RwPolicy {

    public boolean canReadSlave(String... keys);

    public void keyHasBeenWritten(String key);

    public void check();

    public void clearCache();

    /**
     * 重新设置discardShardname，在初始化及scm配置修改时调用
     * 
     */
    public void setDisCardShardnames(String disCardShardnames);

    /**
     * 判断shardname是否为discard的
     * 
     */
    public boolean isDisCardShardname(String shardname);

    /**
     * 获得当前discardShardnames
     * 
     */
    public List<String> getDiscardShardnames();

    /**
     * 设置缓存的新写key的缓存时间
     * 
     */
    public void setWriteKeyCacheTime(String writeKeyCacheTime);
}
