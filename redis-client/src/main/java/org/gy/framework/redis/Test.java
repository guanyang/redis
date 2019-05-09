package org.gy.framework.redis;

import java.util.concurrent.TimeUnit;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SentinelServersConfig;

public class Test {

    public static void main(String[] args) {
        Config config = new Config();
        SentinelServersConfig sentinelServersConfig = config.useSentinelServers();
        sentinelServersConfig.setMasterName("mymaster1");
        sentinelServersConfig.addSentinelAddress("redis://172.19.59.22:26379");
        sentinelServersConfig.addSentinelAddress("redis://172.19.59.23:26379");
        sentinelServersConfig.addSentinelAddress("redis://172.19.59.24:26379");
        RedissonClient client = Redisson.create(config);
        RLock fairLock = client.getFairLock("gy:test");
        boolean tryLock = false;
        try {
            tryLock = fairLock.tryLock(5000, 10000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (tryLock) {
                fairLock.unlock();
            }
        }
    }

}
