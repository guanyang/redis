package org.gy.framework.redis.lock;

import org.gy.framework.redis.response.Result;

public interface DistributedLockRunnable {

    Result run();
}
