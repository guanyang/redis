package org.gy.framework.redis.support;

public abstract class ReadInstructor {

    public static final ThreadLocal<Boolean> canReadSlaveFlag = new ThreadLocal<Boolean>();

    public static void setCanReadSlaveFlag(boolean flag) {
        canReadSlaveFlag.set(flag);
    }

    public static void cleanCanReadSlaveFlag() {
        canReadSlaveFlag.remove();
    }

    public static Boolean getCanReadSlaveFlag() {
        return canReadSlaveFlag.get();
    }

}
