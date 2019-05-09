package org.gy.framework.redis.support;

public interface WarningService {

    public void sendWarningMessage(String msg);

    public void setPhones(String phones);

    public String getPhones();

}
