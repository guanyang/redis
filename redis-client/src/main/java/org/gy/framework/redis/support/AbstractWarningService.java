package org.gy.framework.redis.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractWarningService implements WarningService {

    private String            phones;

    private Map<String, Long> messages = new ConcurrentHashMap<String, Long>();

    @Override
    public void sendWarningMessage(String msg) {
        if (this.phones == null || this.phones.equals("")) {
            return;
        }
        Long lastSendTime = messages.get(msg);
        if (lastSendTime == null || System.currentTimeMillis() - lastSendTime > 30 * 60 * 1000) {
            send(this.phones, msg);
            messages.put(msg, System.currentTimeMillis());
        }
        List<String> needRemove = new ArrayList<String>();
        for (Map.Entry<String, Long> entry : messages.entrySet()) {
            Long lst = entry.getValue();
            if (lst == null || (System.currentTimeMillis() - lst > 24 * 60 * 60 * 1000)) {
                needRemove.add(entry.getKey());
            }
        }
        for (String key : needRemove) {
            messages.remove(key);
        }
    }

    @Override
    public void setPhones(String phones) {
        this.phones = phones;
    }

    @Override
    public String getPhones() {
        return phones;
    }

    protected abstract void send(String phones,
                                 String msg);
}
