package org.gy.framework.redis.support;

import redis.clients.jedis.HostAndPort;

public interface MasterChangedListener {

    public void onMasterChanged(HostAndPort oldHostPort,
                                HostAndPort newHostPort);

}
