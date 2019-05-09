package org.gy.framework.redis.support;

import java.util.List;

import redis.clients.jedis.HostAndPort;

public class MyHostAndPort extends HostAndPort {

    private static final long serialVersionUID = 5933521119118457907L;

    public MyHostAndPort(String host, int port) {
        super(host, port);
    }

    public static HostAndPort toHostAndPort(List<String> strList) {
        String host = strList.get(0);
        int port = Integer.parseInt(strList.get(1));
        return new MyHostAndPort(host, port);
    }

}
