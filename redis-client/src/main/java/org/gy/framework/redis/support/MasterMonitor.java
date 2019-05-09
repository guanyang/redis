package org.gy.framework.redis.support;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class MasterMonitor {

    private static final Logger                          logger           = LoggerFactory.getLogger(MasterMonitor.class);

    private static final MasterMonitor                   INSTANCE         = new MasterMonitor();

    private Map<HostAndPort, MonitorThreadListenersPair> monitorListeners = new ConcurrentHashMap<HostAndPort, MonitorThreadListenersPair>();

    private class MonitorThreadListenersPair {

        private MonitorThread       thread;

        private Set<MasterListener> listeners = new CopyOnWriteArraySet<MasterListener>();

        private MonitorThreadListenersPair(HostAndPort sentinelHostAndPort) {
            this.thread = new MonitorThread(sentinelHostAndPort);
            this.thread.setDaemon(true);
            this.thread.start();
        }
    }

    public static MasterMonitor getInstance() {
        return INSTANCE;
    }

    public synchronized void monitor(HostAndPort sentinelHostAndPort,
                                     MasterListener masterListener) {
        MonitorThreadListenersPair pair = monitorListeners.get(sentinelHostAndPort);
        if (pair == null) {
            pair = new MonitorThreadListenersPair(sentinelHostAndPort);
            monitorListeners.put(sentinelHostAndPort, pair);
        }
        pair.listeners.add(masterListener);
    }

    public synchronized void unMonitor(HostAndPort sentinelHostAndPort,
                                       MasterListener masterListener) {
        MonitorThreadListenersPair pair = monitorListeners.get(sentinelHostAndPort);
        if (pair != null) {
            pair.listeners.remove(masterListener);
            if (pair.listeners.isEmpty()) {
                pair.thread.shutdown();
                monitorListeners.remove(sentinelHostAndPort);
            }
        }
    }

    public interface MasterListener {

        public void masterChanged(String masterName,
                                  HostAndPort newMaster);

    }

    protected class JedisPubSubAdapter extends JedisPubSub {
        @Override
        public void onMessage(String channel,
                              String message) {
        }

        @Override
        public void onPMessage(String pattern,
                               String channel,
                               String message) {
        }

        @Override
        public void onPSubscribe(String pattern,
                                 int subscribedChannels) {
        }

        @Override
        public void onPUnsubscribe(String pattern,
                                   int subscribedChannels) {
        }

        @Override
        public void onSubscribe(String channel,
                                int subscribedChannels) {
        }

        @Override
        public void onUnsubscribe(String channel,
                                  int subscribedChannels) {
        }
    }

    protected class MonitorThread extends Thread {

        protected HostAndPort   hap;

        protected long          subscribeRetryWaitTimeMillis = 5000;

        protected Jedis         j;

        protected AtomicBoolean running                      = new AtomicBoolean(false);

        public MonitorThread(HostAndPort sentinelHostAndPort) {
            this.hap = sentinelHostAndPort;
        }

        public void run() {
            running.set(true);
            while (running.get()) {
                j = new Jedis(hap.getHost(), hap.getPort());
                try {
                    j.subscribe(new JedisPubSubAdapter() {
                        @Override
                        public void onMessage(String channel,
                                              String message) {
                            logger.info("Sentinel " + hap.getHost() + ":" + hap.getPort() + " published: " + message + ".");
                            String[] switchMasterMsg = message.split(" ");
                            if (switchMasterMsg.length > 3) {
                                String name = switchMasterMsg[0];
                                HostAndPort newMaster = MyHostAndPort.toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]));
                                MonitorThreadListenersPair pair = monitorListeners.get(hap);
                                if (pair != null) {
                                    for (MasterListener listener : pair.listeners) {
                                        listener.masterChanged(name, newMaster);
                                    }
                                }
                            } else {
                                logger.error("Invalid message received on Sentinel " + hap.getHost() + ":" + hap.getPort() + " on channel +switch-master: " + message);
                            }
                        }
                    }, "+switch-master");

                } catch (Throwable e) {
                    if (j != null && j.isConnected()) {
                        try {
                            try {
                                j.quit();
                            } catch (Throwable ex) {
                                // ignore
                            }
                            j.disconnect();
                        } catch (Throwable ex) {
                            // ignore
                        }
                    }
                    if (running.get()) {
                        logger.error("Lost connection to Sentinel at " + hap.getHost() + ":" + hap.getPort() + ". Sleeping 5000ms and retrying.", e);
                        try {
                            Thread.sleep(subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } else {
                        logger.info("Unsubscribing from Sentinel at " + hap.getHost() + ":" + hap.getPort());
                    }
                }
            }
        }

        public void shutdown() {
            try {
                logger.info("Shutting down listener on " + hap.getHost() + ":" + hap.getPort());
                running.set(false);
                // This isn't good, the Jedis object is not thread safe
                j.disconnect();
            } catch (Throwable e) {
                logger.error("Caught exception while shutting down: " + e.getMessage());
            }
        }
    }

}
