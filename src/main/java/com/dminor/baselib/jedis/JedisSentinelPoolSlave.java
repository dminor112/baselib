package com.dminor.baselib.jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.exception.ExceptionUtils; 
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

/**
 * ip not user 127.0.0.1
 * 
 * @author guoqingzhou redis 127.0.0.1:26381> sentinel slaves mymaster
 */
public class JedisSentinelPoolSlave extends Pool<Jedis> {

    protected Config poolConfig;

    protected int timeout = Protocol.DEFAULT_TIMEOUT;

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;
    protected boolean closed;

    protected Set<SlaveListener> slaveListeners = new HashSet<SlaveListener>();
    protected ConcurrentHashMap<String, Integer> slavesHostAndPort = new ConcurrentHashMap<String, Integer>();

    protected Logger log = LoggerFactory.getLogger(getClass().getName());

    public JedisSentinelPoolSlave(String masterName, Set<String> sentinels,
            final Config poolConfig) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolSlave(String masterName, Set<String> sentinels) {
        this(masterName, sentinels, new Config(), Protocol.DEFAULT_TIMEOUT,
                null, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolSlave(String masterName, Set<String> sentinels,
            String password) {
        this(masterName, sentinels, new Config(), Protocol.DEFAULT_TIMEOUT,
                password);
    }

    public JedisSentinelPoolSlave(String masterName, Set<String> sentinels,
            final Config poolConfig, int timeout, final String password) {
        this(masterName, sentinels, poolConfig, timeout, password,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolSlave(String masterName, Set<String> sentinels,
            final Config poolConfig, final int timeout) {
        this(masterName, sentinels, poolConfig, timeout, null,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolSlave(String masterName, Set<String> sentinels,
            final Config poolConfig, final String password) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT,
                password);
    }

    public JedisSentinelPoolSlave(String masterName, Set<String> sentinels,
            final Config poolConfig, int timeout, final String password,
            final int database) {
        this.poolConfig = poolConfig;
        this.timeout = timeout;
        this.password = password;
        this.database = database; 

        HostAndPort master = initSentinels(sentinels, masterName);
        initPool(master);
    }
 

    private volatile HostAndPort currentHostSlave;
     
    public void destroy() {
        if (this.internalPool != null) {
            try {
                super.destroy();
            } catch (Exception e) {
            }
        }
    }

    public synchronized void close() {
        try {
            closed = true;
            for (SlaveListener m : slaveListeners) {
                m.running.set(false);
                m.shutdown();
            }
            destroy();
        } catch (Exception e) {
            log.error("close slave JedisPool",e); 
        }
    }

    public HostAndPort getCurrentHostSlave() {
        return currentHostSlave;
    }

    private void initPool(HostAndPort slave) {
        if (slave != null) {
            currentHostSlave = slave;
            log.warn("Created JedisPool to slave at " + slave +",slaves:"+slavesHostAndPort.size());
            super.initPool(poolConfig, new JedisFactory(slave.host, slave.port, timeout, password, database));
        }
    }

    private void findSentinelSlaves(List<Map<String, String>> ss) {
        HostAndPort slave = null;
        if (ss != null && ss.size() > 0) {
            for (int i = 0; i < ss.size(); i++) {
                Map<String, String> ms = ss.get(i);
                String ip = ms.get("ip");
                String port = ms.get("port");
                String flag = ms.get("flags");
                log.warn("redis slave : " + ip + ":" + port + " ,flags " + flag);
                if ("slave".equals(flag)) {
                    slave = toHostAndPort(ip, port);
                    if (slave != null) {
                        slavesHostAndPort.put(slave.toString(), 1);
                    }
                }
            }
        }
    }

    private HostAndPort getSentinelSlave() {
        HostAndPort slave = null;
        List<HostAndPort> shp = new ArrayList<HostAndPort>();
        Iterator<Entry<String, Integer>> iter = slavesHostAndPort.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, Integer> entry = (Entry<String, Integer>) iter.next();
            String k=entry.getKey();
            Integer v = entry.getValue();
            HostAndPort sp =toHostAndPort(Arrays.asList(k.split(":"))); 
            if (v >= 1) {
                shp.add(sp);
            }
        }

        if (shp != null && shp.size() > 0) {
            //随机取一个
            Random ran = new Random();
            int ad = ran.nextInt(shp.size());
            int ad2 =0;
            slave =(shp.get(ad)!=null)?shp.get(ad):shp.get(ad2);
        }
        
        return slave;
    }

    private HostAndPort initSentinels(Set<String> sentinels, final String masterName) {

        HostAndPort slave = null;
        HostAndPort master = null;
        boolean running = true;
        outer: while (running) {
            log.warn("sentinels size "+sentinels.size()+ ", Trying to find slave from available Sentinels...");
            for (String sentinel : sentinels) {
                final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
                log.warn("Connecting to Sentinel " + hap);
                try {
                    Jedis jedis = new Jedis(hap.host, hap.port); 
                    if (slave == null || master==null) {
                        master = toHostAndPort(jedis.sentinelGetMasterAddrByName(masterName));
                        List<Map<String, String>> ss = jedis.sentinelSlaves(masterName);
                        findSentinelSlaves(ss);
                        slave = getSentinelSlave();
                        if (slave == null) {
                            slave = master;
                        }
                        log.warn("Found Redis slaves: " + ss.size() + ", now use at slave " + slave);
                        jedis.disconnect();
                        break outer;
                    }
                } catch (JedisConnectionException e) {
                    log.error("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
                }
            }

            try {
                log.error("All sentinels down, cannot determine where is "   + masterName + " master is running... sleeping 1000ms.");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        log.warn("Redis Slave running at " + slave + ", starting Sentinel listeners...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
            SlaveListener slaveListener = new SlaveListener(masterName, hap.host, hap.port);
            slaveListeners.add(slaveListener);
            slaveListener.start();
        }
         
        return slave;
    }

    private HostAndPort toHostAndPort(List<String> getSalveResult) {
        final HostAndPort hap = new HostAndPort();
        hap.host = getSalveResult.get(0);
        hap.port = Integer.parseInt(getSalveResult.get(1));
        return hap;
    }

    private HostAndPort toHostAndPort(String host, String port) {
        final HostAndPort hap = new HostAndPort();
        hap.host = host;
        hap.port = Integer.parseInt(port);
        return hap;
    }

    protected class SlaveListener extends Thread {

        protected String masterName;
        protected String host;
        protected int port;
        protected long subscribeRetryWaitTimeMillis = 3000;
        protected Jedis j;
        protected AtomicBoolean running = new AtomicBoolean(false);

        protected SlaveListener() {
        }

        public SlaveListener(String masterName, String host, int port) {
            this.masterName = masterName;
            this.host = host;
            this.port = port;
        }

        public SlaveListener(String masterName, String host, int port,
                long subscribeRetryWaitTimeMillis) {
            this(masterName, host, port);
            this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        }

        public void run() {
            running.set(true);
            while (running.get()) {
                j = new Jedis(host, port);
                try {
                    j.subscribe(new JedisPubSubAdapter() {
                        @Override
                        public void onMessage(String channel, String message) {
                            String[] switchMasterMsg = message.split(" ");
                            log.warn("Sentinel " + host + ":" + port + " published channel:" + channel + " message: "
                                    + message + " " + " length:" + switchMasterMsg.length +" slaves size:"+slavesHostAndPort.size());
                            // message: <instance-type> <name> <ip> <port> @
                            // <master-name> <master-ip> <master-port>
                            // message: slave 10.10.52.187:6379 10.10.52.187
                            // 6379 @ mymaster 10.10.52.187 7379 
                            if ("+switch-master".equals(channel) && switchMasterMsg.length>3 ) {
                                //HostAndPort downMaster = toHostAndPort(Arrays.asList(switchMasterMsg[1], switchMasterMsg[2]));
                                HostAndPort newMaster = toHostAndPort(Arrays.asList(switchMasterMsg[3],switchMasterMsg[4]));
                                if (masterName.equals(switchMasterMsg[0])) {
                                    initPool(newMaster);
                                }
                            } 
                                
                            if (switchMasterMsg.length > 7 && "slave".equals(switchMasterMsg[0])
                                    && masterName.equals(switchMasterMsg[5])) {
                                 if ("+convert-to-slave".equals(channel)) {
                                    //原来主变从
                                     HostAndPort addslave = toHostAndPort(Arrays.asList(switchMasterMsg[2], switchMasterMsg[3]));
                                     HostAndPort curmaster = toHostAndPort(Arrays.asList(switchMasterMsg[6],switchMasterMsg[7]));
                                    slavesHostAndPort.put(addslave.toString(), 1);
                                    slavesHostAndPort.remove(curmaster.toString());  
                                    if (curmaster.equals(currentHostSlave)) {
                                        HostAndPort nslave = getSentinelSlave();
                                        nslave= (nslave != null )? nslave : addslave; 
                                        //当前的slave为master的话就切换一下
                                        initPool(nslave);
                                    }

                                } else if (("+sdown".equals(channel) || "+odown".equals(channel))) {
                                   //slave挂了
                                    HostAndPort downslave = toHostAndPort(Arrays.asList(switchMasterMsg[2], switchMasterMsg[3]));
                                    HostAndPort curmaster = toHostAndPort(Arrays.asList(switchMasterMsg[6],switchMasterMsg[7]));
                                    slavesHostAndPort.remove(downslave.toString());
                                    if(slavesHostAndPort.size()<1){
                                        //空池就把master加入
                                        slavesHostAndPort.put(curmaster.toString(),1);
                                    }
                                    if (downslave.equals(currentHostSlave)) { 
                                        // 当前slave挂了
                                        HostAndPort nslave = getSentinelSlave();
                                        nslave= (nslave != null )? nslave : curmaster;  
                                        initPool(nslave);
                                    }
                                } else if ("+reboot".equals(channel) ) {
                                    HostAndPort addslave = toHostAndPort(Arrays.asList(switchMasterMsg[2], switchMasterMsg[3]));
                                    HostAndPort curmaster = toHostAndPort(Arrays.asList(switchMasterMsg[6],switchMasterMsg[7]));
                                    slavesHostAndPort.put(addslave.toString(), 1); 
                                    slavesHostAndPort.remove(curmaster.toString());  
                                    if (curmaster.equals(currentHostSlave)) {
                                        //当前在用master 
                                        HostAndPort nslave = getSentinelSlave();
                                        nslave= (nslave != null )? nslave : curmaster; 
                                        initPool(nslave);
                                    } 
                                } 
                            } 
                            // end fun
                        }
                    }, "+switch-master","+convert-to-slave", "+sdown", "+odown", "+reboot");

                } catch (JedisConnectionException e) { 
                    if (running.get()) {
                        log.error("Lost connection to Sentinel at " + host + ":" + port  + ". Sleeping 3000ms and retrying.");
                        try {
                            Thread.sleep(subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException e1) {
//                            log.error(ExceptionUtils.getFullStackTrace(e));
                        }
                    }
                    
                } finally {
                    if (j != null) {
                        j.disconnect();
                    }
                }

            }
        }
 
        public void shutdown() {
            try {
                log.warn("Shutting down listener on " + host + ":" + port); 
            } catch (Exception e) {
//                log.error(ExceptionUtils.getFullStackTrace(e));
            } finally {
                if (j != null) {
                    j.disconnect();
                }
            }
        }
    }
}