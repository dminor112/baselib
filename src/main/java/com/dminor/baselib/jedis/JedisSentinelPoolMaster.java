package com.dminor.baselib.jedis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set; 
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
 * @author guoqingzhou usger
 * @Autowired private JedisSentinelPool pool;
 *            <p>
 *            public void mymethod() { Jedis jedis = null; try { jedis =
 *            pool.getResource(); jedis.hset(.... } catch (JedisException je) {
 *            throw je; } finally { if (jedis != null)
 *            pool.returnResource(jedis); } } As I am using Spring, you need:
 *            <bean id="redisSentinel"
 *            class="redis.clients.jedis.JedisSentinelPoolMaster">
 *            <constructor-arg index="0" value="mymaster" /> <constructor-arg
 *            index="1"> <set> <value>10.10.52.187:26379</value> </set>
 *            </constructor-arg> <constructor-arg index="2"
 *            ref="jedisPoolConfig"/> </bean>
 *            </p>
 *            redis 127.0.0.1:26381> sentinel masters
 */
public class JedisSentinelPoolMaster extends Pool<Jedis> {

    protected Config poolConfig;

    protected int timeout = Protocol.DEFAULT_TIMEOUT;

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;

    protected Set<MasterListener> masterListeners = new HashSet<MasterListener>();
 

    private volatile HostAndPort currentHostMaster;

    protected Logger log = LoggerFactory.getLogger(getClass().getName());

    public JedisSentinelPoolMaster(String masterName, Set<String> sentinels,
            final Config poolConfig) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolMaster(String masterName, Set<String> sentinels) {
        this(masterName, sentinels, new Config(), Protocol.DEFAULT_TIMEOUT,
                null, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolMaster(String masterName, Set<String> sentinels,
            String password) {
        this(masterName, sentinels, new Config(), Protocol.DEFAULT_TIMEOUT,
                password);
    }

    public JedisSentinelPoolMaster(String masterName, Set<String> sentinels,
            final Config poolConfig, int timeout, final String password) {
        this(masterName, sentinels, poolConfig, timeout, password,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolMaster(String masterName, Set<String> sentinels,
            final Config poolConfig, final int timeout) {
        this(masterName, sentinels, poolConfig, timeout, null,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolMaster(String masterName, Set<String> sentinels,
            final Config poolConfig, final String password) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT,
                password);
    }

    public JedisSentinelPoolMaster(String masterName, Set<String> sentinels,
            final Config poolConfig, int timeout, final String password,
            final int database) { 
        this.poolConfig = poolConfig;
        this.timeout = timeout;
        this.password = password;
        this.database = database;

        HostAndPort master = initSentinels(sentinels, masterName);
        initPool(master);
    } 
     
   
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
            for (MasterListener m : masterListeners) {
                m.running.set(false);
                m.shutdown();
            }
            destroy();
        } catch (Exception e) {
            log.error("close master JedisPool",e); 
        }
    }

    public HostAndPort getCurrentHostMaster() {
        return currentHostMaster;
    }

    private void initPool(HostAndPort master) {
        if (!master.equals(currentHostMaster)) {
            currentHostMaster = master;
            log.warn("Created JedisPool to master at " + master);
            super.initPool(poolConfig, new JedisFactory(master.host, master.port,
                    timeout, password, database));
        }
    }

    private HostAndPort initSentinels(Set<String> sentinels,
            final String masterName) {

        HostAndPort master = null;
        boolean running = true;

        outer: while (running) {
            log.warn("sentinels size "+sentinels.size()+ ", Trying to find master from available Sentinels...");
            for (String sentinel : sentinels) {
                final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
                log.warn("Connecting to Sentinel " + hap);
                try {
                    Jedis jedis = new Jedis(hap.host, hap.port); 
                    if (master == null) {
                        master = toHostAndPort(jedis.sentinelGetMasterAddrByName(masterName)); 
                        log.warn("Found Redis master at " + master);
                        jedis.disconnect();
                        break outer;
                    }
                } catch (JedisConnectionException e) {
                    log.warn("Cannot connect to sentinel running @ " + hap
                            + ". Trying next one.");
                }
            }

            try {
                log.warn("All sentinels down, cannot determine where is "
                        + masterName + " master is running... sleeping 1000ms.");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        log.warn("Redis master running at " + master  + ", starting Sentinel listeners...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
            MasterListener masterListener = new MasterListener(masterName, hap.host, hap.port);
            masterListeners.add(masterListener);
            masterListener.start();
        }
        
        return master;
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        final HostAndPort hap = new HostAndPort();
        hap.host = getMasterAddrByNameResult.get(0);
        hap.port = Integer.parseInt(getMasterAddrByNameResult.get(1));
        return hap;
    }

    protected class MasterListener extends Thread {

        protected String masterName;
        protected String host;
        protected int port;
        protected long subscribeRetryWaitTimeMillis = 3000;
        protected Jedis j;
        protected AtomicBoolean running = new AtomicBoolean(false);

        protected MasterListener() {
        }

        public MasterListener(String masterName, String host, int port) {
            this.masterName = masterName;
            this.host = host;
            this.port = port;
        }

        public MasterListener(String masterName, String host, int port,
                long subscribeRetryWaitTimeMillis) {
            this(masterName, host, port);
            this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        }

        public void run() {

            running.set(true);

            while (running.get()) {
                j = new Jedis(host, port); // Sentinel pub/sub
                try {
                    j.subscribe(new JedisPubSubAdapter() {
                        @Override
                        public void onMessage(String channel, String message) {
                            // massage: <master name> <oldip> <oldport> <newip>
                            // <newport>
                            String[] switchMasterMsg = message.split(" ");
                            log.warn("Sentinel " + host + ":" + port + " published channel:" + channel + " message: "
                                    + message + " message length:" + switchMasterMsg.length);
                            if (switchMasterMsg.length > 3) {
                               // HostAndPort downMaster = toHostAndPort(Arrays.asList(switchMasterMsg[1], switchMasterMsg[2]));
                                HostAndPort newMaster = toHostAndPort(Arrays.asList(switchMasterMsg[3],
                                        switchMasterMsg[4])); 
                                if (masterName.equals(switchMasterMsg[0])) {
                                    initPool(newMaster);
                                }
                            }
                        }
                    }, "+switch-master");

                } catch (JedisConnectionException e) { 
                    if (running.get()) {
                        log.error("Lost connection to Sentinel at " + host
                                + ":" + port
                                + ". Sleeping 3000ms and retrying.");
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