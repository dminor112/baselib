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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;
/**
 * 
 * 
 * @author guoqingzhou
 * @date 2014-10-21
 */

public class ShardedJedisSentinelPool extends Pool<ShardedJedis> {

	public static final int MAX_RETRY_SENTINEL = 10;
	protected Logger log = LoggerFactory.getLogger(getClass().getName()); 
	
	protected Config poolConfig;

    protected int timeout = Protocol.DEFAULT_TIMEOUT; 

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;

    protected Set<ShardedMasterListener> masterListeners = new HashSet<ShardedMasterListener>(); 
    
    private  Hashing shardAlgo;  

    private volatile List<HostAndPort> currentHostList; //current shard host list
    private volatile List<JedisShardInfo> currentJedisShardInfoList;
    
    protected List<HostAndPort> shardMasters = new CopyOnWriteArrayList<HostAndPort>(); //first get
    protected List<HostAndPort> shardSlaves = new CopyOnWriteArrayList<HostAndPort>(); //first get
 
    private  List<HostAndPort> sentinelHostAndPort = new CopyOnWriteArrayList<HostAndPort>();
    
  

	public List<HostAndPort> getCurrentHostList() {
		return currentHostList;
	}
	
	public void setCurrentHostList(List<HostAndPort> currentHostList) {
		this.currentHostList = currentHostList;
	}
	
	public List<HostAndPort> getSentinelHostAndPort() {
		return sentinelHostAndPort;
	}
    

	public List<JedisShardInfo> getCurrentJedisShardInfoList() {
		return currentJedisShardInfoList;
	}

	public void setCurrentJedisShardInfoList(List<JedisShardInfo> currentJedisShardInfoList) {
		this.currentJedisShardInfoList = currentJedisShardInfoList;
	}

	public void setSentinelHostAndPort(List<HostAndPort> sentinelHostAndPort) {
		this.sentinelHostAndPort = sentinelHostAndPort;
	} 

	public ShardedJedisSentinelPool(final Map<String,List<String>> sentinels,final Config poolConfig ){
		this(sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE,Hashing.MURMUR_HASH);
    }
	
	public ShardedJedisSentinelPool(final Map<String,List<String>> sentinels,final Config poolConfig,Hashing algo) {
		this(sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE,algo);
    }
    
    public ShardedJedisSentinelPool(final Map<String,List<String>> sentinels, String password) {
		this(sentinels, new Config(),Protocol.DEFAULT_TIMEOUT, password,Protocol.DEFAULT_DATABASE,Hashing.MURMUR_HASH);
    } 

    public ShardedJedisSentinelPool(final Map<String,List<String>> sentinels,  final Config poolConfig, final int timeout,  final String password) {
		this(sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE,Hashing.MURMUR_HASH);
    }

    public ShardedJedisSentinelPool(final Map<String,List<String>> sentinels,  final Config poolConfig, final int timeout) {
		this(sentinels, poolConfig, timeout, null, Protocol.DEFAULT_DATABASE,Hashing.MURMUR_HASH);
    }

    public ShardedJedisSentinelPool(final Map<String,List<String>> sentinels, final Config poolConfig, final Hashing algo,final String password) {
		this(sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, password,Protocol.DEFAULT_DATABASE,algo);
    }

    public ShardedJedisSentinelPool(final Map<String,List<String>> sentinels,
	    final Config  poolConfig, int timeout,
	    final String password, final int database,Hashing algo) {
		this.poolConfig = poolConfig;
		this.timeout = timeout;
		this.password = password;
		this.database = database;
		this.shardAlgo = algo;
		this.sentinelHostAndPort = makeSentinelsList(sentinels);
		List<HostAndPort> masterList = initSentinels(sentinelHostAndPort);
		initPool(masterList);
    }

    public void destroy() {
      if (this.internalPool != null) {
            try {
                super.destroy();
            } catch (Exception e) {
            	log.error("destroy",e);
            }
        } 
    }
    
    public synchronized void close() {
        try {
            for (ShardedMasterListener m : masterListeners) {
                m.running.set(false);
                m.shutdown();
            }
            destroy();
        } catch (Exception e) {
            log.error("close ShardedJedisSentinelPool",e); 
        }
    }

    protected void initPool(List<HostAndPort> hostList) {  
 	    log.info("Create ShardedJedisPool List " +hostList.toString()); 
 		List<JedisShardInfo> shards = makeShardInfoList(hostList);
 		super.initPool(poolConfig, new ShardedJedisFactory(shards, this.shardAlgo, null));
 		currentHostList = hostList; 
 		currentJedisShardInfoList = shards;
   }
    private List<HostAndPort> makeSentinelsList(final Map<String,List<String>> sentinelMap) {  
    	 List<HostAndPort> sentinelHostAndPort2 = new CopyOnWriteArrayList<HostAndPort>();
    	 Iterator<Entry<String, List<String>>> iter = sentinelMap.entrySet().iterator(); 
 		 while (iter.hasNext()) {
 			Entry<String, List<String>> entry = (Entry<String, List<String>>) iter.next();
 			String name = entry.getKey();
 			List<String> sentinels = entry.getValue(); 
 			for (String sentinel : sentinels) {
 				List<String> item = Arrays.asList(sentinel.split(":"));
 				String host = item.get(0);
 		    	int port = Integer.parseInt(item.get(1));  
 		    	int weight = item.size() >2 ? Integer.parseInt(item.get(2)) : 1; 
 				HostAndPort hap  = new HostAndPort(host, port);
 				hap.setWeight(weight);
 				hap.setName(name);
 				sentinelHostAndPort2.add(hap); 
 			}
 		}
       return sentinelHostAndPort2;
    }
    
	private List<JedisShardInfo> makeShardInfoList(List<HostAndPort> hostList) {
		List<JedisShardInfo> shardList = new ArrayList<JedisShardInfo>();
		 
		for (HostAndPort hp : hostList) { 
			JedisShardInfo jedisShardInfo = new JedisShardInfo(hp.getHost(), hp.getPort(), timeout,hp.getWeight() );
			 
			if(password!=null){
			   jedisShardInfo.setPassword(password);
			} 
			shardList.add(jedisShardInfo);
		}
		log.info("Make jedisShardInfo List " +hostList.toString() +" OK!"); 
		return shardList;
	} 
 
   private List<HostAndPort> initSentinels(final List<HostAndPort> sentinelList) {  
	    log.info("init Sentinels , config Map " +sentinelList.toString()); 
	    HostAndPort master = null;
	    HostAndPort slave = null;  
    	int pos=-1; 
			
		log.info("Trying to find all master from available Sentinels... ");
		for (HostAndPort hap : sentinelList) {
			String masterName =  hap.getName();
		    int weight =hap.getWeight(); 
			log.info("Connecting to Sentinel " + hap);
			Jedis jedis = null;
			try {
				// pub/sub
				jedis = new Jedis(hap.getHost(), hap.getPort());

				// sentinel slaves mymaster
			    List<String> master_addr = null;
				List<Map<String, String>> slaves_addr = null;
				try {
					// redis 127.0.0.1:26381> sentinel get-master-addr-by-name mymaster
					master_addr = jedis.sentinelGetMasterAddrByName(masterName);
					slaves_addr = jedis.sentinelSlaves(masterName);
					//String strinfo=jedis.info("sentinel");
					//log.info(strinfo );
				 
				} catch (Exception e) {
					log.error(hap.toString() + " masterName " + masterName + " " + e.getMessage(), e);
				}
				// found master
				if (master_addr != null) {
					master = toHostAndPort(master_addr);
					master.setName(masterName);
					master.setWeight(weight);
					log.info("Found Redis master at " + master + ", masterName " + masterName + " OK!");
					pos = hostIndexOf(shardMasters, master);
					if (pos < 0) {
						shardMasters.add(master);
					}
				}
				// found slaves
				if (slaves_addr != null && slaves_addr.size() > 0) {
					List<HostAndPort> shardSlaves2 = toSlavesHostAndPort(slaves_addr, masterName,weight, shardSlaves);  
					slave = getRandomSlave(shardSlaves2);
					// meger shard
					if (slave == null) {
						slave = master; 
						pos = hostIndexOf(shardSlaves, slave);
						if (pos < 0) {
							shardSlaves.add(slave);
						}
					} 
				}
				 
			} catch (JedisConnectionException e) {
				log.error("Cannot connect to sentinel running @ " + hap
						+ ". Trying next one.");
			} finally {
				try {
					if (jedis != null) {
						jedis.disconnect();
					}
				} catch (Exception e) {
					log.error(e.getMessage(), e);
				}
			}
		}
		 
        
        log.info("Starting Sentinel listeners sentinelHostAndPort list "+sentinelList.toString());
	    log.info("Found all shardMaster list "+shardMasters.toString());
	    log.info("Found all shardSlave list "+shardSlaves.toString());  
		for (final HostAndPort hap : sentinelList) { 
		    ShardedMasterListener shardMasterListener = new ShardedMasterListener( hap.getHost(), hap.getPort(),hap.getName(),hap.getWeight());
		    masterListeners.add(shardMasterListener);
		    shardMasterListener.start();
		} 
		log.info("Sentinel work on "+getClass().getSimpleName() +" OK!");  
		return shardMasters;
    }
   
	/**
	 * create master jedis hostandport
	 * @param getMasterAddrByNameResult
	 * @return
	 */
	protected  HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
	    	String host = getMasterAddrByNameResult.get(0);
	    	int port = Integer.parseInt(getMasterAddrByNameResult.get(1)); 
	    	return new HostAndPort(host, port);
	}
	/**
	 * 
	 * @param host
	 * @param port
	 * @param clustername
	 * @return
	 */
	protected HostAndPort toHostAndPort(String host, String port,String clustername) {
	    final HostAndPort hap = new HostAndPort(host,port,clustername); 
	    return hap;
	}
	/**
	 * create slaves jedis list hostandport
	 * @param ss
	 * @param clustername
	 * @return
	 */
	protected List<HostAndPort> toSlavesHostAndPort(List<Map<String, String>> ss,String clustername,int weight,List<HostAndPort> shardSlaves) {
		     if(shardSlaves==null){
		        shardSlaves = new CopyOnWriteArrayList<HostAndPort>();
		     }
	         HostAndPort slave = null;
	         if (ss != null && ss.size() > 0) {
	            for (int i = 0; i < ss.size(); i++) {
	                Map<String, String> ms = ss.get(i);
	                String ip = ms.get("ip");
	                String port = ms.get("port");
	                String flag = ms.get("flags");
	                log.info("Found Redis slave at " + ip + ":" + port + " ,flags is " + flag +" ,clustername is "+clustername+" OK!");
	                if ("slave".equals(flag)) {
	                    slave = new HostAndPort(ip,port); 
	                    slave.setName(clustername);
	                    slave.setWeight(weight);  
	                    if (slave != null ) {
	                    	int pos=shardSlaves.indexOf(slave);
	                    	if(pos<0){
	                    		shardSlaves.add(slave);
	                    	}
	                    }
	                }
	            }
	        } 
	        return shardSlaves;
	}
	/**
	 * 
	 * @param shp
	 * @return
	 */
	protected HostAndPort getRandomSlave(List<HostAndPort> shp) {
	    HostAndPort slave = null;  
	    try{
	        if (shp != null && shp.size() > 0) {
	            //随机取一个
	            Random ran = new Random();
	            int ad = ran.nextInt(shp.size()); 
	            slave =(shp.get(ad)!=null)?shp.get(ad):shp.get(0);
	        } 
	    }catch(Exception e){
	    	e.printStackTrace();
	    }
	    return slave;
	}
	/**
	 * 
	 * @param hostlist
	 * @param hap
	 * @return
	 */
	protected static int hostIndexOf(List<HostAndPort> hostlist,HostAndPort hap) {
		int index=-1;
		if(hostlist==null) return index;
		if(hap==null) return index;
		for (int i=0;i<hostlist.size();i++) {
			HostAndPort obj=hostlist.get(i);
			if(hap.equals(obj)){
				index=i;
			}
		}
		return index;
	}
		
	 
	protected class ShardedMasterListener extends Thread  { 
		protected String host;
		protected int port;
		protected int weight;
		protected String clusterName; //jedis cluster name
		protected long subscribeRetryWaitTimeMillis = 3000;
		protected Jedis jedis;
		protected AtomicBoolean running = new AtomicBoolean(false);
	 

		public ShardedMasterListener( String host, int port,String name,int weight) { 
		    this.host = host;
		    this.port = port;
		    this.weight =weight;
		    this.clusterName = name;
		}

		public ShardedMasterListener( String host, int port,String name, int weight, long subscribeRetryWaitTimeMillis) {
		    this(host, port,name,weight);
		    this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
		}

		public void run() {

		    running.set(true);

		   while (running.get()) {
		       log.info("Thread Listener Sentinel pub/sub [" + host+":"+port +"] clusterName "+clusterName);
			   jedis = new Jedis(host, port);  // Sentinel pub/sub

			 try {
			    jedis.subscribe(new JedisPubSubAdapter() {
					@Override
					public void onMessage(String channel, String message) {
						String[] switchMasterMsg = message.split(" ");
                    	List<HostAndPort> newhostList = new CopyOnWriteArrayList<HostAndPort>(getCurrentHostList());
                    	int index = -1;
                    	boolean rebuild=false;
                        log.info("Sentinel " + host + ":" + port + " published channel:" + channel + " message: "
                                + message + " " + " length:" + switchMasterMsg.length + " clusterName "+clusterName );
                        // message: <instance-type> <name> <ip> <port> @
                        // <master-name> <master-ip> <master-port>
                        // message: slave 10.10.52.187:6379 10.10.52.187
                        // 6379 @ mymaster 10.10.52.187 7379 
                        if ("+switch-master".equals(channel) && switchMasterMsg.length>3 ) { 
					     		String megMasterName=switchMasterMsg[0];
					     		log.info("clusterName "+clusterName +" "+megMasterName);
						    	HostAndPort downMaster = toHostAndPort(Arrays.asList(switchMasterMsg[1], switchMasterMsg[2]));   //isdown  
						    	HostAndPort newHostMaster = toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]));
						    	downMaster.setName(clusterName);
						    	downMaster.setVlid(0); //0 is down,1 is ok
						    	downMaster.setWeight(weight); //weight
						    	newHostMaster.setName(clusterName);
						    	newHostMaster.setVlid(1);
						    	newHostMaster.setWeight(weight); //weight
						    	 
						    	try{  
						    		index = hostIndexOf(newhostList, downMaster);
						    		if(index >= 0 && newhostList.size() > index){
						    			rebuild=true;
						    		   //downhost exists  list ，remove
						    		   newhostList.remove(index); 
						    		}
						    		index = hostIndexOf(newhostList, newHostMaster);
						    		if(index<0){
						    			//newhost  not exists list ，add
						    			newhostList.add(newHostMaster);
						    		}
						    		if (rebuild && clusterName.equals(megMasterName) && newhostList.size() > 0) { 
                                      initPool(newhostList);
                                    }
						    	 
						    	}catch(Exception e){
//						    		log.error(ExceptionUtils.getFullStackTrace(e));
						    	}
                        } 
                        log.warn("CurrentHostList "+getCurrentHostList().toString());
                        log.warn("NewhostList "+newhostList.toString());
					}
			    }, "+switch-master");

			} catch (JedisConnectionException e) { 
				    if (running.get()) {
						log.warn("Lost connection to Sentinel at " + host
							+ ":" + port
							+ ". Sleeping 3000ms and retrying.");
						try {
						    Thread.sleep(subscribeRetryWaitTimeMillis);
						} catch (InterruptedException e1) {
//							log.error(ExceptionUtils.getFullStackTrace(e1));
						}
				    } else {
						log.warn("Unsubscribing from Sentinel at " + host + ":" + port);
				    }
			   }finally{
				   log.info("finally subscribing at " + host + ":" + port);
			   }
		    } 
		} //end run

		public void shutdown() {
		    try {
				log.warn("Shutting down listener on " + host + ":" + port); 
		    } catch (Exception e) {
//	            log.error(ExceptionUtils.getFullStackTrace(e));
	        } finally {
	            if (jedis != null) {
	            	jedis.disconnect();
	            }
	        }
		}
		
		

	}

 
    
    public static void testRun(){

    	Config config = new Config();
  		
  	    List<String> sent_list = new CopyOnWriteArrayList<String>();
		sent_list.add("10.10.22.26:26379");
		sent_list.add("10.10.53.67:26379");
		
		Map<String,List<String>> sentinels = new ConcurrentHashMap<String,List<String>>();
		sentinels.put("redisCluster", sent_list);
     
  		ShardedJedisSentinelPool pool = new ShardedJedisSentinelPool(sentinels, config);
  		
  		ShardedJedis jedis = null;
  		String key="mytest";
  		String value ="testshard_"+System.currentTimeMillis();
  		try {
  			jedis = pool.getResource();
  			jedis.set(key, value);  
  		} finally {
  			if (jedis != null) pool.returnResource(jedis);
  			pool.destroy();
  		}
     
    }
    
    public static void main(String args[]){
    	testRun();
    }
} 