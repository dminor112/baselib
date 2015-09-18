package com.dminor.baselib.jedis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.Tuple;
import redis.clients.util.Pool;
import redis.clients.jedis.ShardedJedisPool;

import com.alibaba.fastjson.JSON;
import com.sohu.tv.m.cache.jedis.JedisCache;
import com.sohu.tv.m.cache.util.SerializationUtil;


public abstract class AbstractJedisCache  implements JedisCache {  

	private static final Logger logger = LoggerFactory.getLogger(AbstractJedisCache.class);   
 
	// ShardedJedisPool 
    protected  Pool<ShardedJedis>  shardedJedisPoolWrite;  //ShardedJedisSentinelPoolMaster  
    
    //JedisPool 
    protected   Pool<Jedis> jedisPoolWrite;  //JedisSentinelPoolMaster 
    protected   Pool<Jedis> jedisPoolRead;  //JedisSentinelPoolSlave 
	
    
    
    public AbstractJedisCache(){}
    
    public AbstractJedisCache(JedisSentinelPoolMaster sentinelMaster, JedisSentinelPoolSlave sentinelSlave){
        jedisPoolWrite =sentinelMaster; 
        jedisPoolRead = sentinelSlave;  
        logger.info("init JedisSentinelPool");
   }  
    
    public AbstractJedisCache(JedisPool master, JedisPool slave){  
        jedisPoolWrite = master; 
        jedisPoolRead  = slave;
        logger.info("init JedisPool");
    }  
    
    public AbstractJedisCache(ShardedJedisPool master){ 
         shardedJedisPoolWrite = master; 
        logger.info("init ShardedJedisPool  "); 
    }  
    
    public AbstractJedisCache(ShardedJedisSentinelPool poolMaster){
        shardedJedisPoolWrite= poolMaster; 
        logger.info("init ShardedJedisSentinelPool "); 
    }
 
    public Pool<ShardedJedis> getShardedJedisPoolWrite() {
		return shardedJedisPoolWrite;
	}

	public void setShardedJedisPoolWrite(Pool<ShardedJedis> shardedJedisPoolWrite) {
		this.shardedJedisPoolWrite = shardedJedisPoolWrite;
	}

	public Pool<Jedis> getJedisPoolWrite() {
		return jedisPoolWrite;
	}

	public void setJedisPoolWrite(Pool<Jedis> jedisPoolWrite) {
		this.jedisPoolWrite = jedisPoolWrite;
	}

	public Pool<Jedis> getJedisPoolRead() {
		return jedisPoolRead;
	}

	public void setJedisPoolRead(Pool<Jedis> jedisPoolRead) {
		this.jedisPoolRead = jedisPoolRead;
	}

	public void putString(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = ((Pool<Jedis>) jedisPoolWrite).getResource();
            jedis.set(key, value);  
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite &&  null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        }
    } 

    
    public String jedisInfo() {
        Jedis jedis = null;
        String info="";
        try {
            jedis = ((Pool<Jedis>) jedisPoolWrite).getResource();
            info=jedis.info(); 
            logger.warn("test key:"+jedis.get("test"));
            jedisPoolWrite.returnResource(jedis); 
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite &&  null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        }
        return info;
    }

    public void putString(String key, String value,int seconds) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            jedis.set(key, value); 
            jedis.expire(key, seconds);// 设置过期时间
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }
    public void put(String key, Serializable value) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            if(value instanceof String){
                jedis.set(key, (String) value);
            }else{
                jedis.set(key.getBytes(), SerializationUtil.object2Bytes(value));
            }
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        }  
    }
    public void put(String key, Serializable value,int seconds) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            if(value instanceof String){
                jedis.set(key, (String) value);
                jedis.expire(key, seconds);// 设置过期时间
            }else{
                jedis.set(key.getBytes(), SerializationUtil.object2Bytes(value));
                jedis.expire(key.getBytes(), seconds);// 设置过期时间
            } 
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }
    
    /**
     *Serialization   object2Bytes
     */
     
    public void putBytes(String key, Serializable value) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            jedis.set(key.getBytes(), SerializationUtil.object2Bytes(value));
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }

    /**
      *Serialization   object2Bytes
      */
    public void putBytes(String key, Serializable value, int seconds) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            jedis.set(key.getBytes(), SerializationUtil.object2Bytes(value));
            jedis.expire(key.getBytes(), seconds);// 设置过期时间
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }

    public void putJson(String key, Object value) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            jedis.set(key, JSON.toJSONString(value));
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        }  
    }

    public void putJson(String key, Object value, int seconds) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            jedis.set(key, JSON.toJSONString(value));
            jedis.expire(key, seconds);// 设置过期时间
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        }  
    }
    
    public void expireKey(String key, int seconds){
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource(); 
            jedis.expire(key, seconds);// 设置过期时间
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }
    
    public void remove(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            jedis.del(key); 
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            }
        }  
    }
    
    public String getString(String key) {
        Jedis jedis = null;
        String value=null;
        try {
            jedis = jedisPoolRead.getResource();
            value = jedis.get(key); 
            jedisPoolRead.returnResource(jedis); 
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null !=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        }  
        return value;
    }
   
    public Object get(String key) {
        return getObject(key);
    }
    
  
    @SuppressWarnings("unchecked")
    public <T> T get(byte[] key,Class<T> classOfT) {
        Jedis jedis = null; 
        T obj=null;
        try {
            jedis = jedisPoolRead.getResource();         
            byte[] bt = jedis.get(key);
            if(bt!=null && bt.length > 0 ){
                obj = (T) SerializationUtil.bytes2Object(bt);
            }
            jedisPoolRead.returnResource(jedis); 
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        } 
        return obj;
    }
    
    public Object getObject(String key) {
        Jedis jedis = null;
        Object obj=null;
        try {
            jedis = jedisPoolRead.getResource();
            Object _obj = jedis.get(key); 
            if (null != _obj) {
                obj=_obj;
            }else{
                byte[] bt = jedis.get(key.getBytes());
                obj = SerializationUtil.bytes2Object(bt); 
            }
            jedisPoolRead.returnResource(jedis);
        } catch (Exception e) { 
            logger.error(e.getMessage(),e);
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        } 
        return obj;
    }
    
    public <T> T getJson(String key,Class<T> classOfT) {
        Jedis jedis = null; 
        T obj=null;
        try {
            jedis = jedisPoolRead.getResource();
            String value = jedis.get(key); 
            if (value != null && value instanceof String) { 
                obj = JSON.parseObject(value, classOfT);
            }
            jedisPoolRead.returnResource(jedis); 
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        } 
        return obj;
    }
     
    
    public byte[] get(byte[] key) {
        Jedis jedis = null;
        byte[] obj =null;
        try {
            jedis = jedisPoolRead.getResource(); 
            obj = jedis.get(key);  
            jedisPoolRead.returnResource(jedis);
        } catch (Exception e) { 
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        }  
        return obj;
    }
    
    public Object getBytes(String key) {
        Jedis jedis = null;
        Object obj =null;
        try {
            jedis = jedisPoolRead.getResource(); 
            byte[] _obj  = jedis.get(key.getBytes()); 
            obj = SerializationUtil.bytes2Object(_obj); 
            jedisPoolRead.returnResource(jedis);
        } catch (Exception e) { 
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        } 
        return obj;
    }
    

    public List<Object> mgetFromRedis(byte[] key, byte[]... fields) {
        List<Object> returnList = new ArrayList<Object>();
        Jedis jedis = null;
        try {
            jedis = jedisPoolRead.getResource();
            List<byte[]> list = jedis.hmget(key, fields);
            if (null != list && list.size() > 0) {
                for (byte[] b : list) {
                    returnList.add(SerializationUtil.bytes2Object(b));
                }
            }
            jedisPoolRead.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            }
        } 
        return returnList;

    }
 
 
    public boolean exists(String key) {
        boolean flag = false;
        Jedis jedis = null;
        try {
            jedis = jedisPoolRead.getResource();
            flag = jedis.exists(key);
            jedisPoolRead.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        } 
        return flag;
    }
  
 

    /**
     * lpop(key)：返回并删除名称为key的list中的首元素string
     * @param key
     */
   
    public String lpop(String key) { 
        Jedis jedis = null;
        String value=null;
        try {
            jedis = jedisPoolWrite.getResource();
            value = jedis.lpop(key);
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("lpop " +ExceptionUtils.getFullStackTrace(e)); 
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return value; 
    }
    /**
     * 返回并删除名称为key的list中的首元素string,带阻塞
     * @param timeout
     * @param key
     * @return
     */
    public List<String> blpop(final int timeout, final String keys) { 
        Jedis jedis = null;
        List<String> value=null;
        try {
            jedis = jedisPoolWrite.getResource();
            value = jedis.blpop(timeout, keys);
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("lpop " +ExceptionUtils.getFullStackTrace(e)); 
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return value; 
    }
    public String blpop(final String key) { 
        Jedis jedis = null;
        String value=null;
        List<String> values=null;
        int timeout=3;
        try {
            jedis = jedisPoolWrite.getResource();
            values = jedis.blpop(timeout, key);
            if(values!=null && values.size()>0){
                value=values.get(0);
            }
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("lpop " +ExceptionUtils.getFullStackTrace(e)); 
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return value;
    }
    
    public Long llen(final String key) { 
        Jedis jedis = null; 
        Long llen=0L;
        try {
            jedis = jedisPoolWrite.getResource();
            llen = jedis.llen(key); 
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            llen=0L;
            logger.error("lpop " +ExceptionUtils.getFullStackTrace(e)); 
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return llen;
    }
    
    /**
     * rpush(key, value)：在名称为key的list尾添加一个值为value的元素,返回length
     */
    
    public Long rpush(String key, String value) { 
        Jedis jedis = null;
        Long pos=0L;
        try {
            jedis = jedisPoolWrite.getResource(); 
            pos=jedis.rpush(key, value); 
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("rpush " +ExceptionUtils.getFullStackTrace(e)); 
            if (null != jedisPoolWrite && jedis!=null) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return pos; 
    }
  /**
   * 
   * @param key
   * @param field
   * @param value
   * @param seconds
   */
    public void hset(String key,String field,String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            jedis.hset(key, field, value); 
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hset " +ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
    }
    
    public void hmset(String key, Map<String, String> fieldValues){
    	ShardedJedis jedis = null;
    	try{
    		jedis = shardedJedisPoolWrite.getResource();
    		jedis.hmset(key, fieldValues);
    		shardedJedisPoolWrite.returnBrokenResource(jedis);
    	}catch(Exception e){
    		logger.error("hmset " +ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
    	}
    }
    
    public void hset(String key,String field,String value,int seconds) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            jedis.hset(key, field, value);
            jedis.expire(key, seconds); 
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hset " +ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
    }
   
    public String hget(String key,String field) {
        Jedis jedis = null;
        String value=null;
        try {
            jedis = jedisPoolRead.getResource();
            value = jedis.hget(key, field); 
            jedisPoolRead.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hget " +ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        }  
        return value;
    }
    
    public List<String> hmget(String key,String[] fields) {
        ShardedJedis jedis = null;
        List<String> values = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            values = jedis.hmget(key, fields);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hmget " +ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
            	shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return values;
    }
    
    public Map<String, String> hgetAll(String key,String field) {
        Jedis jedis = null;
        Map<String, String> map=null;
        try {
            jedis = jedisPoolRead.getResource();
            map = jedis.hgetAll(key);
            jedisPoolRead.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        } 
        return map;
    }
    
    public Set<String> hkeys(String key) {
        Jedis jedis = null;
        Set<String> set=null;
        try {
            jedis = jedisPoolRead.getResource();
             set = jedis.hkeys(key);
             jedisPoolRead.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        }  
        return set;
    }
    
    public List<String> hvals(String key) {
        Jedis jedis = null;
        List<String> list=null;
        try {
            jedis = jedisPoolRead.getResource();
            list = jedis.hvals(key);
            jedisPoolRead.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        } 
        return list;
    }
    public Long hdel(String key,String field) {
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.hdel(key, field); 
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return v;
    }
    
    public Long hlen(String key) {
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolRead.getResource();
            v = jedis.hlen(key);
            jedisPoolRead.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        }  
        return v;
    }
    
    public Boolean hexists(String key, String field) {  
        boolean flag = false;
        Jedis jedis = null;
        try {
            jedis = jedisPoolRead.getResource();
            flag = jedis.hexists(key,field);
            jedisPoolRead.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolRead && null!=jedis) {
                jedisPoolRead.returnBrokenResource(jedis);
            } 
        }  
        return flag;    
    }
    
    public Long zadd(String key, double score, String member) {
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.zadd(key, score, member); 
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return v;
    }
    
    public Long zadd(String key, double score, String member,int seconds) {
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.zadd(key, score, member);
            jedis.expire(key, seconds);
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && jedis!=null) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return v;    
    }
     
    
    public Double zscore(String key,String member) {
        Jedis jedis = null;
        Double v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.zscore(key, member);
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return v;    
    }
 
    public Long zcount(String key, String min, String max) { 
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.zcount(key, min, max);
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return v; 
     }
    
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
            double min) { 
        Jedis jedis = null; 
        Set<Tuple> v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.zrevrangeByScoreWithScores(key, min, max);
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return v; 
    } 
  
    
    @Override
    public void subscribe(JedisPubSub jedisPubSub, String channels){
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            jedis.subscribe(jedisPubSub, channels);
        } catch (Exception e) {
            e.printStackTrace();
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite) {
                jedisPoolWrite.returnResource(jedis);
            }
        }
    }

    @Override
    public long publish(String channel, String message) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            return jedis.publish(channel, message);
        } catch (Exception e) {
            e.printStackTrace();
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite) {
                jedisPoolWrite.returnResource(jedis);
            }
        }
        return 0;
    }
    
    //加到一个固定大小的队列里
    public int pushLimitList(String key, String jsonValue,int limitSize,int seconds) {
        int llen =0; 
        Jedis jedis = null; 
        try {
            jedis = jedisPoolWrite.getResource();  
            Long s=  jedis.llen(key);
            if(s!=null){
                llen=s.intValue();
            } 
            if(llen>=limitSize){ 
                jedis.lpop(key);
            }  
            if(jsonValue!=null && jsonValue.length()>0){ 
                jedis.rpush(key, jsonValue); 
            }
            jedis.expire(key, seconds); 
            
        } catch (Exception e) {
            logger.error("pushLimitList "+ExceptionUtils.getFullStackTrace(e));  
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return llen;
    }
    
    //返回名称为key的list中start至end之间的元素（下标从0开始，下同）
    public String lindex(String key,int index) { 
        Jedis jedis = null;
        String value=null;
        try {
            jedis = jedisPoolWrite.getResource();
            value=jedis.lindex(key, index);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));  
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return value;
    
    }
    
    //返回名称为key的list中index位置的元素
    public List<String>  lrange(String key,int start,int end) {
        int llen =0; 
        Jedis jedis = null;
        List<String> value=null;
        try{ 
            jedis = jedisPoolWrite.getResource();  
            Long s=jedis.llen(key);
            if(s!=null){
               llen = s.intValue(); 
            }
            if(end>llen){ 
                end=llen;
            }  
            value=jedis.lrange(key, Long.valueOf(start),Long.valueOf(end));  
        } catch (Exception e) {  
            logger.error("lrange "+ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return value;
    }
    //插入set数据
    public Long sadd(String key, int seconds,final String... members) { 
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.sadd(key, members);
            jedis.expire(key, seconds); 
        } catch (Exception e) { 
            logger.error("sadd "+ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v; 
    }
    public Long saddlimit(String key, int seconds,int limit,final String... members) { 
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();  
            Long s=  jedis.scard(key);
            int llen =0;
            if(s!=null){
                llen=s.intValue();
            }
            if( llen>=limit){
                jedis.spop(key);
            }
            v = jedis.sadd(key, members);
            jedis.expire(key, seconds);
            
        } catch (Exception e) { 
            logger.error("saddlimit "+ ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v; 
    }
    //pop set数据
    public String spop(String key) { 
        Jedis jedis = null;
        String v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.spop(key);  
        } catch (Exception e) { 
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v; 
    }
    public boolean sismember(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolWrite.getResource();
            boolean result = jedis.sismember(key, value);
            return result;
        } catch (Exception e) { 
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return false; 
    }
    //查看set list
    public Set<String> smembers(String key) { 
        Jedis jedis = null;
        Set<String> v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.smembers(key); 
        } catch (Exception e) { 
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v; 
    }
    //set size
    public Long  scard(String key){ 
        Jedis jedis = null;
        Long v=0l;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.scard(key); 
        } catch (Exception e) {
            v=0l; 
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
    //删除指定无素
    public Long  srem(final String key, final String... members){ 
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.srem(key, members);
            jedisPoolWrite.returnResource(jedis);
        } catch (Exception e) { 
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
    //将a从myset移到myset2，从结果可以看出移动成功=1。
    public Long  smove(final String srckey, final String dstkey,  final String member){ 
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.smove(srckey, dstkey, member);
             
        } catch (Exception e) { 
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
   //set差异
    public  Set<String>  sdiff(final String... keys){ 
        Jedis jedis = null;
        Set<String> v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.sdiff(keys); 
        } catch (Exception e) { 
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
    //set差异存到一个新set
    public  Long  sdiffstore(final String dstkey, int seconds,final String... keys){ 
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.sdiffstore(dstkey, keys);
            jedis.expire(dstkey, seconds);
            
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
    
    //set交集
    public  Set<String>  sinter(final String... keys){ 
        Jedis jedis = null;
        Set<String> v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.sinter(keys); 
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
    //set交集存到一个新set
    public  Long  sinterstore(final String dstkey,int seconds, final String... keys){ 
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.sinterstore(dstkey, keys);
            jedis.expire(dstkey, seconds); 
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
  //set并集
    public  Set<String>  sunion(final String... keys){ 
        Jedis jedis = null;
        Set<String> v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.sunion(keys); 
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
    //set并集存到一个新set
    public  Long  sunionstore(final String dstkey,int seconds, final String... keys){ 
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.sunionstore(dstkey, keys);
            jedis.expire(dstkey, seconds); 
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
    //计数器是 Redis 的原子性自增操作
    //incr
    public  Long  incr(final String key,int seconds){ 
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.incr(key); 
            jedis.expire(key, seconds);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
    
    //decr
    public  Long  decr(final String key,int seconds){ 
        Jedis jedis = null;
        Long v=null;
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.decr(key); 
            jedis.expire(key, seconds);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return v;  
    }
    //limitCall,key=ip
    public  boolean  limitIpCall(final String key,int maxSize,int seconds){ 
        Jedis jedis = null;
        Long v=null;
        boolean f=false;
        Long mx=Long.valueOf(maxSize);
        try {
            jedis = jedisPoolWrite.getResource();
            v = jedis.incr(key); 
            jedis.expire(key, seconds);
            Long cur = v;
            if(cur!=null && cur>mx){
            	f= true;
            }else{ 
                f=false;
            } 
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            jedisPoolWrite.returnBrokenResource(jedis);
        } finally {
            if (null != jedisPoolWrite && null!=jedis) {
                jedisPoolWrite.returnResource(jedis);
            }
        }  
        return f;  
    }
 
    public void hsetBytes(String key, String field, Serializable value) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            jedis.hset(key.getBytes(), field.getBytes(), SerializationUtil.object2Bytes(value));
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }
    
    public Object hgetBytes(String key, String field) {
        ShardedJedis jedis = null;
        Object obj =null;
        try {
            jedis = shardedJedisPoolWrite.getResource(); 
            byte[] _obj  = jedis.hget(key.getBytes(), field.getBytes()); 
            obj = SerializationUtil.bytes2Object(_obj); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) { 
            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return obj;
    }

}