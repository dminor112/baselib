package com.dminor.baselib.jedis.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.Tuple;
import redis.clients.util.Pool;
//import com.alibaba.fastjson.JSON;
import com.dminor.baselib.jedis.AbstractJedisCache;
import com.dminor.baselib.jedis.ShardedJedisSentinelPool;
import com.dminor.baselib.utils.SerializationUtil;

public class ShardedJedisCacheImpl extends AbstractJedisCache  {
    private static final Logger logger = LoggerFactory.getLogger(ShardedJedisCacheImpl.class);  
    
    public  ShardedJedisCacheImpl(ShardedJedisSentinelPool shardSentinelMaster){
    	super(shardSentinelMaster); 
        logger.info("ShardedJedisSentinelPool#");
    }
    
    public  ShardedJedisCacheImpl(ShardedJedisPool shardedJedisPool){ 
         super(shardedJedisPool); 
    	logger.info("shardedJedisPool#");
         
    }
    public String jedisInfo() {
        Jedis jedis = null;
        ShardedJedis sdjedis = null;
        String info="";
        try {
        	sdjedis= this.getShardedJedisPoolWrite().getResource();
        	List<Jedis> allshard=(List<Jedis>) sdjedis.getAllShards(); 
            jedis =  allshard.get(0);
            info=jedis.info(); 
            logger.warn("test key:"+jedis.get("test"));
             
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
             
        }
        return info;
    }

  //采用sharding===begin====
    public void putString(String key, String value) {
        ShardedJedis jedis = null;
        try {
            jedis =  shardedJedisPoolWrite.getResource();
            jedis.set(key, value); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite &&  null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        }
    } 
 

    public void putString(String key, String value,int seconds) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            jedis.set(key, value); 
            jedis.expire(key, seconds);// 设置过期时间
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }
    
    public void put(String key, Serializable value) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            if(value instanceof String){
                jedis.set(key, (String) value);
            }else{
                jedis.set(key.getBytes(), SerializationUtil.object2Bytes(value));
            }
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        }  
    }
    public void put(String key, Serializable value,int seconds) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            if(value instanceof String){
                jedis.set(key, (String) value);
                jedis.expire(key, seconds);// 设置过期时间
            }else{
                jedis.set(key.getBytes(), SerializationUtil.object2Bytes(value));
                jedis.expire(key.getBytes(), seconds);// 设置过期时间
            } 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e)); 
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }
    
    /**
     *Serialization   object2Bytes
     *
     */
    public void putBytes(String key, Serializable value) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            jedis.set(key.getBytes(), SerializationUtil.object2Bytes(value));
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }
    
    public void hsetBytes(String key, String field, Serializable value) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            jedis.hset(key.getBytes(), field.getBytes(), SerializationUtil.object2Bytes(value));
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }

    /**
      *Serialization   object2Bytes
      */
    public void putBytes(String key, Serializable value, int seconds) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            jedis.set(key.getBytes(), SerializationUtil.object2Bytes(value));
            jedis.expire(key.getBytes(), seconds);// 设置过期时间
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }

    public void putJson(String key, Object value) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
//            jedis.set(key, JSON.toJSONString(value));
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        }  
    }

    public void putJson(String key, Object value, int seconds) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
//            jedis.set(key, JSON.toJSONString(value));
            jedis.expire(key, seconds);// 设置过期时间
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        }  
    }
    
    public void expireKey(String key, int seconds){
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource(); 
            jedis.expire(key, seconds);// 设置过期时间
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
    }
    
    public void remove(String key) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            jedis.del(key); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        }  
    }
    
    public String getString(String key) {
        ShardedJedis jedis = null;
        String value=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            value = jedis.get(key); 
            shardedJedisPoolWrite.returnResource(jedis); 
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null !=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return value;
    }
   
    public Object get(String key) {
        return getObject(key);
    }
    
  
    @SuppressWarnings("unchecked")
    public <T> T get(byte[] key,Class<T> classOfT) {
        ShardedJedis jedis = null; 
        T obj=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();         
            byte[] bt = jedis.get(key);
            if(bt!=null && bt.length > 0 ){
                obj = (T) SerializationUtil.bytes2Object(bt);
            }
            shardedJedisPoolWrite.returnResource(jedis); 
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return obj;
    }
    
    public Object getObject(String key) {
        ShardedJedis jedis = null;
        Object obj=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            Object _obj = jedis.get(key); 
            if (null != _obj) {
                obj=_obj;
            }else{
                byte[] bt = jedis.get(key.getBytes());
                obj = SerializationUtil.bytes2Object(bt); 
            }
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) { 
            logger.error(e.getMessage(),e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return obj;
    }
    
    public <T> T getJson(String key,Class<T> classOfT) {
        ShardedJedis jedis = null; 
        T obj=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            String value = jedis.get(key); 
            if (value != null && value instanceof String) { 
//                obj = JSON.parseObject(value, classOfT);
            }
            shardedJedisPoolWrite.returnResource(jedis); 
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return obj;
    }
     
    
    public byte[] get(byte[] key) {
        ShardedJedis jedis = null;
        byte[] obj =null;
        try {
            jedis = shardedJedisPoolWrite.getResource(); 
            obj = jedis.get(key);  
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) { 
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return obj;
    }
    
    public Object getBytes(String key) {
        ShardedJedis jedis = null;
        Object obj =null;
        try {
            jedis = shardedJedisPoolWrite.getResource(); 
            byte[] _obj  = jedis.get(key.getBytes()); 
            obj = SerializationUtil.bytes2Object(_obj); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) { 
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return obj;
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
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return obj;
    }

    public List<Object> mgetFromRedis(byte[] key, byte[]... fields) {
        List<Object> returnList = new ArrayList<Object>();
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            List<byte[]> list = jedis.hmget(key, fields);
            if (null != list && list.size() > 0) {
                for (byte[] b : list) {
                    returnList.add(SerializationUtil.bytes2Object(b));
                }
            }
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            }
        } 
        return returnList;

    }
 
 
    public boolean exists(String key) {
        boolean flag = false;
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            flag = jedis.exists(key);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getFullStackTrace(e));
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return flag;
    }
  
 

    /**
     * lpop(key)：返回并删除名称为key的list中的首元素string
     * @param key
     */
   
    public String lpop(String key) { 
        ShardedJedis jedis = null;
        String value=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            value = jedis.lpop(key);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
//            logger.error("lpop " +ExceptionUtils.getFullStackTrace(e)); 
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
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
    
    public List<String> blpop(final int timeout, final String key) { 
        ShardedJedis jedis = null;
        List<String> value=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            value = jedis.blpop(key);
            jedis.expire(key, timeout);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("blpop ", e); 
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return value; 
    }
    
 
    public String blpop(final String key) { 
        ShardedJedis jedis = null;
        String value=null;
        List<String> values=null;
       
        try {
            jedis = shardedJedisPoolWrite.getResource();
            values = jedis.blpop( key);
            if(values!=null && values.size()>0){
                value=values.get(0);
            }
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("blpop ", e); 
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return value;
    }
    
    public Long llen(final String key) { 
        ShardedJedis jedis = null; 
        Long llen=0L;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            llen = jedis.llen(key); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            llen=0L;
            logger.error("llen ", e); 
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return llen;
    }
    
    /**
     * rpush(key, value)：在名称为key的list尾添加一个值为value的元素,返回length
     */
    
    public Long rpush(String key, String value) { 
        ShardedJedis jedis = null;
        Long pos=0L;
        try {
            jedis = shardedJedisPoolWrite.getResource(); 
            pos=jedis.rpush(key, value); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("rpush ", e); 
            if (null != shardedJedisPoolWrite && jedis!=null) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
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
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            jedis.hset(key, field, value); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hset ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
    }
    
    public void hset(String key,String field,String value,int seconds) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            jedis.hset(key, field, value);
            jedis.expire(key, seconds); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hset ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
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
    		logger.error("hmset ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
    	}
    }
   
    public String hget(String key,String field) {
        ShardedJedis jedis = null;
        String value=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            value = jedis.hget(key, field); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hget ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
            	shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return value;
    }
    
    public List<String> hmget(String key, String[] fields) {
        ShardedJedis jedis = null;
        List<String> values = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            values = jedis.hmget(key, fields);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hmget ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
            	shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return values;
    }
    
    public Map<String, String> hgetAll(String key,String field) {
        ShardedJedis jedis = null;
        Map<String, String> map=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            map = jedis.hgetAll(key);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hgetAll ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
            	shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return map;
    }
    
    public Set<String> hkeys(String key) {
        ShardedJedis jedis = null;
        Set<String> set=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
             set = jedis.hkeys(key);
             shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hkeys ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
            	shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return set;
    }
    
    public List<String> hvals(String key) {
        ShardedJedis jedis = null;
        List<String> list=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            list = jedis.hvals(key);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hvals ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
            	shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return list;
    }
    public Long hdel(String key,String field) {
        ShardedJedis jedis = null;
        Long v=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            v = jedis.hdel(key, field); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hdel ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return v;
    }
    
    public Long hlen(String key) {
        ShardedJedis jedis = null;
        Long v=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            v = jedis.hlen(key);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hlen ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
            	shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return v;
    }
    
    public Boolean hexists(String key, String field) {  
        boolean flag = false;
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            flag = jedis.hexists(key,field);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("hexists ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
            	shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return flag;    
    }
    
    public Long zadd(String key, double score, String member) {
        ShardedJedis jedis = null;
        Long v=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            v = jedis.zadd(key, score, member); 
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("zadd ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        }  
        return v;
    }
    
    public Long zadd(String key, double score, String member,int seconds) {
        ShardedJedis jedis = null;
        Long v=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            v = jedis.zadd(key, score, member);
            jedis.expire(key, seconds);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("zadd ", e);
            if (null != shardedJedisPoolWrite && jedis!=null) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return v;    
    }
     
    
    public Double zscore(String key,String member) {
        ShardedJedis jedis = null;
        Double v=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            v = jedis.zscore(key, member);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("zscore ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return v;    
    }
 
    public Long zcount(String key, String min, String max) { 
        ShardedJedis jedis = null;
        Long v=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            v = jedis.zcount(key, min, max);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("zcount ", e);
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return v; 
     }
    
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
            double min) { 
        ShardedJedis jedis = null; 
        Set<Tuple> v=null;
        try {
            jedis = shardedJedisPoolWrite.getResource();
            v = jedis.zrevrangeByScoreWithScores(key, min, max);
            shardedJedisPoolWrite.returnResource(jedis);
        } catch (Exception e) {
            logger.error("zrevrangeByScoreWithScores ", e); 
            if (null != shardedJedisPoolWrite && null!=jedis) {
                shardedJedisPoolWrite.returnBrokenResource(jedis);
            } 
        } 
        return v; 
    }
	
}
