package com.dminor.baselib.jedis;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Tuple;

/**
 * redis Cache base
 * 
 * @author zhouguoqing
 * @date 2012-5-20
 */
public interface JedisCache  { 
    public void putString(String key, String value);
    public void putString(String key, String value, int seconds);
    public void put(String key, Serializable value);
    public void put(String key, Serializable value, int seconds);
    public void putBytes(String key, Serializable value);
    public void putBytes(String key, Serializable value, int seconds);
    public void putJson(String key, Object value); 
    public void putJson(String key, Object value, int seconds);
    public void expireKey(String key, int seconds);
    public Object get(String key);
    public <T> T get(byte[] key,Class<T> classOfT);
    public byte[] get(byte[] key); 
    public String getString(String key); 
    public Object getObject(String key);
    public Object getBytes(String key); 
    public <T> T getJson(String key,Class<T> classOfT);
    public void remove(String key); 
    public boolean exists(String key); 
    public List<Object> mgetFromRedis(byte[] key, byte[]... fields);
    public String jedisInfo(); 
    /**
     * lpop(key)：返回并删除名称为key的list中的首元素string
     * 
     * @param key
     */
    public String lpop(String key);
    
    public Long llen(final String key);
    /**
     * lpop(key)：返回并删除名称为key的list中的首元素string,带阻塞
     * @param timeout
     * @param key
     */
    public List<String> blpop(final int timeout, final String keys);
       
    public String blpop(final String key);


    /**
     * rpush(key, value)：在名称为key的list尾添加一个值为value的元素,返回length
     */
    public Long rpush(String key, String value);

    
    public void hset(String key, String field, String value);
    public void hset(String key, String field, String value, int seconds); 
    public void hsetBytes(String key, String field, Serializable value);
    public Object hgetBytes(String key, String field);
    public void hmset(String key, Map<String, String> fieldValues);
    public String hget(String key, String field); 
    public List<String> hmget(String key, String[] fields);
    public Map<String, String> hgetAll(String key, String field); 
    public Set<String> hkeys(String key); 
    public List<String> hvals(String key);
    public Long hdel(String key,String field) ;
    public Long hlen(String key) ;
    public Boolean hexists(String key, String field);
    public Long zadd(String key, double score, String member);
    public Long zadd(String key, double score, String member, int seconds); 
    public Double zscore(String key, String member); 
    public Long zcount(String key, String min, String max); 
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min);
    public void subscribe(JedisPubSub jedisPubSub, String  channels);

    public long publish(String channel, String message);
    
    public int pushLimitList(String key, String jsonValue,int limitSize,int seconds) ;
    public String lindex(String key,int index) ;
    public List<String>  lrange(String key,int start,int end) ;
    //插入set数据
    public Long sadd(String key, int seconds,final String... members) ;
    //插入set数据
    public Long saddlimit(String key, int seconds,int limitSize,final String... members) ;
    //pop set数据
    public String spop(String key) ;
    //查看value是否为key的成员
    public boolean sismember(String key, String value);
    //查看set list
    public Set<String> smembers(String key);
    //set size
    public Long  scard(String key);
    //删除指定无素
    public Long  srem(final String key, final String... members);
    //将a从myset移到myset2，从结果可以看出移动成功=1。
    public Long  smove(final String srckey, final String dstkey,  final String member);
   //set差异
    public  Set<String>  sdiff(final String... keys);
    //set差异存到一个新set
    public  Long  sdiffstore(final String dstkey,int seconds, final String... keys);
    
    //set交集
    public  Set<String>  sinter(final String... keys) ;
    //set交集存到一个新set
    public  Long  sinterstore(final String dstkey,int seconds, final String... keys) ;
  //set并集
    public  Set<String>  sunion(final String... keys) ;
    //set并集存到一个新set
    public  Long  sunionstore(final String dstkey,int seconds,final String... keys);
    //#该Key的值递增1,计数器
    public Long incr(String key,int seconds) ;
    //#该Key的值递减1,计数器
    public Long decr(String key,int seconds) ;
    //限速器
    public boolean limitIpCall(String key,int maxSize,int seconds);
    
}
