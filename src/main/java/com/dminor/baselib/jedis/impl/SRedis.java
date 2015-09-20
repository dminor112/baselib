package com.dminor.baselib.jedis.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

public class SRedis {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(JRedis.class);

	private volatile JedisPool readPool;
	private volatile JedisPool writePool;

	public SRedis(JedisPoolConfig poolConfig, String readIp, int readPort, String writeIp, int writePort, int dbTimeout, int dbNum) {
		readPool = new JedisPool(poolConfig, readIp, readPort, dbTimeout, null, dbNum);
		writePool = new JedisPool(poolConfig, writeIp, writePort, dbTimeout, null, dbNum);
	}

	private JedisPool getReadPool() {
		return readPool;
	}

	private JedisPool getWritePool() {
		return writePool;
	}

	static private ThreadLocal<Jedis> writeThreadlocal = new ThreadLocal<Jedis>();
	static private ThreadLocal<Jedis> readThreadlocal = new ThreadLocal<Jedis>();

	/**
	 * 获取数据库写操作实例
	 * @return
	 */
	private Jedis getWriteInstance() {
		Jedis handle = writeThreadlocal.get();
		if (handle == null) {
			writeThreadlocal.set(writePool.getResource());
		}
		return writeThreadlocal.get();
	}
	
	/**
	 * 获取数据库读操作的实例
	 * @return
	 */
	private Jedis getReadInstance() {
		Jedis handle = readThreadlocal.get();
		if (handle == null) {
			readThreadlocal.set(readPool.getResource());
		}
		return readThreadlocal.get();
	}
	
	/**
	 * 对数据库写操作进行包装，做额外的操作，把连接实例返回给写操作连接池
	 * @param c
	 * @return
	 */
	private <T> T writeFuncWrapper(Callable<T> c) {
		return writeFuncWrapper(c, true);
	}
	
	private <T> T writeFuncWrapper(Callable<T> c, boolean returnIns) {
		T result = null;
		final int RETRY_LIMIT = 3;
		for (int iRetry=0; iRetry<RETRY_LIMIT; iRetry++)
		{
			try{
				//执行数据库操作
				result = c.call();
				//执行成功，没抛异常，就break到外面去返回结果
				break;
			} catch(JedisConnectionException e) {
				//连接异常，说明这个连接挂了，尝试重试一下
				if (iRetry<(RETRY_LIMIT-1)) {
					//还没到重试极限，销毁当前连接进行重试
					Jedis handle = writeThreadlocal.get();
					if (handle != null) {
						//redis没有关闭连接的接口，这里只能指望超时机制回收掉这个已经坏了的连接了
						//销毁本地的连接引用，这样下一轮会重新取一个新的连接，这事儿就算是重连了。
						writeThreadlocal.remove();
					}
				} else {
					//重试极限到了，没办法，抛异常
					throw e;
				}
			} catch(JedisDataException e) {
				//数据问题，继续抛出吧，业务层逻辑有问题
				throw e;
			} catch(RuntimeException e) {
				//其他运行时异常，照抛
				throw e;
			} catch(Exception e) {
				//奇葩的异常
				throw new RuntimeException(e);
			} finally {
				//确保归还连接，不要泄漏
				if(returnIns){
					if (null != writeThreadlocal.get()) {
						writeThreadlocal.get().close();
//						writePool.returnResource(writeThreadlocal.get());
						writeThreadlocal.remove();
					}
				}
			}
		}
		return result;
	}
	
	/**
	 * 对于没有归还Jredis实例的操作，完成后需要调用该函数进行归还
	 */
	public void returnJedisIntance(){
		if (null != writeThreadlocal.get()) {
			writeThreadlocal.get().close();
//			writePool.returnResource(writeThreadlocal.get());
			writeThreadlocal.remove();
		}
	}
	
	/**
	 * 对数据库读操作进行包装，做额外的操作，把连接实例返回给读操作连接池
	 * @param c
	 * @return
	 */
	private <T> T readFuncWrapper(Callable<T> c) {
		T result = null;
		final int RETRY_LIMIT = 3;
		for (int iRetry=0; iRetry<RETRY_LIMIT; iRetry++)
		{
			try{
				//执行数据库操作
				result = c.call();
				//执行成功，没抛异常，就break到外面去返回结果
				break;
			} catch(JedisConnectionException e) {
				//连接异常，说明这个连接挂了，尝试重试一下
				if (iRetry<(RETRY_LIMIT-1)) {
					//还没到重试极限，销毁当前连接进行重试
					Jedis handle = readThreadlocal.get();
					if (handle != null) {
						//redis没有关闭连接的接口，这里只能指望超时机制回收掉这个已经坏了的连接了
						//销毁本地的连接引用，这样下一轮会重新取一个新的连接，这事儿就算是重连了。
						readThreadlocal.remove();
					}
				} else {
					//重试极限到了，没办法，抛异常
					throw e;
				}
			} catch(JedisDataException e) {
				//数据问题，继续抛出吧，业务层逻辑有问题
				throw e;
			} catch(RuntimeException e) {
				//其他运行时异常，照抛
				throw e;
			} catch(Exception e) {
				//奇葩的异常
				throw new RuntimeException(e);
			} finally {
				//确保归还连接，不要泄漏
				if (null != readThreadlocal.get()) {
					readThreadlocal.get().close();
//					readPool.returnResource(readThreadlocal.get());
					readThreadlocal.remove();
				}
			}
		}
		return result;
	}

	public Set<String> keys(final String key) {
		return readFuncWrapper(new Callable<Set<String>>() {

			@Override
			public Set<String> call() {
				return getReadInstance().keys(key);
			}
		});
	}
	
	public Long del(final String key) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().del(key);
			}
		});
	}
	
	public Long del(final String[] keys) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().del(keys);
			}
		});
	}
	
	public String set(final String key, final String value) {
		return writeFuncWrapper(new Callable<String>() {

			@Override
			public String call() {
				return getWriteInstance().set(key, value);
			}
		});
	}
	
	public Long append(final String key, final String value) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().append(key, value);
			}
		});
	}
	
	public String get(final String key) {
		return readFuncWrapper(new Callable<String>() {
			
			@Override
			public String call(){
				return getReadInstance().get(key);
			}
		});
	}
	
	public String getrange(final String key, final long startOffset, final long endOffset) {
		return readFuncWrapper(new Callable<String>() {
			
			@Override
			public String call(){
				return getReadInstance().getrange(key, startOffset, endOffset);
			}
		});
	}
	
	public Long incr(final String key) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().incr(key);
			}
		});
	}
	
	public Long incrBy(final String key, final long integer) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().incrBy(key, integer);
			}
		});
	}
	
	public Long hset(final String key, final String field, final String value) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().hset(key, field, value);
			}
		});
	}
	
	public String hget(final String key, final String field) {
		return readFuncWrapper(new Callable<String>() {
			
			@Override
			public String call(){
				return getReadInstance().hget(key, field);
			}
		});
	}
	
	public String hmset(final String key, final Map<String, String> hash) {
		return writeFuncWrapper(new Callable<String>() {
			
			@Override
			public String call(){
				return getWriteInstance().hmset(key, hash);
			}
		});
	}
	
	public String[] hmget(final String key, final String[] fields) {
		return readFuncWrapper(new Callable<String[]>() {
			
			@Override
			public String[] call(){
				Jedis jedis = getReadInstance();
				return jedis.hmget(key, fields).toArray(new String[fields.length]);
			}
		});
	}
	
	public Map<String, String> hgetAll(final String key) {
		return readFuncWrapper(new Callable<Map<String, String>>() {
			
			@Override
			public Map<String, String> call(){
				return getReadInstance().hgetAll(key);
			}
		});
	}
	
	public Long hdel(final String key, final String... fields) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().hdel(key, fields);
			}
		});
	}
	
	public Set<String> hkeys(final String key) {
		return readFuncWrapper(new Callable<Set<String>>() {
			
			@Override
			public Set<String> call() {
				return getReadInstance().hkeys(key);
			}
		});
	}
	
	public Boolean hexists(final String key, final String field) {
		return readFuncWrapper(new Callable<Boolean>() {
			
			@Override
			public Boolean call() {
				return getReadInstance().hexists(key, field);
			}
		});
	}

	public Long lpush(final String key, final String... strings) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().lpush(key, strings);
			}
		});
	}
	
	public String lpop(final String key) {
		return writeFuncWrapper(new Callable<String>() {

			@Override
			public String call() {
				return getWriteInstance().lpop(key);
			}
		});
	}
	
	public Long lrem(final String key, final int count, final String value) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().lrem(key, count, value);
			}
		});
	}
	
	public Long llen(final String key) {
		return readFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() throws Exception {
				return getReadInstance().llen(key);
			}
			
		});
	}
	
	public Long sadd(final String key, final String... members) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().sadd(key, members);
			}
		});
	}
	
	public Long srem(final String key, final String... members) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().srem(key, members);
			}
		});
	}
	
	public String srandmember(final String key) {
		return readFuncWrapper(new Callable<String>() {

			@Override
			public String call() {
				return getReadInstance().srandmember(key);
			}
		});
	}
	
	public Set<String> smembers(final String key) {
		return readFuncWrapper(new Callable<Set<String>>() {

			@Override
			public Set<String> call() {
				return getReadInstance().smembers(key);
			}
		});
	}

	public String spop(final String key) {
		return writeFuncWrapper(new Callable<String>() {

			@Override
			public String call() {
				return getWriteInstance().spop(key);
			}
		});
	}

	public Long zadd(final String key, final double score, final String member) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().zadd(key, score, member);
			}
		});
	}
	
	public Set<String> zrange(final String key, final long start, final long end) {
		return readFuncWrapper(new Callable<Set<String>>() {

			@Override
			public Set<String> call() {
				return getReadInstance().zrange(key, start, end);
			}
		});
	}
	
	public Long zrank(final String key, final String member) {
		return readFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getReadInstance().zrank(key, member);
			}
		});
	}
	
	public Set<String> zrevrange(final String key, final long start, final long end) {
		return readFuncWrapper(new Callable<Set<String>>() {

			@Override
			public Set<String> call() {
				return getReadInstance().zrevrange(key, start, end);
			}
		});
	}
	
	public Long zcard(final String key) {
		return readFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() throws Exception {
				return getReadInstance().zcard(key);
			}
			
		});
	}
	
	public Long zremrangeByRank(final String key, final long start, final long end) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().zremrangeByRank(key, start, end);
			}
		});
	}
	
	public Long zremrangeByScore(final String key, final String start, final String end) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().zremrangeByScore(key, start, end);
			}
		});
	}
	
	public Long zrem(final String key, final String... members) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().zrem(key, members);
			}
		});
	}
	
	public Transaction multi() {
		return writeFuncWrapper(new Callable<Transaction>() {

			@Override
			public Transaction call() {
				return getWriteInstance().multi();
			}
		}, false);
	}
	
//	public Long test() {
//		return writeFuncWrapper(new Callable<Long>() {
//
//			@Override
//			public Long call() {
//				String key = "tranxection_test";
//				Jedis j = getWriteInstance();
//				j.watch(key);
//				Transaction tx = j.multi();
//				tx.set(key, "value");
//				tx.exec();
//				return null;
//			}
//		});
//	}
	
	public String watch(final String key) {
		return writeFuncWrapper(new Callable<String>() {

			@Override
			public String call() {
				return getWriteInstance().watch(key);
			}
		}, false);
	}
	
	public String unwatch() {
		return writeFuncWrapper(new Callable<String>() {

			@Override
			public String call() {
				return getWriteInstance().unwatch();
			}
		});
	}
	
	public Long expire(final String key, final int seconds) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().expire(key, seconds);
			}
		});
	}
	
	public boolean exists(final String key) {
		return readFuncWrapper(new Callable<Boolean>() {
			
			@Override
			public Boolean call() {
				return getReadInstance().exists(key);
			}
		});
	}
	
	public Long rpush(final String key, final String value) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().rpush(key, value);
			}
		});
	}
	
	public Long rpush(final String key, final String...values) {
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				return getWriteInstance().rpush(key, values);
			}
		});
	}
	
	/**
	 * 阻塞的List pop
	 * @param key
	 * @return
	 */
	public List<String> blpop(final int timeout, final String... keys) {
		return writeFuncWrapper(new Callable<List<String>>() {

			@Override
			public List<String> call() {
				return getWriteInstance().blpop(timeout, keys);
			}
		});
	}
	
	/**
	 * 遍历set
	 * @param key
	 * @param cursor 只能为String，int版本被废弃
	 * @return
	 */
	public ScanResult<String> sscan(final String key, final String cursor) {
		return readFuncWrapper(new Callable<ScanResult<String>>() {

			@Override
			public ScanResult<String> call() {
				return getReadInstance().sscan(key, cursor);
			}
		});
	}
	
	public Long scard(final String key) {
		return readFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() throws Exception {
				return getReadInstance().scard(key);
			}
			
		});
	}
	
	
	public Boolean sismember(final String key, final String member) {
		return readFuncWrapper(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				return getReadInstance().sismember(key, member);
			}
		});
	}
	
	public List<String> lrange(final String key, final long start,
		    final long end) {
		return readFuncWrapper(new Callable<List<String>>() {

			@Override
			public List<String> call() {
				return getReadInstance().lrange(key, start, end);
			}
		});
	}
	
	/**
	 * 获取当前数据库的key数量
	 * @return
	 */
	public Long getDbSize() {
		return readFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() throws Exception {
				return getReadInstance().dbSize();
			}
			
		});
	}
	
	/**
	 * 在做incr操作之前，会先判断这个key是否存在，如果存在则直接返回incr的结果，
	 * 如果不存在，会在一个事务里面以当前的系统时间为基数创建这个key，然后再做返回incr的值，保证Id的唯一性。
	 * @param key
	 * @return
	 */
	public Long incrSecurity(final String key){
		
		return writeFuncWrapper(new Callable<Long>() {

			@Override
			public Long call() {
				Jedis redis = getWriteInstance();
				if (redis.exists(key)) {
					return redis.incr(key);
				} else {
					while (true) {
						if (redis.exists(key)) {
							return redis.incr(key);
						} else {
							redis.watch(key);
							Transaction tran = redis.multi();
							Long now = System.currentTimeMillis();
							tran.set(key, "" + now);
							List<Object> res = tran.exec();
							if (res.get(0) != null) {
								return now;
							}
						}
					}
				}
			}
		});
	}
	
	private boolean isDBConnectAlive(Jedis jedis){
		boolean res = true;
		try{
			jedis.ping();
		}catch(Exception e){
			LOGGER.error("redis isDBConnectAlive, {}", e);
			res = false;
		}
		return res;
	}
	
	/**
	 * 判断写库的连接是否还保持连接
	 * @return
	 */
	public boolean isWriteDBConnectAlive(){
		return writeFuncWrapper(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				return isDBConnectAlive(getWriteInstance());
			}
			
		});
	}
	
	/**
	 * 判断读库的连接是否还保持连接
	 * @return
	 */
	public boolean isReadDBConnectAlive(){
		return readFuncWrapper(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				return isDBConnectAlive(getReadInstance());
			}
			
		});
	}
	
	public boolean test(){
//		return readFuncWrapper(new Callable<Boolean>() {
//
//			@Override
//			public Boolean call() throws Exception {
				return isDBConnectAlive(getReadInstance());
//			}
//			
//		});
	}
}