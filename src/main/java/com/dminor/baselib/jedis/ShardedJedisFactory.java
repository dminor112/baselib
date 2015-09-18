package com.dminor.baselib.jedis;

import java.util.List;
import java.util.regex.Pattern;
 
import org.apache.commons.pool.BasePoolableObjectFactory;

 
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis; 
import redis.clients.util.Hashing;

/**
 * 
 * 
 * @author guoqingzhou
 * @date 2015-03-25
 */
public class ShardedJedisFactory extends BasePoolableObjectFactory<ShardedJedis> {
	private List<JedisShardInfo> shards;
	private Hashing algo;
	private Pattern keyTagPattern;

	public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo,
			Pattern keyTagPattern) {
		this.shards = shards;
		this.algo = algo;
		this.keyTagPattern = keyTagPattern;
	}

	public   ShardedJedis  makeObject() throws Exception {
		 final ShardedJedis jedis = new ShardedJedis(shards, algo, keyTagPattern);
		return jedis;
	}
	@Override
    public void destroyObject(final ShardedJedis obj) throws Exception  { 
    	if ((obj != null) && (obj instanceof ShardedJedis)) {
			ShardedJedis shardedJedis = (ShardedJedis) obj;
			for (Jedis jedis : shardedJedis.getAllShards()) {
				try {
					try {
						jedis.quit();
					} catch (Exception e) {

					}
					jedis.disconnect();
				} catch (Exception e) {

				}
			}
		}
	
    }
    
 
    /**
     *  No-op.
     *
     *  @param p ignored
     */
    public void activateObject(final ShardedJedis obj)
			throws Exception {
    	if (obj instanceof ShardedJedis) {
    		//final ShardedJedis shardedJedis = (ShardedJedis) obj;
            
        }
	}

	public boolean validateObject(final ShardedJedis obj) {
		boolean f=false;
		try {
			final ShardedJedis jedis = (ShardedJedis) obj;
			for (Jedis shard : jedis.getAllShards()) {
			    f= shard.isConnected(); 
				if (!shard.ping().equals("PONG")) {
					return false;
				}
			}
			return true;
		} catch (Exception ex) {
			return f;
		}
	} 
}