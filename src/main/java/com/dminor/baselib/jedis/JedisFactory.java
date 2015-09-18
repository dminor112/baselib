package com.dminor.baselib.jedis;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool.BasePoolableObjectFactory; 
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.Jedis; 
/**
 *  继承自BasePoolableObjectFactory，只实现了3个方法
* makeObject(),连接，new Socket()
* destroyObject()--断开连接,
* validateObject()--ping
 * @author guoqingzhou
 *
 */
public class JedisFactory extends BasePoolableObjectFactory<Jedis> {
	private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<HostAndPort>();
	private final int timeout;
	private final String password;
	private final int database;
	private final String clientName; 
 

	public JedisFactory(String host, int port, int timeout, String password,
			int database) {
		this(host, port, timeout, password, database, null);
	}

	public JedisFactory(final String host, final int port,final  int timeout,final String password,
			final int database,final String clientName) {
		 super();
		this.hostAndPort.set(new HostAndPort(host, port));
		this.timeout = timeout;
		this.password = password;
		this.database = database;
		this.clientName = clientName;
	}

	public void setHostAndPort(HostAndPort hostAndPort) {
		this.hostAndPort.set(hostAndPort);
	}
    @Override
	public void activateObject(Jedis obj)
			throws Exception { 
		if (obj instanceof Jedis) {
            final Jedis jedis = (Jedis)obj;
            if (jedis.getDB().longValue() != database) {
                jedis.select(database);
            } 
        }
	}
    
    @Override
	public void destroyObject(final Jedis obj) throws Exception {
		 if (obj instanceof Jedis) {
	            final Jedis jedis = (Jedis) obj;
	            if (jedis.isConnected()) {
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
    
    @Override
	public Jedis  makeObject() throws Exception {
		HostAndPort hostAndPort = (HostAndPort) this.hostAndPort.get();
		final Jedis jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(),this.timeout);

		jedis.connect();
		if (null != this.password) {
			jedis.auth(this.password);
		}
		if (this.database != 0) {
			jedis.select(this.database);
		}
		if (this.clientName != null) {
			jedis.clientSetname(this.clientName);
		}  
        return jedis; 
	}

    @Override
	public boolean validateObject(final Jedis obj) {
		if (obj instanceof Jedis) {
            final Jedis jedis = (Jedis) obj;
            try {
              //   return jedis.isConnected();
              /* 而TwemProxy本身是不支持ping命令的。 
               *    解决方法：1，暂且设置testOnBorrow=false(默认值,其实还是不够的，
               *    因为TimerTask还是会去校验IDLE对象,
               *    如果真要这么干还要设置.testWhileIdle=false,后台不会校验到)
               *  2，修改Proxy是器支持ping命令
               */
       
                return jedis.isConnected() && jedis.ping().equals("PONG");  
            } catch (final Exception e) {
                return false;
            }
        } else {
            return false;
        }
	}
}