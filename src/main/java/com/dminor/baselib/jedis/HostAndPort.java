package com.dminor.baselib.jedis;

 
/**
 * 
 * 
 * @author guoqingzhou
 * @date 2014-10-21
 */
 
public class HostAndPort {
    protected String host;
    protected int port;
    protected String name=""; //mymastername
    protected int vlid=1; //0=down,1=ok 
    
    protected int weight=1;  
  

    public String getHost() {
        return host;
    }
    public void setHost(String host) {
        this.host = host;
    }
    public int getPort() {
        return port;
    }
    public void setPort(int port) {
        this.port = port;
    } 
    
    public int getVlid() {
		return vlid;
	}
	public void setVlid(int vlid) {
		this.vlid = vlid;
	} 
	 
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	public HostAndPort() { } 
    
    public HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }
    public HostAndPort(String host, String port) {
        this.host = host;
        this.port = Integer.parseInt(port);
    }
    
    public HostAndPort(String host, String port,String name) {
        this.host = host;
        this.port = Integer.parseInt(port);
        this.name = name;
    }
    
    public HostAndPort(String host, String port,String name,int vlid) {
        this.host = host;
        this.port = Integer.parseInt(port);
        this.name = name;
        this.vlid =vlid;
    }
    public HostAndPort(String host, String port,String name,int vlid,int weight) {
        this.host = host;
        this.port = Integer.parseInt(port);
        this.name = name;
        this.vlid =vlid;
        this.weight=weight;
    }
    
    public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	@Override
    public boolean equals(Object obj) {
        if (obj instanceof HostAndPort) {
            HostAndPort hp = (HostAndPort) obj;
            return port == hp.port && host.equals(hp.host);
        }
        return false;
    }
 
  
	public String print() {
		return "HostAndPort [host=" + host + ", port=" + port + ", name="
				+ name + ", vlid=" + vlid +", weight=" + weight + "]";
	}
	
	@Override
	public String toString() {
        //must return host:port
        return host + ":" + port;
    }
}