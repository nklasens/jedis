package redis.clients.jedis;

import redis.clients.util.ShardInfo;
import redis.clients.util.Sharded;

public abstract class ConnetionShardInfo<T> extends ShardInfo<T> {

    public String toString() {
      return host + ":" + port + "*" + getWeight();
    }
    
    private final int timeout;
    private final String host;
    private final int port;
    private String password = null;
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public ConnetionShardInfo(String host) {
        this(host, Protocol.DEFAULT_PORT);
    }
    
    public ConnetionShardInfo(String host, int port) {
        this(host, port, 2000);
    }
    
    public ConnetionShardInfo(String host, int port, int timeout) {
        this(host, port, timeout, Sharded.DEFAULT_WEIGHT);
    }
    
    public ConnetionShardInfo(String host, int port, int timeout, int weight) {
        super(weight);
        this.host = host;
        this.port = port;
        this.timeout = timeout;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String auth) {
        this.password = auth;
    }
    
    public int getTimeout() {
        return timeout;
    }

}
