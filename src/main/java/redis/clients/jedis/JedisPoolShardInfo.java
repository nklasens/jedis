package redis.clients.jedis;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

public class JedisPoolShardInfo extends ConnetionShardInfo<JedisPool> {
  
    private GenericObjectPool.Config poolConfig;

    public JedisPoolShardInfo(final GenericObjectPool.Config poolConfig,
          final String host) {
      this(poolConfig, new ConnectionInfo(host));
    }
    
    public JedisPoolShardInfo(String host, int port) {
      this(new GenericObjectPool.Config(), host, port);
    }
    
    public JedisPoolShardInfo(final GenericObjectPool.Config poolConfig, final String host, final int port) {
      this(poolConfig, new ConnectionInfo(host, port));
    }
    
    public JedisPoolShardInfo(final GenericObjectPool.Config poolConfig, final String host, int port, int timeout) {
      this(poolConfig, new ConnectionInfo(host, port, timeout));
    }

    public JedisPoolShardInfo(final GenericObjectPool.Config poolConfig, String host, int port, int timeout, int weight) {
      this(poolConfig, new ConnectionInfo(host, port, timeout), weight);
    }

    public JedisPoolShardInfo(Config poolConfig, ConnectionInfo connectionInfo) {
      super(connectionInfo);
      this.poolConfig = poolConfig;
    }

    public JedisPoolShardInfo(Config poolConfig, ConnectionInfo connectionInfo, int weight) {
      super(connectionInfo, weight);
      this.poolConfig = poolConfig;
    }

    public GenericObjectPool.Config getPoolConfig() {
      return poolConfig;
    }
  
    @Override
    protected JedisPool createResource() {
        return new JedisPool(this.getPoolConfig(), this.getConnectionInfo());
    }
}
