package redis.clients.jedis;

import org.apache.commons.pool.impl.GenericObjectPool;

public class JedisPoolShardInfo extends ConnetionShardInfo<JedisPool> {
  
    private GenericObjectPool.Config poolConfig;

    public JedisPoolShardInfo(final GenericObjectPool.Config poolConfig,
          final String host) {
      super(host);
      this.poolConfig = poolConfig;
    }
    
    public JedisPoolShardInfo(String host, int port) {
      this(new GenericObjectPool.Config(), host, port);
    }
    
    public JedisPoolShardInfo(final GenericObjectPool.Config poolConfig,
          final String host, final int port) {
      super(host, port);
      this.poolConfig = poolConfig;
    }
    
    public JedisPoolShardInfo(final GenericObjectPool.Config poolConfig, final String host, int port,
          int timeout) {
      super(host, port, timeout);
      this.poolConfig = poolConfig;
    }

    public JedisPoolShardInfo(final GenericObjectPool.Config poolConfig, String host, int port, int timeout, int weight) {
      super(host, port, timeout, weight);
      this.poolConfig = poolConfig;
    }

    public GenericObjectPool.Config getPoolConfig() {
      return poolConfig;
    }
  
    @Override
    protected JedisPool createResource() {
        return new JedisPool(this.getPoolConfig(), this.getHost(), this.getPort(), this.getTimeout(), this.getPassword());
    }
}
