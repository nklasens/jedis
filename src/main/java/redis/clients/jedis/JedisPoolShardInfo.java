package redis.clients.jedis;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.util.ShardInfo;

public class JedisPoolShardInfo extends ShardInfo<JedisPool>{

  private final JedisShardInfo shardInfo;
  private final Config poolConfig;
  
  public JedisPoolShardInfo(JedisShardInfo shardInfo, Config poolConfig){
    super(shardInfo.getWeight(), shardInfo.getIdentifier());
    
    this.shardInfo = shardInfo;
    this.poolConfig = poolConfig;
  }
  
  public JedisShardInfo getShardInfo() {
    return shardInfo;
  }

  public ConnectionInfo getConnectionInfo() {
    return shardInfo.getConnectionInfo();
  }

  @Override
  protected JedisPool createResource() {
    return new JedisPool(poolConfig, shardInfo.getConnectionInfo());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + shardInfo.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    JedisPoolShardInfo other = (JedisPoolShardInfo) obj;
    if (shardInfo != other.shardInfo) {
      return false;
    }
    return true;
  }
  
}
