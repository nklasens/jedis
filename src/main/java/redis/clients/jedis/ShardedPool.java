package redis.clients.jedis;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Hashing;
import redis.clients.util.Sharded;

public class ShardedPool extends Sharded<JedisPool, JedisPoolShardInfo> {

  private Map<ConnectionInfo,JedisPoolShardInfo> shardInfoLookup = new HashMap<ConnectionInfo, JedisPoolShardInfo>();
  
  public ShardedPool(GenericObjectPool.Config poolConfig, List<JedisShardInfo> shards) {
    super(createPooledShards(poolConfig, shards));
    initLookup();
  }

  public ShardedPool(GenericObjectPool.Config poolConfig, List<JedisShardInfo> shards, Hashing algo) {
    super(createPooledShards(poolConfig, shards), algo);
    initLookup();
  }

  public ShardedPool(GenericObjectPool.Config poolConfig, List<JedisShardInfo> shards, Pattern tagPattern) {
    super(createPooledShards(poolConfig, shards), tagPattern);
    initLookup();
  }

  public ShardedPool(GenericObjectPool.Config poolConfig, List<JedisShardInfo> shards, Hashing algo, Pattern tagPattern) {
    super(createPooledShards(poolConfig, shards), algo, tagPattern);
    initLookup();
  }

  private static List<JedisPoolShardInfo> createPooledShards(GenericObjectPool.Config poolConfig,  List<JedisShardInfo> shards) {
    List<JedisPoolShardInfo> pooledShards = new ArrayList<JedisPoolShardInfo>(shards.size()); 
    for(JedisShardInfo shardInfo : shards){
      pooledShards.add(new JedisPoolShardInfo(shardInfo, poolConfig));
    }
    return pooledShards;
  }

  private void initLookup() {
    for (JedisPoolShardInfo shardInfo : getAllShardInfo()) {
      shardInfoLookup.put(shardInfo.getConnectionInfo(), shardInfo);
    }
  }

  public Jedis getResource(String key) {
    try {
      JedisPool pool = getShard(key);
      return pool.getResource();
    } catch (Exception e) {
      throw new JedisConnectionException("Could not get a resource from the pool", e);
    }
  }

  public Jedis getResource(byte[] key) {
    try {
      JedisPool pool = getShard(key);
      return pool.getResource();
    } catch (Exception e) {
      throw new JedisConnectionException("Could not get a resource from the pool", e);
    }
  }

  public void returnResource(final Jedis resource) {
    try {
      JedisPool pool = findPool(resource);
      pool.returnResource(resource);
    } catch (Exception e) {
      throw new JedisException("Could not return the resource to the pool", e);
    }
  }

  public void returnBrokenResource(final Jedis resource) {
    try {
      JedisPool pool = findPool(resource);
      pool.returnBrokenResource(resource);
    } catch (Exception e) {
      throw new JedisException("Could not return the resource to the pool", e);
    }
  }

  protected JedisPool findPool(Jedis resource) {
    JedisPoolShardInfo shardInfo = shardInfoLookup.get(resource.getClient().getConnectionInfo());
    if (shardInfo == null) {
      throw new JedisException("Could not find pool. ");
    }
    return getShard(shardInfo);
  }

  public void destroy() {
    Exception lastException = null;
    for (JedisPool pool : this.getAllShards()) {
      try {
        pool.destroy();
      } catch (Exception e) {
        lastException = e;
      }
    }

    if (lastException != null) {
      throw new JedisException("Could not destroy some pools", lastException);
    }
  }

}
