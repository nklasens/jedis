package redis.clients.jedis;

import java.util.*;
import java.util.regex.Pattern;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Hashing;
import redis.clients.util.Sharded;

public class ShardedPool extends Sharded<JedisPool, JedisPoolShardInfo> {

  private Map<String,JedisPoolShardInfo> shardInfoLookup = new HashMap<String, JedisPoolShardInfo>();
  
  public ShardedPool(List<JedisPoolShardInfo> shards) {
    super(shards);
    initLookup(shards);
  }

  public ShardedPool(List<JedisPoolShardInfo> shards, Hashing algo) {
    super(shards, algo);
    initLookup(shards);
  }

  public ShardedPool(List<JedisPoolShardInfo> shards, Pattern tagPattern) {
    super(shards, tagPattern);
    initLookup(shards);
  }

  public ShardedPool(List<JedisPoolShardInfo> shards, Hashing algo, Pattern tagPattern) {
    super(shards, algo, tagPattern);
    initLookup(shards);
  }

  private void initLookup(List<JedisPoolShardInfo> shards) {
    for (JedisPoolShardInfo shardInfo : shards) {
      shardInfoLookup.put(shardInfo.getHost() + ":" + shardInfo.getPort(), shardInfo);
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
    JedisPoolShardInfo shardInfo = shardInfoLookup.get(resource.getClient().getHost() + ":" + resource.getClient().getPort());
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
