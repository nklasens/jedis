package redis.clients.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import redis.clients.jedis.exceptions.JedisException;

public class Sharded<R, S extends ShardInfo<R>> {

    public static final int DEFAULT_WEIGHT = 1;
    private TreeMap<Long, S> nodes;
    private final Hashing algo;
    private final Map<ShardInfo<R>, R> resources = new LinkedHashMap<ShardInfo<R>, R>();

    /**
     * The default pattern used for extracting a key tag. The pattern must have
     * a group (between parenthesis), which delimits the tag to be hashed. A
     * null pattern avoids applying the regular expression for each lookup,
     * improving performance a little bit is key tags aren't being used.
     */
    private Pattern tagPattern = null;
    // the tag is anything between {}
    public static final Pattern DEFAULT_KEY_TAG_PATTERN = Pattern
            .compile("\\{(.+?)\\}");

    public Sharded(List<S> shards) {
        this(shards, Hashing.MURMUR_HASH); // MD5 is really not good as we works
        // with 64-bits not 128
    }

    public Sharded(List<S> shards, Hashing algo) {
        this.algo = algo;
        initialize(shards);
    }

    public Sharded(List<S> shards, Pattern tagPattern) {
        this(shards, Hashing.MURMUR_HASH, tagPattern); // MD5 is really not good
        // as we works with
        // 64-bits not 128
    }

    public Sharded(List<S> shards, Hashing algo, Pattern tagPattern) {
        this.algo = algo;
        this.tagPattern = tagPattern;
        initialize(shards);
    }

    private void initialize(List<S> shards) {
        nodes = new TreeMap<Long, S>();

        if (this.algo == Hashing.KETAMA) {
          createKetamaContinuum(shards);
        }
        else {
          createContinuum(shards);
        }

        for (S shardInfo : shards) {
          resources.put(shardInfo, shardInfo.createResource());
        }
    }

    protected String createContinuumId(final S shardInfo, int shardPosition, int repetition) {
      String shardId = shardInfo.getName();
      if (shardId == null) {
        shardId = "SHARD-" + shardPosition + "-NODE";
      }
      return shardId + "-" + repetition;
    }

    protected void createContinuum(List<S> shards) {
      for (int shardPosition = 0; shardPosition != shards.size(); ++shardPosition) {
        final S shardInfo = shards.get(shardPosition);
        for (int repetition = 0; repetition < 160 * shardInfo.getWeight(); repetition++) {
            String continuumid = createContinuumId(shardInfo, shardPosition, repetition);
            nodes.put(this.algo.hash(continuumid), shardInfo);
        }
      }
    }

    /**
     * ketama - a consistent hashing algo
     * libketama compatible implementation
     * http://www.last.fm/user/RJ/journal/2007/04/10/rz_libketama_-_a_consistent_hashing_algo_for_memcache_clients
     * 
     * Notice the difference with {@link #createContinuum(List)}. 
     * The repetition is calculated differently. Evenly weighted shards are for ketama max 40 and not 160
     */
    protected void createKetamaContinuum(List<S> shards) {
      if (this.algo != Hashing.KETAMA) {
        throw new JedisException("KetamaContinuum requires KETAMA algo");
      }
      int totalWeight = 0;
      for (S shardInfo : shards) {
        totalWeight += shardInfo.getWeight() <= 0 ? DEFAULT_WEIGHT : shardInfo.getWeight();
      }

      for (int i = 0; i != shards.size(); ++i) {
        final S shardInfo = shards.get(i);
        int weight = shardInfo.getWeight() <= 0 ? DEFAULT_WEIGHT : shardInfo.getWeight();
        double factor = Math.floor(((double) (40 * shards.size() * weight)) / (double) totalWeight);

        for (int repetition = 0; repetition < factor; repetition++) {
          String continuumid = createContinuumId(shardInfo, i, repetition);
          byte[] d = Hashing.KETAMA.hashBytes(continuumid);
          for (int h = 0; h < 4; h++) {
            Long k = ((long) (d[3 + h * 4] & 0xFF) << 24)
                   | ((long) (d[2 + h * 4] & 0xFF) << 16)
                   | ((long) (d[1 + h * 4] & 0xFF) << 8)
                   | ((long) (d[0 + h * 4] & 0xFF));
            nodes.put(k, shardInfo);
          }
        }
      }
    }

    public R getShard(byte[] key) {
        return getShard(getShardInfo(key));
    }

    public R getShard(String key) {
      return getShard(getShardInfo(key));
    }

    protected R getShard(S shardInfo) {
        return resources.get(shardInfo);
    }

    public S getShardInfo(byte[] key) {
        SortedMap<Long, S> tail = nodes.tailMap(algo.hash(key));
        if (tail.size() == 0) {
            return nodes.get(nodes.firstKey());
        }
        return tail.get(tail.firstKey());
    }

    public S getShardInfo(String key) {
        return getShardInfo(SafeEncoder.encode(getKeyTag(key)));
    }

    /**
     * A key tag is a special pattern inside a key that, if preset, is the only
     * part of the key hashed in order to select the server for this key.
     *
     * @see http://code.google.com/p/redis/wiki/FAQ#I
     *      'm_using_some_form_of_key_hashing_for_partitioning,_but_wh
     * @param key
     * @return The tag if it exists, or the original key
     */
    public String getKeyTag(String key) {
        if (tagPattern != null) {
            Matcher m = tagPattern.matcher(key);
            if (m.find())
                return m.group(1);
        }
        return key;
    }

    public Collection<S> getAllShardInfo() {
        return Collections.unmodifiableCollection(nodes.values());
    }

    public Collection<R> getAllShards() {
        return Collections.unmodifiableCollection(resources.values());
    }
}

