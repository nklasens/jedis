package redis.clients.jedis;

import redis.clients.jedis.BinaryClient.LIST_POSITION;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class ShardedJedisPipeline {
    private ShardedJedis jedis;
    private List<FutureResult> results = new ArrayList<FutureResult>();

    private class FutureResult {
        private Client client;

        public FutureResult(Client client) {
            this.client = client;
        }

        public Object get() {
            return client.getOne();
        }
    }

    public void setShardedJedis(ShardedJedis jedis) {
        this.jedis = jedis;
    }

    protected Client getClient(String key) {
      return jedis.getShard(key).getClient();
    }

    protected void set(String key, String value) {
        Client c = getClient(key);
        c.set(key, value);
        results.add(new FutureResult(c));
    }

    protected void get(String key) {
        Client c = getClient(key);
        c.get(key);
        results.add(new FutureResult(c));
    }

    protected void exists(String key) {
        Client c = getClient(key);
        c.exists(key);
        results.add(new FutureResult(c));
    }

    protected void type(String key) {
        Client c = getClient(key);
        c.type(key);
        results.add(new FutureResult(c));
    }

    protected void expire(String key, int seconds) {
        Client c = getClient(key);
        c.expire(key, seconds);
        results.add(new FutureResult(c));
    }

    protected void expireAt(String key, long unixTime) {
        Client c = getClient(key);
        c.expireAt(key, unixTime);
        results.add(new FutureResult(c));
    }

    protected void ttl(String key) {
        Client c = getClient(key);
        c.ttl(key);
        results.add(new FutureResult(c));
    }

    protected void getSet(String key, String value) {
        Client c = getClient(key);
        c.getSet(key, value);
        results.add(new FutureResult(c));
    }

    protected void setnx(String key, String value) {
        Client c = getClient(key);
        c.setnx(key, value);
        results.add(new FutureResult(c));
    }

    protected void setex(String key, int seconds, String value) {
        Client c = getClient(key);
        c.setex(key, seconds, value);
        results.add(new FutureResult(c));
    }

    protected void decrBy(String key, int integer) {
        Client c = getClient(key);
        c.decrBy(key, integer);
        results.add(new FutureResult(c));
    }

    protected void decr(String key) {
        Client c = getClient(key);
        c.decr(key);
        results.add(new FutureResult(c));
    }

    protected void incrBy(String key, int integer) {
        Client c = getClient(key);
        c.incrBy(key, integer);
        results.add(new FutureResult(c));
    }

    protected void incr(String key) {
        Client c = getClient(key);
        c.incr(key);
        results.add(new FutureResult(c));
    }

    protected void append(String key, String value) {
        Client c = getClient(key);
        c.append(key, value);
        results.add(new FutureResult(c));
    }

    protected void substr(String key, int start, int end) {
        Client c = getClient(key);
        c.substr(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void hset(String key, String field, String value) {
        Client c = getClient(key);
        c.hset(key, field, value);
        results.add(new FutureResult(c));
    }

    protected void hget(String key, String field) {
        Client c = getClient(key);
        c.hget(key, field);
        results.add(new FutureResult(c));
    }

    protected void hsetnx(String key, String field, String value) {
        Client c = getClient(key);
        c.hsetnx(key, field, value);
        results.add(new FutureResult(c));
    }

    protected void hmset(String key, Map<String, String> hash) {
        Client c = getClient(key);
        c.hmset(key, hash);
        results.add(new FutureResult(c));
    }

    protected void hmget(String key, String... fields) {
        Client c = getClient(key);
        c.hmget(key, fields);
        results.add(new FutureResult(c));
    }

    protected void hincrBy(String key, String field, int value) {
        Client c = getClient(key);
        c.hincrBy(key, field, value);
        results.add(new FutureResult(c));
    }

    protected void hexists(String key, String field) {
        Client c = getClient(key);
        c.hexists(key, field);
        results.add(new FutureResult(c));
    }

    protected void hdel(String key, String field) {
        Client c = getClient(key);
        c.hdel(key, field);
        results.add(new FutureResult(c));
    }

    protected void hlen(String key) {
        Client c = getClient(key);
        c.hlen(key);
        results.add(new FutureResult(c));
    }

    protected void hkeys(String key) {
        Client c = getClient(key);
        c.hkeys(key);
        results.add(new FutureResult(c));
    }

    protected void hvals(String key) {
        Client c = getClient(key);
        c.hvals(key);
        results.add(new FutureResult(c));
    }

    protected void hgetAll(String key) {
        Client c = getClient(key);
        c.hgetAll(key);
        results.add(new FutureResult(c));
    }

    protected void rpush(String key, String string) {
        Client c = getClient(key);
        c.rpush(key, string);
        results.add(new FutureResult(c));
    }

    protected void lpush(String key, String string) {
        Client c = getClient(key);
        c.lpush(key, string);
        results.add(new FutureResult(c));
    }

    protected void llen(String key) {
        Client c = getClient(key);
        c.llen(key);
        results.add(new FutureResult(c));
    }

    protected void lrange(String key, int start, int end) {
        Client c = getClient(key);
        c.lrange(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void ltrim(String key, int start, int end) {
        Client c = getClient(key);
        c.ltrim(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void lindex(String key, int index) {
        Client c = getClient(key);
        c.lindex(key, index);
        results.add(new FutureResult(c));
    }

    protected void lset(String key, int index, String value) {
        Client c = getClient(key);
        c.lset(key, index, value);
        results.add(new FutureResult(c));
    }

    protected void lrem(String key, int count, String value) {
        Client c = getClient(key);
        c.lrem(key, count, value);
        results.add(new FutureResult(c));
    }

    protected void lpop(String key) {
        Client c = getClient(key);
        c.lpop(key);
        results.add(new FutureResult(c));
    }

    protected void rpop(String key) {
        Client c = getClient(key);
        c.rpop(key);
        results.add(new FutureResult(c));
    }

    protected void sadd(String key, String member) {
        Client c = getClient(key);
        c.sadd(key, member);
        results.add(new FutureResult(c));
    }

    protected void smembers(String key) {
        Client c = getClient(key);
        c.smembers(key);
        results.add(new FutureResult(c));
    }

    protected void srem(String key, String member) {
        Client c = getClient(key);
        c.srem(key, member);
        results.add(new FutureResult(c));
    }

    protected void spop(String key) {
        Client c = getClient(key);
        c.spop(key);
        results.add(new FutureResult(c));
    }

    protected void scard(String key) {
        Client c = getClient(key);
        c.scard(key);
        results.add(new FutureResult(c));
    }

    protected void sismember(String key, String member) {
        Client c = getClient(key);
        c.sismember(key, member);
        results.add(new FutureResult(c));
    }

    protected void srandmember(String key) {
        Client c = getClient(key);
        c.srandmember(key);
        results.add(new FutureResult(c));
    }

    protected void zadd(String key, double score, String member) {
        Client c = getClient(key);
        c.zadd(key, score, member);
        results.add(new FutureResult(c));
    }

    protected void zrange(String key, int start, int end) {
        Client c = getClient(key);
        c.zrange(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void zrem(String key, String member) {
        Client c = getClient(key);
        c.zrem(key, member);
        results.add(new FutureResult(c));
    }

    protected void zincrby(String key, double score, String member) {
        Client c = getClient(key);
        c.zincrby(key, score, member);
        results.add(new FutureResult(c));
    }

    protected void zrank(String key, String member) {
        Client c = getClient(key);
        c.zrank(key, member);
        results.add(new FutureResult(c));
    }

    protected void zrevrank(String key, String member) {
        Client c = getClient(key);
        c.zrevrank(key, member);
        results.add(new FutureResult(c));
    }

    protected void zrevrange(String key, int start, int end) {
        Client c = getClient(key);
        c.zrevrange(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void zrangeWithScores(String key, int start, int end) {
        Client c = getClient(key);
        c.zrangeWithScores(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void zrevrangeWithScores(String key, int start, int end) {
        Client c = getClient(key);
        c.zrevrangeWithScores(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void zcard(String key) {
        Client c = getClient(key);
        c.zcard(key);
        results.add(new FutureResult(c));
    }

    protected void zscore(String key, String member) {
        Client c = getClient(key);
        c.zscore(key, member);
        results.add(new FutureResult(c));
    }

    protected void sort(String key) {
        Client c = getClient(key);
        c.sort(key);
        results.add(new FutureResult(c));
    }

    protected void sort(String key, SortingParams sortingParameters) {
        Client c = getClient(key);
        c.sort(key, sortingParameters);
        results.add(new FutureResult(c));
    }

    protected void zcount(String key, double min, double max) {
        Client c = getClient(key);
        c.zcount(key, min, max);
        results.add(new FutureResult(c));
    }

    protected void zrangeByScore(String key, double min, double max) {
        Client c = getClient(key);
        c.zrangeByScore(key, min, max);
        results.add(new FutureResult(c));
    }

    protected void zrangeByScore(String key, double min, double max,
            int offset, int count) {
        Client c = getClient(key);
        c.zrangeByScore(key, min, max, offset, count);
        results.add(new FutureResult(c));
    }

    protected void zrangeByScoreWithScores(String key, double min, double max) {
        Client c = getClient(key);
        c.zrangeByScoreWithScores(key, min, max);
        results.add(new FutureResult(c));
    }

    protected void zrangeByScoreWithScores(String key, double min, double max,
            int offset, int count) {
        Client c = getClient(key);
        c.zrangeByScoreWithScores(key, min, max, offset, count);
        results.add(new FutureResult(c));
    }

    protected void zremrangeByRank(String key, int start, int end) {
        Client c = getClient(key);
        c.zremrangeByRank(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void zremrangeByScore(String key, double start, double end) {
        Client c = getClient(key);
        c.zremrangeByScore(key, start, end);
        results.add(new FutureResult(c));
    }

    protected void linsert(String key, LIST_POSITION where, String pivot,
            String value) {
        Client c = getClient(key);
        c.linsert(key, where, pivot, value);
        results.add(new FutureResult(c));
    }

    protected void getbit(String key, long offset) {
        Client c = jedis.getShard(key).getClient();
        c.getbit(key, offset);
        results.add(new FutureResult(c));
    }

    public void setbit(String key, long offset, boolean value) {
        Client c = jedis.getShard(key).getClient();
        c.setbit(key, offset, value);
        results.add(new FutureResult(c));
    }

    public void setrange(String key, long offset, String value) {
        Client c = jedis.getShard(key).getClient();
        c.setrange(key, offset, value);
        results.add(new FutureResult(c));
    }

    public void getrange(String key, long startOffset, long endOffset) {
        Client c = jedis.getShard(key).getClient();
        c.getrange(key, startOffset, endOffset);
        results.add(new FutureResult(c));
    }

    public List<Object> getResults() {
        List<Object> r = new ArrayList<Object>();
        for (FutureResult fr : results) {
            r.add(fr.get());
        }
        return r;
    }

    public abstract void execute();
}