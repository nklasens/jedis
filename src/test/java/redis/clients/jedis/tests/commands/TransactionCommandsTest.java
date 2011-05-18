package redis.clients.jedis.tests.commands;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.Protocol.Keyword;
import redis.clients.jedis.exceptions.JedisDataException;

public class TransactionCommandsTest extends JedisCommandTestBase {
    final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
    final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
    final byte[] ba = { 0x0A };
    final byte[] bb = { 0x0B };

    final byte[] bmykey = { 0x42, 0x02, 0x03, 0x04 };

    Jedis nj;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        nj = new Jedis(hnp.host, hnp.port, 500);
        nj.connect();
        nj.auth("foobared");
        nj.flushAll();
    }

    @Test
    public void multi() {
        Transaction trans = jedis.multi();

        trans.sadd("foo", "a");
        trans.sadd("foo", "b");
        trans.scard("foo");

        List<Object> response = trans.exec();

        List<Object> expected = new ArrayList<Object>();
        expected.add(1L);
        expected.add(1L);
        expected.add(2L);
        assertEquals(expected, response);

        // Binary
        trans = jedis.multi();

        trans.sadd(bfoo, ba);
        trans.sadd(bfoo, bb);
        trans.scard(bfoo);

        response = trans.exec();

        expected = new ArrayList<Object>();
        expected.add(1L);
        expected.add(1L);
        expected.add(2L);
        assertEquals(expected, response);

    }

    @Test
    public void multiBlock() {
        List<Object> response = jedis.multi(new TransactionBlock() {
            @Override
            public void execute() {
                sadd("foo", "a");
                sadd("foo", "b");
                scard("foo");
            }
        });

        List<Object> expected = new ArrayList<Object>();
        expected.add(1L);
        expected.add(1L);
        expected.add(2L);
        assertEquals(expected, response);

        // Binary
        response = jedis.multi(new TransactionBlock() {
            @Override
            public void execute() {
                sadd(bfoo, ba);
                sadd(bfoo, bb);
                scard(bfoo);
            }
        });

        expected = new ArrayList<Object>();
        expected.add(1L);
        expected.add(1L);
        expected.add(2L);
        assertEquals(expected, response);

    }

    @Test
    public void watch() throws UnknownHostException, IOException {
        jedis.watch("mykey", "somekey");
        Transaction t = jedis.multi();

        nj.connect();
        nj.auth("foobared");
        nj.set("mykey", "bar");
        nj.disconnect();

        t.set("mykey", "foo");
        List<Object> resp = t.exec();
        assertEquals(null, resp);
        assertEquals("bar", jedis.get("mykey"));

        // Binary
        jedis.watch(bmykey);
        t = jedis.multi();

        nj.connect();
        nj.auth("foobared");
        nj.set(bmykey, bbar);
        nj.disconnect();

        t.set(bmykey, bfoo);
        resp = t.exec();
        assertEquals(null, resp);
        assertTrue(Arrays.equals(bbar, jedis.get(bmykey)));
    }

    @Test
    public void unwatch() throws UnknownHostException, IOException {
        jedis.watch("mykey");
        String val = jedis.get("mykey");
        val = "foo";
        String status = jedis.unwatch();
        assertEquals("OK", status);
        Transaction t = jedis.multi();

        nj.connect();
        nj.auth("foobared");
        nj.set("mykey", "bar");
        nj.disconnect();

        t.set("mykey", val);
        List<Object> resp = t.exec();
        assertEquals(1, resp.size());
        assertEquals("OK", resp.get(0));

        // Binary
        jedis.watch(bmykey);
        byte[] bval = jedis.get(bmykey);
        bval = bfoo;
        status = jedis.unwatch();
        assertEquals(Keyword.OK.name(), status);
        t = jedis.multi();

        nj.connect();
        nj.auth("foobared");
        nj.set(bmykey, bbar);
        nj.disconnect();

        t.set(bmykey, bval);
        resp = t.exec();
        assertEquals(1, resp.size());
        assertEquals("OK", resp.get(0));
    }

    @Test(expected = JedisDataException.class)
    public void validateWhenInMulti() {
        jedis.multi();
        jedis.ping();
    }

    @Test
    public void discard() {
        Transaction t = jedis.multi();
        String status = t.discard();
        assertEquals("OK", status);
    }

    @Test
    public void transactionResponse() {
        jedis.set("string", "foo");
        jedis.lpush("list", "foo");
        jedis.hset("hash", "foo", "bar");
        jedis.zadd("zset", 1, "foo");
        jedis.sadd("set", "foo");

        Transaction t = jedis.multi();
        Response<String> string = t.get("string");
        Response<String> list = t.lpop("list");
        Response<String> hash = t.hget("hash", "foo");
        Response<Set<String>> zset = t.zrange("zset", 0, -1);
        Response<String> set = t.spop("set");
        t.exec();

        assertEquals("foo", string.get());
        assertEquals("foo", list.get());
        assertEquals("bar", hash.get());
        assertEquals("foo", zset.get().iterator().next());
        assertEquals("foo", set.get());
    }

    @Test(expected = JedisDataException.class)
    public void transactionResponseWithinPipeline() {
        jedis.set("string", "foo");

        Transaction t = jedis.multi();
        Response<String> string = t.get("string");
        string.get();
        t.exec();
    }

    @Test
    public void lala() {
        Transaction multi = jedis.multi();
        Response<Long> publish = multi.publish("foo", "bar");
        Response<Long> bpublish = multi.publish("foo".getBytes(), "bar"
                .getBytes());
        multi.exec();
        assertEquals(0, publish.get().longValue());
        assertEquals(0, bpublish.get().longValue());
    }
}