package redis.clients.jedis.tests;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.tests.commands.JedisCommandTestBase;
import redis.clients.util.SafeEncoder;

public class JedisTest extends JedisCommandTestBase {
    @Test
    public void useWithoutConnecting() {
        Jedis jedis = new Jedis("localhost");
        jedis.auth("foobared");
        jedis.dbSize();
    }

    @Test
    public void checkBinaryData() {
        byte[] bigdata = new byte[1777];
        for (int b = 0; b < bigdata.length; b++) {
            bigdata[b] = (byte) ((byte) b % 255);
        }
        Map<String, String> hash = new HashMap<String, String>();
        hash.put("data", SafeEncoder.encode(bigdata));

        String status = jedis.hmset("foo", hash);
        assertEquals("OK", status);
        assertEquals(hash, jedis.hgetAll("foo"));
    }

    @Test(expected = JedisConnectionException.class)
    public void timeoutConnection() throws Exception {
        jedis = new Jedis("localhost", 6379, 15000);
        jedis.auth("foobared");
        jedis.configSet("timeout", "1");
        // we need to sleep a long time since redis check for idle connections
        // every 10 seconds or so
        Thread.sleep(20000);
        jedis.hmget("foobar", "foo");
    }
}