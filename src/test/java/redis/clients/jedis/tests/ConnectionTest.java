package redis.clients.jedis.tests;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionInfo;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class ConnectionTest extends Assert {
    private Connection client;

    @After
    public void tearDown() throws Exception {
        client.disconnect();
    }

    @Test(expected = JedisConnectionException.class)
    public void checkUnkownHost() {
        client = new Connection(new ConnectionInfo("someunknownhost"));
        client.connect();
    }

    @Test(expected = JedisConnectionException.class)
    public void checkWrongPort() {
        client = new Connection(new ConnectionInfo("localhost", 55665));
        client.connect();
    }
}