package redis.clients.jedis;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.util.Pool;

public class JedisPool extends Pool<Jedis> {

    public JedisPool(final GenericObjectPool.Config poolConfig, final String host) {
        this(poolConfig, new ConnectionInfo(host));
    }

    public JedisPool(String host, int port) {
        this(new GenericObjectPool.Config(), new ConnectionInfo(host, port));
    }

    public JedisPool(final GenericObjectPool.Config poolConfig, final String host, final int port) {
        this(poolConfig, new ConnectionInfo(host, port));
    }

    public JedisPool(final GenericObjectPool.Config poolConfig, final String host, final int port, final String password) {
        this(poolConfig, new ConnectionInfo(host, port, password));
    }
    
    public JedisPool(final GenericObjectPool.Config poolConfig, final String host, final int port, final String password, int database) {
        this(poolConfig, new ConnectionInfo(host, port, password, database));
    }

    public JedisPool(final GenericObjectPool.Config poolConfig, final String host, final int port, final int timeout) {
        this(poolConfig, new ConnectionInfo(host, port, timeout));
    }

    public JedisPool(final GenericObjectPool.Config poolConfig, final String host, int port, int timeout, final String password) {
        this(poolConfig, new ConnectionInfo(host, port, timeout, password));
    }

    public JedisPool(final GenericObjectPool.Config poolConfig, final String host, int port, int timeout, final String password, int database) {
        this(poolConfig, new ConnectionInfo(host, port, timeout, password, database));
    }

    public JedisPool(Config poolConfig, ConnectionInfo connectionInfo) {
        super(poolConfig, new JedisFactory(connectionInfo));
    }

    /**
     * PoolableObjectFactory custom impl.
     */
    private static class JedisFactory extends BasePoolableObjectFactory {
        private final ConnectionInfo connectionInfo;

        public JedisFactory(final ConnectionInfo connectionInfo) {
            super();
            this.connectionInfo = connectionInfo;
        }

        public Object makeObject() throws Exception {
            final Jedis jedis = new Jedis(connectionInfo);
            jedis.connect();
            return jedis;
        }

        public void destroyObject(final Object obj) throws Exception {
            if (obj instanceof Jedis) {
                final Jedis jedis = (Jedis) obj;
                if (jedis.isConnected()) {
                    try {
                        try {
                            jedis.quit();
                        } catch (Exception e) {
                        }
                        jedis.disconnect();
                    } catch (Exception e) {

                    }
                }
            }
        }

        public boolean validateObject(final Object obj) {
            if (obj instanceof Jedis) {
                final Jedis jedis = (Jedis) obj;
                try {
                    return jedis.isConnected() && jedis.ping().equals("PONG");
                } catch (final Exception e) {
                    return false;
                }
            } else {
                return false;
            }
        }

    }
}
