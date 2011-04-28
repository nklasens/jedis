package redis.clients.jedis;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.util.Pool;

public class JedisPool extends Pool<Jedis> {

    public JedisPool(final GenericObjectPool.Config poolConfig,
            final String host) {
        this(poolConfig, host, Protocol.DEFAULT_PORT, Protocol.DEFAULT_TIMEOUT,
                null);
    }

    public JedisPool(String host, int port) {
        super(new GenericObjectPool.Config(), new JedisFactory(host, port,
                Protocol.DEFAULT_TIMEOUT, null));
    }

    public JedisPool(final GenericObjectPool.Config poolConfig, final String host, int port,
            int timeout, final String password) {
        super(poolConfig, new JedisFactory(host, port, timeout, password));
    }

    public JedisPool(final GenericObjectPool.Config poolConfig,
            final String host, final int port) {
        this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, null);
    }

    public JedisPool(final GenericObjectPool.Config poolConfig,
            final String host, final int port, final int timeout) {
        this(poolConfig, host, port, timeout, null);
    }

    /**
     * PoolableObjectFactory custom impl.
     */
    private static class JedisFactory extends BasePoolableObjectFactory {
        private final String host;
        private final int port;
        private final int timeout;
        private final String password;

        public JedisFactory(final String host, final int port,
                final int timeout, final String password) {
            super();
            this.host = host;
            this.port = port;
            this.timeout = (timeout > 0) ? timeout : -1;
            this.password = password;
        }

        public Object makeObject() throws Exception {
            final Jedis jedis;
            if (timeout > 0) {
                jedis = new Jedis(this.host, this.port, this.timeout, this.password);
            } else {
                jedis = new Jedis(this.host, this.port, this.password);
            }

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
