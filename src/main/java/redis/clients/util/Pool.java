package redis.clients.util;

import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public abstract class Pool<T> {
    private final GenericObjectPool internalPool;
    private long deadHostWait = -1L;
    private long deadHostMaxWait = -1L;

    private final ReentrantLock hostDeadLock = new ReentrantLock();
    private volatile long deadSince = -1L;
    private volatile long deadDuration = -1L;

    public Pool(final GenericObjectPool.Config poolConfig,
            PoolableObjectFactory factory) {
        this.internalPool = new GenericObjectPool(factory, poolConfig);
        if (poolConfig instanceof JedisPoolConfig) {
          JedisPoolConfig jedisPoolConfig = (JedisPoolConfig) poolConfig;
          deadHostWait = jedisPoolConfig.getDeadWait();
          deadHostMaxWait = jedisPoolConfig.getDeadMaxWait();
        }
    }

    @SuppressWarnings("unchecked")
    public T getResource() {
      if (deadHostWait <= 0) {
        try {
          return (T) internalPool.borrowObject();
        } catch(Exception e) {
          throw new JedisConnectionException(
              "Could not get a resource from the pool", e);
        }
      }
      else {
        hostDeadLock.lock();
        try {
          if (deadSince > -1 && (deadSince + deadDuration) > System.currentTimeMillis() ) {
              throw new JedisConnectionException(
                  "Could not get a resource from dead pool");
            }
        }
        finally {
          hostDeadLock.unlock();
        }

        T borrowedObject = null;
        try {
          borrowedObject = (T) internalPool.borrowObject();
        } catch(NoSuchElementException e) {
          throw new JedisConnectionException(
              "Could not get a resource from the pool", e);
        } catch (JedisConnectionException e) {
          hostDeadLock.lock();
          try {
            deadSince = System.currentTimeMillis();
            deadDuration = ( deadDuration > -1L ) ? (deadDuration * 2) : deadHostWait;
            if ( deadDuration > deadHostMaxWait ) {
              deadDuration = deadHostMaxWait;
            }
            internalPool.clear();
          }
          finally {
            hostDeadLock.unlock();
          }
          throw new JedisConnectionException("Pool is dead will wait " + deadDuration + "ms for retry", e);
        } catch (Exception e) {
          throw new JedisConnectionException(
              "Could not get a resource from the pool", e);
        }
        
        hostDeadLock.lock();
        try {
          deadSince = -1L;
          deadDuration = -1L;
        }
        finally {
          hostDeadLock.unlock();
        }
        return borrowedObject;
      }
    }

    public void returnResource(final T resource) {
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            throw new JedisException(
                    "Could not return the resource to the pool", e);
        }
    }

    public void returnBrokenResource(final T resource) {
        try {
            internalPool.invalidateObject(resource);
        } catch (Exception e) {
            throw new JedisException(
                    "Could not return the resource to the pool", e);
        }
    }

    public void destroy() {
        try {
            internalPool.close();
        } catch (Exception e) {
            throw new JedisException("Could not destroy the pool", e);
        }
    }
}