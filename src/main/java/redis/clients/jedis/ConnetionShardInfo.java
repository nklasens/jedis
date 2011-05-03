package redis.clients.jedis;

import redis.clients.util.ShardInfo;
import redis.clients.util.Sharded;

public abstract class ConnetionShardInfo<T> extends ShardInfo<T> {

    private ConnectionInfo connectionInfo;

    public ConnetionShardInfo(ConnectionInfo connectionInfo) {
      this(connectionInfo, Sharded.DEFAULT_WEIGHT);
    }
    
    public ConnetionShardInfo(ConnectionInfo connectionInfo, int weight) {
      super(weight);
      this.connectionInfo = connectionInfo;
    }

    public ConnectionInfo getConnectionInfo() {
      return connectionInfo;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionInfo == null) ? 0 : connectionInfo.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ConnetionShardInfo<?> other = (ConnetionShardInfo<?>) obj;
      if (connectionInfo == null) {
        if (other.connectionInfo != null)
          return false;
      } else if (!connectionInfo.equals(other.connectionInfo))
        return false;
      return true;
    }
}
