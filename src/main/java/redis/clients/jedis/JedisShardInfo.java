/*
 * Copyright 2009-2010 MBTE Sweden AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redis.clients.jedis;

import redis.clients.util.ShardInfo;
import redis.clients.util.Sharded;

public class JedisShardInfo extends ShardInfo<Jedis> {
  
    private ConnectionInfo connectionInfo;

    public String toString() {
        return connectionInfo.toString() + "*" + getWeight();
    }

    public JedisShardInfo(String host, String name) {
    	this(new ConnectionInfo(host), name);
    }
    	 
    public JedisShardInfo(String host, int port) {
        this(new ConnectionInfo(host, port));
    }
    
    public JedisShardInfo(String host, int port, String name) {
    	this(new ConnectionInfo(host, port), name);
    }
    
    public JedisShardInfo(String host, int port, int timeout) {
        this(new ConnectionInfo(host, port, timeout));
    }
    
    public JedisShardInfo(String host, int port, int timeout, String name) {
    	this(new ConnectionInfo(host, port, timeout), name);
    }
    
    public JedisShardInfo(String host, int port, int timeout, int weight) {
      this(new ConnectionInfo(host, port, timeout), weight);
    }

    public JedisShardInfo(ConnectionInfo connectionInfo) {
      this(connectionInfo, Sharded.DEFAULT_WEIGHT);
    }

    public JedisShardInfo(ConnectionInfo connectionInfo, String name) {
      this(connectionInfo, Sharded.DEFAULT_WEIGHT, name);
    }

    public JedisShardInfo(ConnectionInfo connectionInfo, int weight) {
      this(connectionInfo, weight, null);
    }

    public JedisShardInfo(ConnectionInfo connectionInfo, int weight, String name) {
      super(weight, name);
      this.connectionInfo = connectionInfo;
    }

    @Override
    public Jedis createResource() {
        return new Jedis(this.getConnectionInfo());
    }
    
    public ConnectionInfo getConnectionInfo() {
      return connectionInfo;
    }
}
