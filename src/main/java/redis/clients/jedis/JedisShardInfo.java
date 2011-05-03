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

public class JedisShardInfo extends ConnetionShardInfo<Jedis> {
  
    public JedisShardInfo(String host) {
        super(new ConnectionInfo(host));
    }

    public JedisShardInfo(String host, int port) {
      super(new ConnectionInfo(host, port));
    }

    public JedisShardInfo(String host, int port, String password) {
      super(new ConnectionInfo(host, port, password));
    }
    
    public JedisShardInfo(String host, int port, int timeout) {
      super(new ConnectionInfo(host, port, timeout));
    }
    
    public JedisShardInfo(String host, int port, int timeout, int weight) {
        super(new ConnectionInfo(host, port, timeout), weight);
    }

    @Override
    public Jedis createResource() {
        return new Jedis(this.getConnectionInfo());
    }
}
