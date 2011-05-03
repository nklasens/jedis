package redis.clients.jedis;

public class ConnectionInfo {

  private String host;
  private int port;
  private int timeout;
  private String password;
  private int database;

  public ConnectionInfo(String host) {
    this(host, Protocol.DEFAULT_PORT);
  }

  public ConnectionInfo(String host, int port) {
    this(host, port, Protocol.DEFAULT_TIMEOUT);
  }
  
  public ConnectionInfo(String host, int port, int timeout) {
    this(host, port, timeout, null);
  }
  
  public ConnectionInfo(String host, int port, String password) {
    this(host, port, Protocol.DEFAULT_TIMEOUT, password);
  }

  public ConnectionInfo(String host, int port, String password, int database) {
    this(host, port, Protocol.DEFAULT_TIMEOUT, password, database);
  }

  public ConnectionInfo(String host, int port, int timeout, String password) {
    this(host, port, timeout, password, Protocol.DEFAULT_DATABASE); 
  }

  public ConnectionInfo(String host, int port, int timeout, String password, int database) {
    this.host = host;
    this.port = port;
    this.timeout = timeout > 0 ? timeout : Protocol.DEFAULT_TIMEOUT;
    this.password = password;
    this.database = database >= 0 ? database : Protocol.DEFAULT_DATABASE;
  }
  
  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }
  
  public int getTimeout() {
    return timeout;
  }
  
  public String getPassword() {
    return password;
  }

  public int getDatabase() {
    return database;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((host == null) ? 0 : host.hashCode());
    result = prime * result + port;
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
    ConnectionInfo other = (ConnectionInfo) obj;
    if (host == null) {
      if (other.host != null)
        return false;
    } else if (!host.equals(other.host))
      return false;
    if (port != other.port)
      return false;
    return true;
  }
  
  public String toString() {
    return host + ":" + port;
  }

}