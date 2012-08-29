package redis.clients.jedis;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.*;

public class Connection {
  
    private ConnectionInfo connectionInfo;
    private Socket socket;
    private Protocol protocol = new Protocol();
    private RedisOutputStream outputStream;
    private RedisInputStream inputStream;
    private int pipelinedCommands = 0;

    public Connection(final ConnectionInfo connectionInfo) {
        super();
        this.connectionInfo = connectionInfo;
	}

    public Socket getSocket() {
        return socket;
    }

    public ConnectionInfo getConnectionInfo() {
      return connectionInfo;
    }
    
    public void setTimeoutInfinite() {
        try {
            socket.setSoTimeout(0);
        } catch (SocketException ex) {
            throw new JedisException(ex);
        }
    }

    public void rollbackTimeout() {
        try {
            socket.setSoTimeout(connectionInfo.getTimeout());
        } catch (SocketException ex) {
            throw new JedisException(ex);
        }
    }

    protected void flush() {
        try {
            outputStream.flush();
        } catch (IOException e) {
            throw new JedisConnectionException(e);
        }
    }

    protected Connection sendCommand(final Command cmd, final String... args) {
        final byte[][] bargs = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            bargs[i] = SafeEncoder.encode(args[i]);
        }
        return sendCommand(cmd, bargs);
    }

    protected Connection sendCommand(final Command cmd, final byte[]... args) {
        connect();
        sendProtocolCommand(cmd, args);
        pipelinedCommands++;
        return this;
    }

    protected Connection sendCommand(final Command cmd) {
        connect();
        sendProtocolCommand(cmd, new byte[0][]);
        pipelinedCommands++;
        return this;
    }

    protected void sendProtocolCommand(final Command cmd, final byte[]... args) {
      try {
          protocol.sendCommand(outputStream, cmd, args);
      } catch (JedisConnectionException e) {
        disconnect();
        throw e;
      }
    }

    protected Object read() {
      try {
        return protocol.read(inputStream);
      } catch (JedisConnectionException e) {
        disconnect();
        throw e;
      }
    }

    protected void read(ReplyOutputStream stream) {
      try {
        protocol.read(inputStream, stream);
      } catch (JedisConnectionException e) {
        disconnect();
        throw e;
      }
    }

    public void connect() {
        if (!isConnected()) {
            try {
                socket = new Socket();
                socket.connect(connectionInfo.getInetSocketAddress(), connectionInfo.getTimeout());
                socket.setSoTimeout(connectionInfo.getTimeout());
                outputStream = new RedisOutputStream(socket.getOutputStream());
                inputStream = new RedisInputStream(socket.getInputStream());
            } catch (IOException ex) {
                throw new JedisConnectionException(ex);
            }
        }
    }

    public void disconnect() {
        if (isConnected()) {
            try {
                inputStream.close();
                outputStream.close();
                if (!socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException ex) {
                throw new JedisConnectionException(ex);
            }
        }
    }

    public boolean isConnected() {
        return socket != null && socket.isBound() && !socket.isClosed()
                && socket.isConnected() && !socket.isInputShutdown()
                && !socket.isOutputShutdown();
    }

    protected String getStatusCodeReply() {
        flush();
        pipelinedCommands--;
        final byte[] resp = (byte[]) read();
        if (null == resp) {
            return null;
        } else {
            return SafeEncoder.encode(resp);
        }
    }

    public String getBulkReply() {
        final byte[] result = getBinaryBulkReply();
        if (null != result) {
            return SafeEncoder.encode(result);
        } else {
            return null;
        }
    }

    public byte[] getBinaryBulkReply() {
        flush();
        pipelinedCommands--;
        return (byte[]) read();
    }

    public void getStreamReply(ReplyOutputStream stream) {
      flush();
      pipelinedCommands--;
      read(stream);
    }

    public Long getIntegerReply() {
        flush();
        pipelinedCommands--;
        return (Long) read();
    }

    public List<String> getMultiBulkReply() {
        return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply());
    }

    @SuppressWarnings("unchecked")
    public List<byte[]> getBinaryMultiBulkReply() {
        flush();
        pipelinedCommands--;
        return (List<byte[]>) read();
    }

    @SuppressWarnings("unchecked")
    public List<Object> getObjectMultiBulkReply() {
        flush();
        pipelinedCommands--;
        return (List<Object>) read();
    }

    public List<Object> getAll() {
        return getAll(0);
    }

    public List<Object> getAll(int except) {
        List<Object> all = new ArrayList<Object>();
        flush();
        while (pipelinedCommands > except) {
            all.add(read());
            pipelinedCommands--;
        }
        return all;
    }

    public Object getOne() {
        flush();
        pipelinedCommands--;
        return read();
    }
}