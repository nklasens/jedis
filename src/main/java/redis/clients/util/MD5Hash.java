package redis.clients.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Hash implements Hashing {

    public long hash(String key) {
        return hash(SafeEncoder.encode(key));
    }

    public long hash(byte[] key) {
        byte[] bKey = hashBytes(key);
        long res = ((long) (bKey[3] & 0xFF) << 24)
                | ((long) (bKey[2] & 0xFF) << 16)
                | ((long) (bKey[1] & 0xFF) << 8)
                | (long) (bKey[0] & 0xFF);
        return res;
    }

    public byte[] hashBytes(byte[] key) {
      MessageDigest md5 = MD5.get();
      md5.reset();
      md5.update(key);
      byte[] bKey = md5.digest();
      return bKey;
    }

    private static ThreadLocal<MessageDigest> MD5 = new ThreadLocal<MessageDigest>() {
      @Override
      protected final MessageDigest initialValue() {
        try {
          return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
          throw new IllegalStateException("++++ no md5 algorythm found");
        }
      }
    };
    
}
