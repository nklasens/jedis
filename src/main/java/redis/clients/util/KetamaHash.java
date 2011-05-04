package redis.clients.util;

public class KetamaHash extends MD5Hash {

  public byte[] hashBytes(String key) {
    return hashBytes(SafeEncoder.encode(key));
  }

}
