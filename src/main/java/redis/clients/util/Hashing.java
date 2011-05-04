package redis.clients.util;

public interface Hashing {
    public static final Hashing MURMUR_HASH = new MurmurHash();
    public static final Hashing MD5 = new MD5Hash();
    public static final KetamaHash KETAMA = new KetamaHash();

    public long hash(String key);
    public long hash(byte[] key);
}