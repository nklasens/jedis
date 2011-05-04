package redis.clients.util;

public abstract class ShardInfo<T> {
    private int weight;
    private String identifier;

    public ShardInfo(int weight, String identifier) {
      this.weight = weight;
      this.identifier = identifier;
    }
    
    public int getWeight() {
        return this.weight;
    }
    
    public String getIdentifier() {
      return identifier;
    }

    protected abstract T createResource();

}
