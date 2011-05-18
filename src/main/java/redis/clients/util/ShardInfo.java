package redis.clients.util;

public abstract class ShardInfo<T> {
    private int weight;
    private String name;

    public ShardInfo(int weight, String name) {
      this.weight = weight;
      this.name = name;
    }
    
    public int getWeight() {
        return this.weight;
    }
    
    public String getName() {
      return name;
    }

    protected abstract T createResource();
}
