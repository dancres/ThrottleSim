import java.util.LinkedHashMap;

public class LruCache<K,V> extends LinkedHashMap<K,V> {

    private static final long serialVersionUID = 1L;
    private final int cacheSize;

    public LruCache(int size) {
        super(size, 0.75f, true);
        this.cacheSize = size;

    }

    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<K,V> eldest) {
        return size() > cacheSize;
    }
}