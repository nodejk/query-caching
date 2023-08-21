package cache.enums;

public enum CacheType {
    LRU("lru"),
    FIFO("fifo"),
    RR("rr"),
    MRU("mru"),
    LFU("lfu");

    private final String label;

    private CacheType(String s) {
        this.label = s;
    }

    public static CacheType fromString(String text) {
        for (CacheType b : CacheType.values()) {
            if (b.label.equals(text)) {
                return b;
            }
        }
        return null;
    }
}
