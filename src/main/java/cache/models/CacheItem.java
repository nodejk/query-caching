package cache.models;

import java.util.Objects;

public class CacheItem<T> {
    protected String key;
    public int order;
    protected T item;
    protected long size;
    private long lastAccessTime;
    private int numAccessed;

    public CacheItem (String key, T item, long value, int order) {
        this.key = key;
        this.item = item;
        this.size = value;
        this.order = order;
        this.numAccessed = 0;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public CacheItem (String key, T item, long value) {
        this.key = key;
        this.item = item;
        this.size = value;
        this.numAccessed = 0;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public T getItem() {
        this.lastAccessTime = System.currentTimeMillis();
        this.incrementNumAccessed();
        return this.item;
    }

    public long getSize() {
        return this.size;
    }
    public String getKey() {
        return this.key;
    }

    @Override
    public String toString() {
        return String.format("[ORDER]: %d, [SIZE]: %d, [LAST_TIME_ACCESSED]: %d%n", this.order, this.size, this.lastAccessTime);
    }

    public final long getLastAccessTime() {
        return this.lastAccessTime;
    }

    public final int getNumAccessed() {
        return this.numAccessed;
    }

    protected final void incrementNumAccessed() {
        this.numAccessed++;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        CacheItem<?> cacheItem = (CacheItem<?>) o;
        return Objects.equals(cacheItem.key, this.key);
    }
}
