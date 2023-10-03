package cache.models;

import cache.Cache;
import cache.dim.Dimension;
import cache.enums.DimensionType;
import cache.policy.ReplacementPolicy;
import common.QueryUtils;

import java.util.List;
import java.util.Map;

public abstract class AbstractCachePolicy<T> {

    protected final float PRUNE_TO = 0.7f;
    protected final float PRUNE_THRESHOLD = 0.8f;
    protected int numberOfCacheItems;
    protected final float cacheSizeThreshold;
    protected long currentCacheSize;
    public abstract void add(String key, T item, long itemSize);
    public abstract List<T> get(String key);
    public abstract void clean();

    public final Dimension dimension;

    public void incrementCacheItems() {
        this.numberOfCacheItems++;
    }

    public void decrementCacheItems() {
        this.numberOfCacheItems--;
    }

    protected void resetCache() {
        this.numberOfCacheItems = 0;
        this.currentCacheSize = 0;
    }

    public AbstractCachePolicy(Dimension dimension) {
        this.dimension = dimension;
        this.numberOfCacheItems = 0;
        this.cacheSizeThreshold = dimension.getValue() * this.PRUNE_TO;
    }

    protected void incrementNumberOfCacheItems() {
        this.currentCacheSize++;
    }

    protected void decrementNumberOfCacheItems() {
        this.currentCacheSize--;
    }

    public final float getCurrentCacheSize() {
        return this.cacheSizeThreshold;
    }

    public boolean isFull() {
        return this.cacheSizeThreshold == this.currentCacheSize;
    }

    public boolean isEmpty() {
        return this.currentCacheSize == 0;
    }

    public abstract void removeUnwantedIndexes();

    public final void onCacheSizeChange() {
        if (this.currentCacheSize > this.dimension.getValue() * this.PRUNE_THRESHOLD) {
            this.clean();
        }
    }
}
