package cache.policy.rr;

import cache.dim.Dimension;
import cache.models.AbstractCachePolicy;
import cache.models.CacheItem;
import kotlin.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class RRPolicy<T> extends AbstractCachePolicy<T> {
    protected Map<String, List<CacheItem<T>>> cache;
    public RRPolicy(Dimension dimension) {
        super(dimension);
        this.currentCacheSize = 0;

        this.cache = new HashMap<>();
    }

    /*
        @TODO: fix this for other get functions.
     */
    public List<T> get(String key) {
        if (!this.cache.containsKey(key)) {
            return new ArrayList<>();
        }

        return this.cache.getOrDefault(key, null)
            .stream()
            .map(CacheItem::getItem)
            .collect(Collectors.toList());
    }

    public void add(String key, T item, long itemSize) {
        if (this.cache.containsKey(key)) {
            this.cache.get(key).add(new CacheItem<T>(key, item, itemSize, this.numberOfCacheItems));
        } else {
            ArrayList<CacheItem<T>> index = new ArrayList<>();
            index.add(new CacheItem<>(key, item, itemSize, this.numberOfCacheItems));
            this.cache.put(key, index);
        }

        this.currentCacheSize += itemSize;
        this.incrementCacheItems();
        this.onCacheSizeChange();
    }

    public void clean() {
        this.removeUnwantedIndexes();
    }

    public void removeUnwantedIndexes() {
        long currentSize = this.currentCacheSize;
        int removed = 0;

        Map<String, List<CacheItem<T>>> removableMap = new HashMap<>();

        List<Pair<String, CacheItem<T>>> allItems = new LinkedList<>();

        for (Map.Entry<String, List<CacheItem<T>>> entry: this.cache.entrySet()) {
            entry.getValue().forEach(
                    cacheItem -> {
                        allItems.add(new Pair<>(entry.getKey(), cacheItem));
                    }
            );
        }

        Collections.shuffle(allItems);

        int index = 0;

        while (currentSize > this.cacheSizeThreshold && removed < this.numberOfCacheItems) {
            Pair<String, CacheItem<T>> p = allItems.get(index);
            currentSize -= p.getSecond().getSize();

            if (removableMap.containsKey(p.getFirst())) {
                removableMap.get(p.getFirst()).add(p.getSecond());
            } else {
                List<CacheItem<T>> it = new ArrayList<>();
                it.add(p.getSecond());
                removableMap.put(p.getFirst(), it);
            }
            removed += 1;
        }

        for (Map.Entry<String, List<CacheItem<T>>> entry: removableMap.entrySet()) {
            this.cache.get(entry.getKey()).removeAll(entry.getValue());
            entry.getValue().forEach(cacheItem -> this.currentCacheSize -= cacheItem.getSize());
        }
    }
}
