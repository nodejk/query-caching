package cache.policy.rr;

import cache.dim.Dimension;
import cache.models.AbstractCachePolicy;
import cache.models.CacheItem;
import kotlin.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RRPolicy<T> extends AbstractCachePolicy<T> {
    public RRPolicy(Dimension dimension) {
        super(dimension);
        this.currentCacheSize = 0;

        this.cache = new HashMap<>();
    }

    @Override
    public long comparator(CacheItem<T> item) {
        System.exit(1);
        return 0;
    }

    /*
        @TODO: fix this for other get functions.
     */
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

    @Override
    public List<Integer> getOrderedIndex(List<Pair<String, CacheItem<T>>> allItems) {
        List<Integer> test = IntStream.range(0, allItems.size()).boxed().collect(Collectors.toList());

        Collections.shuffle(test);

        return test;
    }

    public void _removeUnwantedIndexes() {
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
            index += 1;
        }

        for (Map.Entry<String, List<CacheItem<T>>> entry: removableMap.entrySet()) {
            this.cache.get(entry.getKey()).removeAll(entry.getValue());
            entry.getValue().forEach(cacheItem -> this.currentCacheSize -= cacheItem.getSize());
        }
    }
}
