package cache.policy.mru;

import cache.dim.Dimension;
import cache.models.AbstractCachePolicy;
import cache.models.CacheItem;
import kotlin.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MRUPolicy<T> extends AbstractCachePolicy<T> {

    protected Map<String, List<CacheItem<T>>> cache;

    public MRUPolicy(Dimension dimension) {
        super(dimension);
        this.cache = new HashMap<>();
    }

    @Override
    public void add(String key, T item, long itemSize) {
        if (this.cache.containsKey(key)) {
            this.cache.get(key).add(new CacheItem<>(key, item, itemSize, this.numberOfCacheItems));
        } else {
            ArrayList<CacheItem<T>> index = new ArrayList<>();
            index.add(new CacheItem<>(key, item, itemSize, this.numberOfCacheItems));
            this.cache.put(key, index);
        }

        this.currentCacheSize += itemSize;
        this.incrementCacheItems();
        this.onCacheSizeChange();
    }

    @Override
    public List<T> get(String key) {
        if (!this.cache.containsKey(key)) {
            return new ArrayList<>();
        }

        return this.cache.get(key)
            .stream()
            .map(CacheItem::getItem)
            .collect(Collectors.toList());
    }

    @Override
    public void clean() {
        this.removeUnwantedIndexes();
    }

    @Override
    public void removeUnwantedIndexes() {
        long currentSize = this.currentCacheSize;

        int removed = 0;

        Map<String, List<CacheItem<T>>> removableMap = new HashMap<>();

        List<Pair<String, CacheItem<T>>> allItems = new LinkedList<>();

        for (Map.Entry<String, List<CacheItem<T>>> entry : this.cache.entrySet()) {
            entry.getValue().forEach(
                    cacheItem -> {
                        allItems.add(new Pair<>(entry.getKey(), cacheItem));
                    }
            );
        }

        List<Integer> sortedByOrder = IntStream.range(0, allItems.size())
                .boxed()
                .sorted(Comparator.comparingLong(
                        i -> -1 * allItems.get(i).getSecond().getLastAccessTime())
                )
                .collect(Collectors.toList());

        List<Pair<String, CacheItem<T>>> map = allItems
                .stream()
                .sorted(
                        Comparator.comparing(
                                (Pair<String, CacheItem<T>> a) ->
                                        -a.getSecond().getLastAccessTime()
                )).collect(Collectors.toList());

        int index = 0;

//        System.out.println("currentCacheSize-->" + currentSize + " " + this.cacheSizeThreshold);

        while (currentSize > this.cacheSizeThreshold && removed < this.numberOfCacheItems) {
            int cacheItem = sortedByOrder.get(index);
            Pair<String, CacheItem<T>> p = allItems.get(cacheItem);
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

        for (Map.Entry<String, List<CacheItem<T>>> entry : removableMap.entrySet()) {
            this.cache.get(entry.getKey()).removeAll(entry.getValue());
            entry.getValue().forEach(cacheItem -> this.currentCacheSize -= cacheItem.getSize());
        }
    }
}
