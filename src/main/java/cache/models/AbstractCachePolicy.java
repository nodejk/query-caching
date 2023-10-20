package cache.models;

import cache.Cache;
import cache.dim.Dimension;
import cache.enums.DimensionType;
import cache.policy.ReplacementPolicy;
import common.QueryUtils;
import kotlin.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractCachePolicy<T> {
    protected Map<String, List<CacheItem<T>>> cache;

    protected final float PRUNE_TO = 0.7f;
    protected final float PRUNE_THRESHOLD = 1.0f;
    protected int numberOfCacheItems;
    protected final float cacheSizeThreshold;
    protected long currentCacheSize;
    public abstract void add(String key, T item, long itemSize);

    public final List<T> get(String key) {
        System.out.println("INCREMENTING CACHE COUNT");
        if (!this.cache.containsKey(key)) {
            return new ArrayList<>();
        }

        return this.cache.get(key)
            .stream()
            .map(CacheItem::getItem)
            .collect(Collectors.toList());
    }
    public abstract void clean();

    public final Dimension dimension;

    public abstract List<Integer> getOrderedIndex(List<Pair<String, CacheItem<T>>> allItems);

    public final List<T> getAllItemsForRead(String key) {
        if (!this.cache.containsKey(key)) {
            return new ArrayList<>();
        }

        return this.cache.get(key)
            .stream()
            .map(CacheItem::getItemForRead)
            .collect(Collectors.toList());
    }

    public void incrementCacheItems() {
        this.numberOfCacheItems++;
    }

    public int getNumberOfCacheItems() {
        return this.numberOfCacheItems;
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

    public abstract long comparator(CacheItem<T> item);

    public final void removeUnwantedIndexes() {
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

        List<Integer> sortedByTime = this.getOrderedIndex(allItems);

        List<CacheItem<T>> sorted = this.getOrderedIndex(allItems).stream().map(i -> allItems.get(i).getSecond()).collect(Collectors.toList());

//        System.out.println("-------[sorted_list]----> \n" + sorted);
//
//
//
//        System.out.println("-------[allItems]----> \n");
////
//        for (Pair<String, CacheItem<T>> item : allItems) {
//            System.out.println(item.getSecond());
//        }

//        System.exit(1);

//        System.out.println("------------[SORTED_ORDER]--->--> \n" + sortedByTime);
//
//        System.out.println("------------[SIZE]--->--> " +  String.format("%d%n", currentSize));

        int index = 0;

        while (currentSize > this.cacheSizeThreshold && removed < this.numberOfCacheItems) {
            int cacheItem = sortedByTime.get(index);

            Pair<String, CacheItem<T>> p = allItems.get(cacheItem);

            currentSize -= p.getSecond().getSize();

            if (removableMap.containsKey(p.getFirst())) {
                removableMap.get(p.getFirst()).add(p.getSecond());
            } else {
                List<CacheItem<T>> it = new ArrayList<>();
                it.add(p.getSecond());
                removableMap.put(p.getFirst(), it);
            }

            index++;
            removed += 1;

        }

        for (Map.Entry<String, List<CacheItem<T>>> entry: removableMap.entrySet()) {

            entry.getValue().forEach((CacheItem<T> item) -> {
                this.decrementCacheItems();
            });

            this.cache.get(entry.getKey()).removeAll(entry.getValue());
            entry.getValue().forEach(cacheItem -> this.currentCacheSize -= cacheItem.getSize());
        }

        System.out.println("[REMOVED_ITEMS]:: " + removed + " [SIZE]: " + currentSize + " [THRESHOLD]: " + this.cacheSizeThreshold);

        System.out.println("[CURRENT_ITEMS]:: " + this.currentCacheSize);
    }

    public final void onCacheSizeChange() {
        if (this.currentCacheSize > this.dimension.getValue() * this.PRUNE_THRESHOLD) {
            this.clean();
        }
    }
}
