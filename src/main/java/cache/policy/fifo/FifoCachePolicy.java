package cache.policy.fifo;

import cache.dim.Dimension;
import cache.models.CacheItem;
import cache.models.AbstractCachePolicy;
import kotlin.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static common.Logger.logCache;

public class FifoCachePolicy<T> extends AbstractCachePolicy<T> {
    protected Map<String, List<CacheItem<T>>> cache;
    public FifoCachePolicy(Dimension dimension) {
        super(dimension);
//        System.out.println("[SIZE_BYES]: " + this.dimension.getValue());
        this.cache = new HashMap<>();
    }

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
//            System.out.println("[FOUND_KEY]: " + key);
            this.cache.get(key).add(new CacheItem<T>(key, item, itemSize, this.numberOfCacheItems));
        } else {
//            System.out.println("[ADDING_KEY] " + key + " [WITH_SIZE] " + itemSize);
            ArrayList<CacheItem<T>> index = new ArrayList<>();
            index.add(new CacheItem<>(key, item, itemSize, this.numberOfCacheItems));
            this.cache.put(key, index);
        }

        this.currentCacheSize += itemSize;
        this.incrementCacheItems();
        this.onCacheSizeChange();
    }

    public void clean() {
//        System.out.println("-------------------------------------[CLEANING THE CACHE]-------------------------------------");
        this.removeUnwantedIndexes();

//        System.exit(1);
    }

    public void removeUnwantedIndexes() {
        long currentSize = this.currentCacheSize;
        int removed = 0;

//        System.out.println("BEFORE-->" + this.cache);

        Map<String, List<CacheItem<T>>> removableMap = new HashMap<>();

        List<Pair<String, CacheItem<T>>> allItems = new LinkedList<>();

        for (Map.Entry<String, List<CacheItem<T>>> entry: this.cache.entrySet()) {
            entry.getValue().forEach(
                cacheItem -> {
                    allItems.add(new Pair<>(entry.getKey(), cacheItem));
                }
            );
        }

//        System.out.println("------------[UNSORTED_ORDER]---> \n" + allItems);
        List<Integer> sortedByOrder = IntStream.range(0, allItems.size())
                .boxed()
                .sorted(Comparator.comparingLong(i -> allItems.get(i).getSecond().getOrder()))
                .collect(Collectors.toList());

//        System.out.println("");

        List<CacheItem<T>> sorted = allItems.stream().map(Pair::getSecond).sorted(Comparator.comparing(CacheItem::getOrder)).collect(Collectors.toList());
//

//        System.out.println("-------[sorted_list]----> \n" + sorted);
//        System.out.println("------------[SORTED_ORDER]--->--> \n" + sortedByOrder);

//        System.out.println("------------[SIZE]--->--> " +  String.format("%d%n", currentSize));

        int index = 0;

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

//        System.out.println("removables: " + removableMap);

        for (Map.Entry<String, List<CacheItem<T>>> entry: removableMap.entrySet()) {
            this.cache.get(entry.getKey()).removeAll(entry.getValue());
            entry.getValue().forEach(cacheItem -> this.currentCacheSize -= cacheItem.getSize());
        }

//        System.out.println("[REMOVED_ITEMS]:: " + removed + " [SIZE]: " + currentSize + " [THRESHOLD]: " + this.cacheSizeThreshold);
//
//        System.out.println("left-->" + this.cache);
//        System.exit(1);

    }
}
