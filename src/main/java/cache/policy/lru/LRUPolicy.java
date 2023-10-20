package cache.policy.lru;

import cache.dim.Dimension;
import cache.models.AbstractCachePolicy;
import cache.models.CacheItem;
import kotlin.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LRUPolicy<T> extends AbstractCachePolicy<T> {
    public LRUPolicy(Dimension dimension) {
        super(dimension);
        this.cache = new HashMap<>();
    }

    @Override
    public long comparator(CacheItem<T> item) {
        return item.getLastAccessTime();
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
    public void clean() {
        this.removeUnwantedIndexes();
    }

    @Override
    public List<Integer> getOrderedIndex(List<Pair<String, CacheItem<T>>> allItems) {
        return IntStream.range(0, allItems.size())
            .boxed()
            .sorted(Comparator.comparingLong(i -> this.comparator(allItems.get(i).getSecond())))
            .collect(Collectors.toList());
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

        List<Integer> sortedByTime = IntStream.range(0, allItems.size())
            .boxed()
            .sorted(Comparator.comparingLong(i -> allItems.get(i).getSecond().getLastAccessTime()))
            .collect(Collectors.toList());


        List<CacheItem<T>> sorted = allItems.stream().map(Pair::getSecond).sorted(Comparator.comparingLong(CacheItem::getLastAccessTime)).collect(Collectors.toList());

//        System.out.println("-------[sorted_list]----> \n" + sorted);
//
//
//
//        System.out.println("-------[allItems]----> \n");
//
//        for (Pair<String, CacheItem<T>> item : allItems) {
//            System.out.println(item.getSecond());
//        }

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

//        System.out.println("[REMOVED_ITEMS]:: " + removed + " [SIZE]: " + currentSize + " [THRESHOLD]: " + this.cacheSizeThreshold);
//        System.out.println("[CURR_ITEMS]:: " + this.getNumberOfCacheItems());

    }
}
