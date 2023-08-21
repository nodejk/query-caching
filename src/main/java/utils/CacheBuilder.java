package utils;

import cache.models.AbstractCachePolicy;
import cache.policy.fifo.FifoCachePolicy;
import cache.policy.lfu.LFUCachePolicy;
import cache.policy.mru.MRUPolicy;
import cache.policy.rr.RRPolicy;

public class CacheBuilder<T> {

    public AbstractCachePolicy<T> build(Configuration configuration) {
        switch (configuration.cacheType) {
            case RR: {
                return new RRPolicy<>(configuration.getDimension());
            }
            case FIFO: {
                return new FifoCachePolicy<>(configuration.getDimension());
            }
            case LFU: {
                return new LFUCachePolicy<>(configuration.getDimension());
            }
            case MRU: {
                return new MRUPolicy<>(configuration.getDimension());
            }
            default: {
                throw new Error("can not resolve cache type: " + configuration.cacheType);
            }
        }
    }
}
