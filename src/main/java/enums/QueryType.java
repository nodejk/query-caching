package enums;

import cache.enums.CacheType;

public enum QueryType {
    ALL("all"),
    SIMPLE_FILTER("simple_filter"),
    COMPLEX_FILTER("complex_filter"),
    FILTER_JOIN("filter_join"),
    FILTER_AGGREGATE("filter_aggregate"),
    FILTER_JOIN_AGGREGATE("filter_join_aggregate");

    public final String label;

    private QueryType(String label) {
        this.label = label;
    }

    public static QueryType fromString(String text) {
        for (QueryType b : QueryType.values()) {
            if (b.label.equals(text)) {
                return b;
            }
        }
        return null;
    }
}
