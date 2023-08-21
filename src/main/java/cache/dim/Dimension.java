package cache.dim;

import cache.enums.DimensionType;

public class Dimension {

    private DimensionType type;
    private long value;

    private Dimension(DimensionType type, long value) {
        this.type = type;
        this.value = value;
    }

    public static Dimension COUNT(int value) {
        return new Dimension(DimensionType.COUNT, value);
    }

    public static Dimension SIZE(long value) {
        return new Dimension(DimensionType.SIZE_BYTES, value);
    }

    public DimensionType getType() {
        return this.type;
    }

    public long getValue() {
        return value;
    }
}
