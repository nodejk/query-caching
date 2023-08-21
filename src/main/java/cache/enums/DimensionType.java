package cache.enums;

public enum DimensionType {
    COUNT("count"),
    SIZE_BYTES("size_bytes");

    private final String name;

    private DimensionType(String s) {
        this.name = s;
    }

    public static DimensionType fromString(String text) {

        for (DimensionType b : DimensionType.values()) {
            if (b.name.equals(text)) {
                return b;
            }
        }
        return null;
    }
}
