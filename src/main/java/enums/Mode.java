package enums;

public enum Mode {
    SEQUENCE("sequence"),
    HYBRID("hybrid"),
    BATCH("batch"),
    MVR("mvr");

    public final String label;

    private Mode(String label) {
        this.label = label;
    }

    public static Mode fromString(String text) {
        for (Mode b : Mode.values()) {
            if (b.label.equals(text)) {
                return b;
            }
        }
        return null;
    }
}
