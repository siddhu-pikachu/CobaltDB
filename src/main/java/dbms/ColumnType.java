package dbms;

public class ColumnType {
    private final DataType dataType;
    private final TextType textType;

    private ColumnType(DataType dataType) {
        this.dataType = dataType;
        this.textType = null;
    }

    private ColumnType(TextType textType) {
        this.dataType = null;
        this.textType = textType;
    }

    public static ColumnType of(DataType type) {
        return new ColumnType(type);
    }

    public static ColumnType ofText(int length) {
        return new ColumnType(new TextType(length));
    }

    public static ColumnType fromCode(int code) {
        if (TextType.isTextCode(code)) {
            return new ColumnType(TextType.fromCode(code));
        }
        for (DataType type : DataType.values()) {
            if (type.getCode() == code) {
                return new ColumnType(type);
            }
        }
        throw new IllegalArgumentException("Invalid type code: " + code);
    }

    public int getCode() {
        return isText() ? textType.getCode() : dataType.getCode();
    }

    public int getLength() {
        return isText() ? textType.getLength() : dataType.getLength();
    }

    public boolean isText() {
        return textType != null;
    }

    @Override
    public String toString() {
        if (isText()) {
            return "TEXT(" + textType.getLength() + ")";
        }
        return dataType.name();
    }
}