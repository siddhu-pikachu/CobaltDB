package dbms;

public enum DataType {
    NULL(0x00),
    TINYINT(0x01),
    SMALLINT(0x02),
    INT(0x03),
    BITGINT(0x04),
    FLOAT(0x05),
    DOUBLE(0x06),
    YEAR(0x08),
    TIME(0x09),
    DATETIME(0x0A),
    DATE(0x0B),
    TEXT(0x0C);

    private final int code;
    private Integer length;

    DataType(int code) {
        this.code = code;
        this.length = null;
    }

    public int getCode() {
        if (this == TEXT && length != null) {
            // handles datatypes from 0x0D and so on.
            return code + length;
        }
        return code;
    }

    public void setLength(Integer length) {
        if (this != TEXT) {
            throw new IllegalStateException("Length can only be set for TEXT type");
        }
        if (length < 0 || length > 115) {
            throw new IllegalArgumentException("Text length must be between 0 and 115");
        }
        this.length = length;
    }

    public Integer getLength() {
        return length;
    }
}
