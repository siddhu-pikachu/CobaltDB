// DataType.java
package dbms;

public enum DataType {
    NULL(0x00),
    TINYINT(0x01),
    SMALLINT(0x02),
    INT(0x03),
    BIGINT(0x04),
    FLOAT(0x05),
    DOUBLE(0x06),
    YEAR(0x08),
    TIME(0x09),
    DATETIME(0x0A),
    DATE(0x0B);
    // the data type text is handled seperately because addint it to the enum will make it a singleton
    // which means we can only have a single type of text(text of length = some(x)) at any point of time

    private final int code; // initializes the unicode of each custom data type and cannot be changed once initialized

    // constructor for the class
    DataType(int code) {
        this.code = code;
    }

    //
    public int getCode() {
        return code;
    }

    public int getLength() {
        return switch (this) {
            case TINYINT, YEAR -> 1;
            case SMALLINT -> 2;
            case INT, FLOAT, TIME -> 4;
            case BIGINT, DOUBLE, DATETIME, DATE -> 8;
            case NULL -> 0;
        };
    }
}