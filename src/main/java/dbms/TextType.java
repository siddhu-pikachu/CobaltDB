package dbms;

public class TextType {
    // static makes sure this is attribute is shared between all the instances
    // final makes sure that once initialised we cant change it!
    private static final int BASE_CODE = 0x0C;
    // attribute to store the length of string
    private final int length;

    // constructor for the class
    public TextType(int length) {
        if (length < 0 || length > 115) {
            throw new IllegalArgumentException("Length must be between 0 and 115");
        }
        this.length = length;
    }

    public int getCode() {
        return BASE_CODE + length;
    }

    public int getLength() {
        return length;
    }

    // method to check if the data type is text
    public static Boolean isTextCode(int code) {
        return code >= BASE_CODE && code < BASE_CODE + 115;
    }

    // creates a new instance of the text type
    public static TextType fromCode(int code) {
        if (!isTextCode(code)) {
            throw new IllegalArgumentException("Invalid TEXT type code" + code);
        }
        return new TextType(code - BASE_CODE);
    }
}
