package dbms;

/**
 * Represents a TEXT data type in the database.
 * Handles variable-length string fields from 0 to 115 characters.
 */
public class TextType {
    private static final int BASE_CODE = 0x0C;
    private final int length;

    /**
     * Creates a new TEXT type with specified length.
     * @param length The length of the text field (0-115)
     * @throws IllegalArgumentException if length is invalid
     */
    public TextType(int length) {
        if (length < 0 || length > 115) {
            throw new IllegalArgumentException("Length must be between 0 and 115");
        }
        this.length = length;
    }

    /**
     * Gets the type code for this TEXT type.
     * @return The type code (BASE_CODE + length)
     */
    public int getCode() {
        return BASE_CODE + length;
    }

    public int getLength() {
        return length;
    }

    /**
     * Checks if a given type code represents a TEXT type.
     * @param code The type code to check
     * @return true if code represents a TEXT type
     */
    public static boolean isTextCode(int code) {
        return code >= BASE_CODE && code < BASE_CODE + 115;
    }

    /**
     * Creates a TextType instance from a type code.
     * @param code The type code
     * @return A new TextType instance
     * @throws IllegalArgumentException if code is invalid
     */
    public static TextType fromCode(int code) {
        if (!isTextCode(code)) {
            throw new IllegalArgumentException("Invalid TEXT type code: " + code);
        }
        return new TextType(code - BASE_CODE);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof TextType)) return false;
        TextType other = (TextType) obj;
        return this.length == other.length;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(length);
    }

    @Override
    public String toString() {
        return "TEXT(" + length + ")";
    }
}