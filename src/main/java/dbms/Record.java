package dbms;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Record {
    private int rowId;
    private final String firstName;
    private final char middleInitial;
    private final String lastName;
    private final String ssn;
    private final String birthDate;
    private final String address;
    private final char sex;
    private final int salary;
    private final short departmentNumber;
    private char deletionMarker;

    public Record(int rowId, String firstName, char middleInitial, String lastName,
                  String ssn, String birthDate, String address, char sex,
                  int salary, short departmentNumber) {
        this.rowId = rowId;
        this.firstName = firstName;
        this.middleInitial = middleInitial;
        this.lastName = lastName;
        this.ssn = ssn;
        this.birthDate = birthDate;
        this.address = address;
        this.sex = sex;
        this.salary = salary;
        this.departmentNumber = departmentNumber;
        this.deletionMarker = '\u0000';
    }

    public void setRowId(int id) {
        this.rowId = id;
    }

    public void setDeletionMarker() {
        this.deletionMarker = '1';
    }

    public String getSsn() {
        return this.ssn;
    }

    public int getRowId() {
        return this.rowId;
    }

    public byte[] serialize() {
        byte[] buffer = new byte[112];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        byteBuffer.order(ByteOrder.BIG_ENDIAN);

        // Write all fields in fixed-length format
        byteBuffer.putInt(rowId);  // 4 bytes
        writeFixedLengthString(byteBuffer, ssn, 9);
        writeFixedLengthString(byteBuffer, firstName, 20);
        writeFixedLengthString(byteBuffer, String.valueOf(middleInitial), 1);
        writeFixedLengthString(byteBuffer, lastName, 20);
        writeFixedLengthString(byteBuffer, birthDate, 10);
        writeFixedLengthString(byteBuffer, address, 40);
        writeFixedLengthString(byteBuffer, String.valueOf(sex), 1);
        byteBuffer.putInt(salary);  // 4 bytes
        byteBuffer.putShort(departmentNumber);  // 2 bytes
        writeFixedLengthString(byteBuffer, String.valueOf(deletionMarker), 1);

        return buffer;
    }

    private void writeFixedLengthString(ByteBuffer buffer, String str, int length) {
        byte[] bytes = new byte[length];
        byte[] strBytes = str.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(strBytes, 0, bytes, 0, Math.min(strBytes.length, length));
        buffer.put(bytes);
    }

    public static Record deserialize(byte[] data, int rowId) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);

        String firstName = readFixedLengthString(buffer, 20);
        char middleInitial = readFixedLengthString(buffer, 1).charAt(0);
        String lastName = readFixedLengthString(buffer, 20);
        String ssn = readFixedLengthString(buffer, 9);
        String birthDate = readFixedLengthString(buffer, 10);
        String address = readFixedLengthString(buffer, 40);
        char sex = readFixedLengthString(buffer, 1).charAt(0);
        int salary = buffer.getInt();
        short departmentNumber = buffer.getShort();

        return new Record(rowId, firstName, middleInitial, lastName, ssn,
                birthDate, address, sex, salary, departmentNumber);
    }

    private static String readFixedLengthString(ByteBuffer buffer, int length) {
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        String str = new String(bytes, StandardCharsets.US_ASCII);
        return str.trim();
    }
}