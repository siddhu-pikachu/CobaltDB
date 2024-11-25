package dbms;

import java.io.RandomAccessFile;
import java.io.IOException;

public class Page {
    private static final int PAGE_SIZE = 512;
    private static final int RECORD_SIZE = 112;
    private static final int HEADER_SIZE = 16;
    private static final int OFFSET_SIZE = 2;
    private static final int MAX_RECORDS = (PAGE_SIZE - HEADER_SIZE) / (RECORD_SIZE + OFFSET_SIZE);

    private final RandomAccessFile file;
    private final int pageNumber;
    private final byte pageType;
    private short recordCount;
    private short cellContentStart;
    private short rootPage;
    private short rightSibling;
    private short parentPage;
    private final short[] cellOffsets;

    public Page(RandomAccessFile file, int pageNumber, byte pageType) {
        this.file = file;
        this.pageNumber = pageNumber;
        this.pageType = pageType;
        this.cellOffsets = new short[MAX_RECORDS];
        this.rightSibling = -1;
        this.parentPage = -1;
    }

    public void initialize() throws IOException {
        file.seek(pageNumber * PAGE_SIZE);

        // Write header (16 bytes)
        file.writeByte(pageType);        // Page type (1 byte)
        file.writeByte(0);               // Unused (1 byte)
        file.writeShort(0);              // Number of cells (2 bytes)
        file.writeShort(PAGE_SIZE);      // Start of cell content (2 bytes)
        file.writeShort(rootPage);       // Root page number (2 bytes)
        file.writeShort(rightSibling);   // Right sibling/child (2 bytes)
        file.writeShort(parentPage);     // Parent page (2 bytes)
        file.writeInt(0);                // Unused (4 bytes)

        recordCount = 0;
        cellContentStart = (short) PAGE_SIZE;
    }

    public boolean hasSpace(int recordSize) {
        int neededSpace = recordSize + 2;  // record + offset entry
        int availableSpace = cellContentStart - (HEADER_SIZE + recordCount * 2);
        return availableSpace >= neededSpace;
    }

    public boolean addRecord(Record record) throws IOException {
        byte[] recordData = record.serialize();
        if (!hasSpace(recordData.length + 5)) { // +5 for payload size(1) + rowId(4)
            return false;
        }

        cellContentStart = (short) (cellContentStart - recordData.length - 5);
        file.seek(pageNumber * PAGE_SIZE + cellContentStart);
        file.writeByte(recordData.length);    // Write payload size
        file.write(recordData);               // Write actual record data

        // Add cell offset to array (maintained in sorted order by rowId)
        int insertPos = 0;
        while (insertPos < recordCount && getCellRowId(cellOffsets[insertPos]) < record.getRowId()) {
            insertPos++;
        }

        // Shift existing offsets
        System.arraycopy(cellOffsets, insertPos,
                cellOffsets, insertPos + 1,
                recordCount - insertPos);

        // Insert new offset
        cellOffsets[insertPos] = cellContentStart;

        recordCount++;
        updateHeader();
        return true;
    }

    private int getCellRowId(short offset) throws IOException {
        file.seek(pageNumber * PAGE_SIZE + offset);
        if (pageType == 0x0d) {  // Table Leaf
            file.skipBytes(1);    // Skip payload size
            return file.readInt();     // Read rowId
        } else if (pageType == 0x05) {  // Table Interior
            file.skipBytes(2);    // Skip left child pointer
            return file.readInt();     // Read rowId
        } else {
            throw new IllegalStateException("Invalid page type: " + pageType);
        }
    }

    private boolean updateHeader() throws IOException {
        file.seek(pageNumber * PAGE_SIZE);

        file.writeByte(pageType);
        file.writeByte(0);
        file.writeShort(recordCount);
        file.writeShort(cellContentStart);
        file.writeShort(rootPage);
        file.writeShort(rightSibling);
        file.writeShort(parentPage);
        file.writeInt(0);

        // Write cell offsets array
        long offsetPos = pageNumber * PAGE_SIZE + HEADER_SIZE;
        for (int i = 0; i < recordCount; i++) {
            file.seek(offsetPos);
            file.writeShort(cellOffsets[i]);
            offsetPos += 2;
        }
        return true;
    }

    public Record[] getAllRecords() throws IOException {
        Record[] records = new Record[recordCount];

        // First read cell offsets from header area
        file.seek(pageNumber * PAGE_SIZE + HEADER_SIZE);
        short[] offsets = new short[recordCount];
        for (int i = 0; i < recordCount; i++) {
            offsets[i] = file.readShort();
        }

        // Now read records using these offsets
        for (int i = 0; i < recordCount; i++) {
            file.seek(pageNumber * PAGE_SIZE + offsets[i]);

            byte payloadSize = file.readByte();
            int rowId = file.readInt();

            byte[] recordData = new byte[payloadSize];
            file.read(recordData);

            records[i] = Record.deserialize(recordData, rowId);
        }

        return records;
    }

    public void clear() throws IOException {
        file.seek(pageNumber * PAGE_SIZE);

        // Reinitialize header
        file.writeByte(pageType);
        file.writeByte(0);
        file.writeShort(0);
        file.writeShort(PAGE_SIZE);
        file.writeShort(rootPage);
        file.writeShort(rightSibling);
        file.writeShort(parentPage);
        file.writeInt(0);

        // Reset page state
        recordCount = 0;
        cellContentStart = (short) PAGE_SIZE;
        for (int i = 0; i < cellOffsets.length; i++) {
            cellOffsets[i] = 0;
        }
    }

    public void setParent(int parent) throws IOException {
        parentPage = (short) parent;
        updateHeader();
    }

    public void setRightSibling(int sibling) throws IOException {
        rightSibling = (short) sibling;
        updateHeader();
    }

    public Integer getRightSibling() {
        return rightSibling == -1 ? null : (int) rightSibling;
    }
}