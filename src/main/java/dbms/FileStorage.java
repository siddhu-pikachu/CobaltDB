package dbms;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileStorage {
    private static final int PAGE_SIZE = 512;
    private final Map<Table, RandomAccessFile> file = new HashMap<>();
    private final Map<String, Table> table = new HashMap<>();
    private final String filename;
    private final String columns;

    public FileStorage(String filename, String columns) {
        this.filename = filename;
        this.columns = columns;
        initializeFile();
    }

    private void initializeFile() {
        try {
            // Get table name without extension
            String tableName = filename.split("\\.")[0];

            // Create .tbl file as per spec
            String tableFile = tableName + ".tbl";
            RandomAccessFile tempFile = new RandomAccessFile(tableFile, "rw");

            // Create table with B+tree
            Table newTable = new Table(tempFile, tableName, columns != null ? columns : "");
            table.put(filename, newTable);
            file.put(newTable, tempFile);

            // Initialize with first page
            tempFile.setLength(0);
            tempFile.setLength(PAGE_SIZE);

            // Initialize first page with zeros
            byte[] emptyPage = new byte[PAGE_SIZE];
            tempFile.write(emptyPage);

            // Initialize table
            table.get(filename).initialize();

        } catch (Exception e) {
            System.out.println("Error initializing file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void startCSVProcess() {
        try {
            Table currentTable = table.get(filename);
            RandomAccessFile currentFile = file.get(currentTable);
            currentTable.processCsv(currentFile, filename);  // Pass both the file and filename
        } catch (Exception e) {
            System.out.println("Error processing CSV: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public long getFileSize() {
        try {
            return file.get(table.get(filename)).length();
        } catch (IOException e) {
            throw new RuntimeException("Error getting file size: " + e.getMessage());
        }
    }
}