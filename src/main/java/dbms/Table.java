package dbms;

import java.io.*;

public class Table {
    private final String tableName;
    private final BPlusTree bPlusTree;
    private int nextRowId;
    private final RandomAccessFile file;
    private final String columns;

    public Table(RandomAccessFile file, String name, String columns) {
        this.tableName = name;
        this.file = file;
        this.columns = columns;
        this.bPlusTree = new BPlusTree(file);
        this.nextRowId = 0;
    }

    public void initialize() throws IOException {
        file.setLength(512);
        System.out.println("Initialized table file with size: " + file.length());
    }

    public void processCsv(RandomAccessFile dbFile, String csvFilePath) {
        System.out.println("Starting to process CSV file: " + csvFilePath);
        try {
            // Create a File object to check if the CSV exists
            File csvFile = new File(csvFilePath);
            System.out.println("CSV file exists: " + csvFile.exists());
            System.out.println("CSV file absolute path: " + csvFile.getAbsolutePath());

            if (!csvFile.exists()) {
                throw new FileNotFoundException("CSV file not found: " + csvFilePath);
            }

            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            System.out.println("Successfully opened CSV file");

            String line = br.readLine(); // skip header
            System.out.println("Header line: " + line);

            line = br.readLine();
            int recordCount = 0;

            while (line != null) {
                System.out.println("Processing line: " + line);
                int rId = assignRowId();
                Record empRecord = parseCSVLine(line, rId);
                System.out.println("Created record with rowId: " + rId);

                if (bPlusTree.insert(empRecord)) {
                    System.out.println("Successfully inserted record " + rId);
                    recordCount++;
                } else {
                    System.out.println("Failed to insert record with rowId " + rId);
                }

                line = br.readLine();
            }

            System.out.println("Finished processing CSV. Total records processed: " + recordCount);
            br.close();

            // Verify file size after processing
            System.out.println("Final table file size: " + dbFile.length());

        } catch (Exception e) {
            System.out.println("Error processing CSV: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private char toChar(String s) {
        return s.length() == 1 ? s.charAt(0) : '\u0000';
    }

    private short toShort(String s) {
        try {
            int value = Integer.parseInt(s);
            if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
                return (short) value;
            }
        } catch (NumberFormatException ignored) {
            System.out.println("Warning: Could not parse short value: " + s);
        }
        return 0;
    }

    private String removeDash(String s) {
        return s.substring(0, 3) + s.substring(4, 6) + s.substring(7, 11);
    }

    private Record parseCSVLine(String line, int rId) {
        try {
            String[] attributes = line.split(",");
            System.out.println("Parsing line with " + attributes.length + " attributes");

            return new Record(
                    rId,
                    attributes[0],
                    toChar(attributes[1]),
                    attributes[2],
                    removeDash(attributes[3]),
                    attributes[4],
                    attributes[5],
                    toChar(attributes[6]),
                    Integer.parseInt(attributes[7]),
                    toShort(attributes[8])
            );
        } catch (Exception e) {
            System.out.println("Error parsing line: " + line);
            e.printStackTrace();
            throw e;
        }
    }

    private int assignRowId() {
        return ++nextRowId;
    }
}