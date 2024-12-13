package dbms;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.lang.Math.*;
import java.io.IOException;
import static java.lang.System.out;

/**
 *
 *  @author Chris Irwin Davis
 *  @version 1.1
 *  <p>This code is (c) 2018 Chris Irwin Davis
 *
 */
public class HexDump {
	
	/* TODO: This will be deprecated in future versions */
	static String columnGap = " ";
	static int pageSize = 0x200;
	static RandomAccessFile raf;
	static String displayControlCharacterAs = ".";
	static int currentByteColumn = 0;

	/* Canonical hex+ASCII display. 
	 * Display the input offset in hexadecimal, followed by
	 * sixteen space-separated, two column, hexadecimal bytes,
	 * followed by the same sixteen bytes in %_p format enclosed
	 * in ``|'' characters. 
	 *
	 * This is analogous to unix hexdump -C
	 */
	static boolean displayASCII = true;
	/* This will be an integer 0x0-0xF, i.e. 0-15 */
	static boolean displayPageHeader = true;
	static boolean displayHelp = false;






	/**
	 *  main() method
	 */
	public static void main(String[] args) {

		displayCopyright();
		
		/* TODO: Add command line options to set boolean flags */

		if(args.length == 0) {
			out.println();
			out.println("ERROR: Must supply a file name to be displayed");
			out.println("USAGE: java HexDump <file_name>");
			out.println();
			/* Exit the JVM */
			System.exit(0);
		}

		try {
			/* Open the file given in arg[0] as a RandomAccessFile */
			raf = new RandomAccessFile(args[0],"r");
			/* Call displayHexDump() method to display the file */
			displayHexDump();
		}
		catch (IOException e) {
			out.println(e);
		}
	}
	/* END OF MAIN */
	
	/**
	 * <p>This method will display the byte contents of the
	 * RandomAccessFile raf to Standard Out (stdout)
	 */
	static void displayHexDump() {
		/* This try block is needed because RandomAccessFile is used */
		try {

			/* Reset the file pointer to the beginning of the block. */
			raf.seek(0); 
			
			/* This int is used to keep track of the current pointer location.
			 * This is more efficient than re-executing raf.getFilePointer()
			 * every byte. But it is less safe since it is possible for
			 * thisByteOffset to become out of sync with raf.getFilePointer()
			 * if the value is modified other than a single increment for 
			 * each iteration of the byte-by-byte while loop. */
			int thisByteOffset = 0;
			
			/* Collect the length of the file in bytes so we'll know
			 * when to exit the while loop */
			long size = raf.length(); 

			/* Declare a byte array of 16 bytes to store a given row */
			byte[] rowOfBytes = new byte[16];

			/* 
			 * BEGIN BYTE-BY-BYTE ITERATION THROUGH THE FILE
			 */
			// while(raf.getFilePointer() < size) {
			/* Using the while condition below instead of the line above is
			 * potentially unsafe if the code is modified!!! */
			while(thisByteOffset < size) {

				/* Display a page header before every pageSize number of bytes */
				if(thisByteOffset % pageSize == 0) {
					printPageHeader();
				}

				/* Print the row prefix every 16 bytes */
				if(thisByteOffset % 16 == 0) {
					/* Print the row prefix address block in hex */
					// out.print(String.format("0x%08x  ", thisByteOffset));
					out.print(String.format("%08x  ", thisByteOffset));
					currentByteColumn = 0;
				}

				/* Collect a row of 16 bytes */
				{
					int ndx = thisByteOffset % 16;
					rowOfBytes[ndx] = raf.readByte();
					thisByteOffset++;
					currentByteColumn++;
				}

				/* Every 16 bytes, display the row collected in the block above */
				if(thisByteOffset % 16 == 0) {
					printRowOfBytes(rowOfBytes);
					// Reset the rowOfBytes once a row is displayed
					rowOfBytes = new byte[16];
				}
				currentByteColumn++;
			}
			/* END OF BYTE-BY-BYTE while loop */
			
			/* If there is a partial row remaining, print it */
			printRowOfBytes(rowOfBytes);
			out.println("currentByteColumn: " + currentByteColumn);

			/* 
			 * This inserts a newline at the end of the hex dump
			 * so that the command prompt will be at the start
			 * of the next line.
			 */
			out.println();
		} /* End of TRY block */
		catch (IOException e) {
			out.println(e);
		} /* End of CATCH block */
	}

	static void printPageHeader() {

		out.println();
		
		/* Hex header */
		out.print("Address    0  1  2  3  4  5  6  7 " + columnGap + " 8  9  A  B  C  D  E  F");

		/* ASCII header */
		if(displayASCII)
			out.print("  |0123456789ABCDEF|");
		out.println();


		out.print(line(58,"-"));
		if(displayASCII)
			out.print(line(20,"-"));
		out.println();


	}
	
	// FIXME: Need to accomodate printing of rows that are fewer than 16 bytes
	// FIXME: Otherwise, the remaining bytes display he default value of 0x00
	static void printRowOfBytes(byte[] row) {
		int rowLength = row.length;

		/* Print a hex byte row */
		for(int n = 0; n < rowLength; n++) {
			if(n==8)
				out.print(columnGap);
			out.print(String.format("%02X ", row[n]));
		}
		
		if(displayASCII) {
			/* Delimiter between Hex row output and ASCII row output */
			out.print(" |");

			/* Print an ASCII row */
			for(int n = 0; n < rowLength; n++) {
				if(row[n] < 0x20 || row[n] > 0x7e)
					out.print(displayControlCharacterAs);
				else
					out.print((char)row[n]);
			}
			out.print("|");
		}
		
		out.println();
	}

	static void displayCopyright() {
		out.println("*");
		out.println("* HexDump (c)2018 Chris Irwin Davis");
		out.println("*");
	}

	/**
	 *  Construct and return a String of "length" number of String "c"
	 */
	static String line(int length, String c) {
		String s = "";
		while(length>0) {
			s = s + c;
			length--;
		}
		return s;
	}
}
