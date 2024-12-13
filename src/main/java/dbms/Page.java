package dbms;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class Page {

	public PageType pg_type;
	short CellCnt = 0;
	public int pgnum;
	short content_starting_offset;
	public int RightPage;
	public int ParentPgNumber;
	private List<TableRecord> records;
	boolean IsTblRecordsRefreshed = false;
	long pg_start;
	int last_row_id;
	int space_available;
	RandomAccessFile binFile;
	private boolean is_index_pg_clean;
	List<InternalTableRecord> LeftChild;
	private IndexNode incoming_insert;
	public DataType index_val_datatype;
	public TreeSet<Long> IdxValues;
	public TreeSet<String> SidxValues;
	public HashMap<String, IndexRecord> IdxValuePointer;
	private Map<Integer, TableRecord> records_map;

	public Page(RandomAccessFile file, int pgnum) {
		try {
			this.pgnum = pgnum;
			index_val_datatype = null;
			IdxValues = new TreeSet<>();
			SidxValues = new TreeSet<>();
			IdxValuePointer = new HashMap<String, IndexRecord>();
			records_map = new HashMap<>();
			this.binFile = file;
			last_row_id = 0;
			pg_start = AppFileHandler.page_size * pgnum;
			binFile.seek(pg_start);
			pg_type = PageType.get(binFile.readByte()); // pagetype
			binFile.readByte(); // unused
			CellCnt = binFile.readShort();
			content_starting_offset = binFile.readShort();
			space_available = content_starting_offset - 0x10 - (CellCnt * 2);

			RightPage = binFile.readInt();

			ParentPgNumber = binFile.readInt();

			binFile.readShort();
			if (pg_type == PageType.LEAF)
				fill_table_records();
			if (pg_type == PageType.INTERIOR)
				fill_left_children();
			if (pg_type == PageType.INTERIORINDEX || pg_type == PageType.LEAFINDEX)
				fill_index_records();

		} catch (IOException ex) {
			System.out.println("! Error while reading the page " + ex.getMessage());
		}
	}

	public List<TableRecord> GetPageRecords() {

		if (IsTblRecordsRefreshed)
			fill_table_records();

		IsTblRecordsRefreshed = false;

		return records;
	}

	private void del_pg_record(short recordIndex) {
		try {

			for (int i = recordIndex + 1; i < CellCnt; i++) {
				binFile.seek(pg_start + 0x10 + (i * 2));
				short cell_start = binFile.readShort();

				if (cell_start == 0)
					continue;

				binFile.seek(pg_start + 0x10 + ((i - 1) * 2));
				binFile.writeShort(cell_start);
			}

			CellCnt--;

			binFile.seek(pg_start + 2);
			binFile.writeShort(CellCnt);

		} catch (IOException e) {
			System.out.println("Unable to delete record at " + recordIndex + "in page " + pgnum);
		}
	}

	public void del_table_record(String TableName, short recordIndex) {
		del_pg_record(recordIndex);
		MetaDataOfTable metaData = new MetaDataOfTable(TableName);
		metaData.record_count--;
		metaData.UpdateMetaData();
		IsTblRecordsRefreshed = true;

	}

	private void AddNewPgRecord(Byte[] RecordHead, Byte[] record_content) throws IOException {
		if (RecordHead.length + record_content.length + 4 > space_available) {
			try {
				if (pg_type == PageType.LEAF || pg_type == PageType.INTERIOR) {
					table_overflow_handling();
				} else {
					index_overflow_handling();
					return;
				}
			} catch (IOException e) {
				System.out.println("! Error while table_overflow_handling");
			}
		}

		short cell_start = content_starting_offset;

		short new_cell_start = Integer.valueOf((cell_start - record_content.length - RecordHead.length - 2))
				.shortValue();
		binFile.seek(pgnum * AppFileHandler.page_size + new_cell_start);

		// record head
		binFile.write(BytesConversion.Bytes_to_bytes(RecordHead)); // datatypes

		// record body
		binFile.write(BytesConversion.Bytes_to_bytes(record_content));

		binFile.seek(pg_start + 0x10 + (CellCnt * 2));
		binFile.writeShort(new_cell_start);

		content_starting_offset = new_cell_start;

		binFile.seek(pg_start + 4);
		binFile.writeShort(content_starting_offset);

		CellCnt++;
		binFile.seek(pg_start + 2);
		binFile.writeShort(CellCnt);

		space_available = content_starting_offset - 0x10 - (CellCnt * 2);

	}

	private void index_overflow_handling() throws IOException {
		if (pg_type == PageType.LEAFINDEX) {
			if (ParentPgNumber == -1) {
				ParentPgNumber = add_new_pg(binFile, PageType.INTERIORINDEX, pgnum, -1);
			}
			int newLeftLeafPageNo = add_new_pg(binFile, PageType.LEAFINDEX, pgnum, ParentPgNumber);

			setParent(ParentPgNumber);
			IndexNode incoming_insert_temp = this.incoming_insert;

			Page left_leaf_pg = new Page(binFile, newLeftLeafPageNo);
			IndexNode to_insert_parent_index_node = index_split_records_between_pgs(left_leaf_pg);
			Page parent_pg = new Page(binFile, ParentPgNumber);
			int comparison_result = Condition.compare(incoming_insert_temp.index_val.field_value,
					to_insert_parent_index_node.index_val.field_value, incoming_insert.index_val.data_type);

			if (comparison_result == 0) {
				to_insert_parent_index_node.row_ids.addAll(incoming_insert_temp.row_ids);
				parent_pg.add_index(to_insert_parent_index_node, newLeftLeafPageNo);
				ShiftPage(parent_pg);
				return;
			} else if (comparison_result < 0) {
				left_leaf_pg.add_index(incoming_insert_temp);
				ShiftPage(left_leaf_pg);
			} else {
				add_index(incoming_insert_temp);
			}

			parent_pg.add_index(to_insert_parent_index_node, newLeftLeafPageNo);

		}

		else {

			if (CellCnt < 3 && !is_index_pg_clean) {
				is_index_pg_clean = true;
				String[] temp_index_vals = get_index_vals().toArray(new String[get_index_vals().size()]);
				@SuppressWarnings("unchecked")
				HashMap<String, IndexRecord> indexValuePointerTemp = (HashMap<String, IndexRecord>) IdxValuePointer
						.clone();
				IndexNode incoming_insert_temp = this.incoming_insert;
				clean_page();
				for (int i = 0; i < temp_index_vals.length; i++) {
					add_index(indexValuePointerTemp.get(temp_index_vals[i]).getIndexNode(),
							indexValuePointerTemp.get(temp_index_vals[i]).left_pgno);
				}

				add_index(incoming_insert_temp);
				return;
			}

			if (is_index_pg_clean) {
				System.out.println(
						"! Page overflow, increase the page size. Reached Max number of rows for an Index value");
				return;
			}

			if (ParentPgNumber == -1) {
				ParentPgNumber = add_new_pg(binFile, PageType.INTERIORINDEX, pgnum, -1);
			}
			int new_left_internal_pgnum = add_new_pg(binFile, PageType.INTERIORINDEX, pgnum, ParentPgNumber);

			setParent(ParentPgNumber);

			IndexNode incoming_insert_temp = this.incoming_insert;
			Page leftInteriorPage = new Page(binFile, new_left_internal_pgnum);

			IndexNode to_insert_parent_index_node = index_split_records_between_pgs(leftInteriorPage);

			Page parent_pg = new Page(binFile, ParentPgNumber);
			int comparison_result = Condition.compare(incoming_insert_temp.index_val.field_value,
					to_insert_parent_index_node.index_val.field_value, incoming_insert.index_val.data_type);
			Page middle_orphan = new Page(binFile, to_insert_parent_index_node.left_page_number);
			middle_orphan.setParent(ParentPgNumber);
			leftInteriorPage.setRightPageNo(middle_orphan.pgnum);

			if (comparison_result == 0) {
				to_insert_parent_index_node.row_ids.addAll(incoming_insert_temp.row_ids);
				parent_pg.add_index(to_insert_parent_index_node, new_left_internal_pgnum);
				ShiftPage(parent_pg);
				return;
			} else if (comparison_result < 0) {
				leftInteriorPage.add_index(incoming_insert_temp);
				ShiftPage(leftInteriorPage);
			} else {
				add_index(incoming_insert_temp);
			}

			parent_pg.add_index(to_insert_parent_index_node, new_left_internal_pgnum);

		}

	}

	private void clean_page() throws IOException {

		CellCnt = 0;
		content_starting_offset = Long.valueOf(AppFileHandler.page_size).shortValue();
		space_available = content_starting_offset - 0x10 - (CellCnt * 2); 
		byte[] emptybytes = new byte[512 - 16];
		Arrays.fill(emptybytes, (byte) 0);
		binFile.seek(pg_start + 16);
		binFile.write(emptybytes);
		binFile.seek(pg_start + 2);
		binFile.writeShort(CellCnt);
		binFile.seek(pg_start + 4);
		binFile.writeShort(content_starting_offset);
		IdxValues = new TreeSet<>();
		SidxValues = new TreeSet<>();
		IdxValuePointer = new HashMap<>();

	}

	private IndexNode index_split_records_between_pgs(Page newleftPage) throws IOException {

		try {
			int mid = get_index_vals().size() / 2;
			String[] temp_index_vals = get_index_vals().toArray(new String[get_index_vals().size()]);

			IndexNode to_insert_parent_index_node = IdxValuePointer.get(temp_index_vals[mid]).getIndexNode();
			to_insert_parent_index_node.left_page_number = IdxValuePointer.get(temp_index_vals[mid]).left_pgno;

			@SuppressWarnings("unchecked")
			HashMap<String, IndexRecord> indexValuePointerTemp = (HashMap<String, IndexRecord>) IdxValuePointer
					.clone();

			for (int i = 0; i < mid; i++) {
				newleftPage.add_index(indexValuePointerTemp.get(temp_index_vals[i]).getIndexNode(),
						indexValuePointerTemp.get(temp_index_vals[i]).left_pgno);
			}

			clean_page();
			SidxValues = new TreeSet<>();
			IdxValues = new TreeSet<>();
			IdxValuePointer = new HashMap<String, IndexRecord>();
			for (int i = mid + 1; i < temp_index_vals.length; i++) {
				add_index(indexValuePointerTemp.get(temp_index_vals[i]).getIndexNode(),
						indexValuePointerTemp.get(temp_index_vals[i]).left_pgno);
			}

			return to_insert_parent_index_node;
		} catch (IOException e) {
			System.out.println("! Insert into Index File failed. Error while splitting index pages");
			throw e;
		}

	}

	private void table_overflow_handling() throws IOException {
		if (pg_type == PageType.LEAF) {
			int new_rightleaf_pgnum = add_new_pg(binFile, pg_type, -1, -1);
			if (ParentPgNumber == -1) {
				int new_parent_pgnum = add_new_pg(binFile, PageType.INTERIOR, new_rightleaf_pgnum, -1);
				setRightPageNo(new_rightleaf_pgnum);
				setParent(new_parent_pgnum);
				Page new_parent_pg = new Page(binFile, new_parent_pgnum);
				new_parent_pgnum = new_parent_pg.add_left_table_child(pgnum, last_row_id);
				new_parent_pg.setRightPageNo(new_rightleaf_pgnum);
				Page new_leaf_pg = new Page(binFile, new_rightleaf_pgnum);
				new_leaf_pg.setParent(new_parent_pgnum);
				ShiftPage(new_leaf_pg);
			} else {
				Page parent_pg = new Page(binFile, ParentPgNumber);
				ParentPgNumber = parent_pg.add_left_table_child(pgnum, last_row_id);
				parent_pg.setRightPageNo(new_rightleaf_pgnum);
				setRightPageNo(new_rightleaf_pgnum);
				Page new_leaf_pg = new Page(binFile, new_rightleaf_pgnum);
				new_leaf_pg.setParent(ParentPgNumber);
				ShiftPage(new_leaf_pg);
			}
		} else {
			int new_rightleaf_pgnum = add_new_pg(binFile, pg_type, -1, -1);
			int new_parent_pgnum = add_new_pg(binFile, PageType.INTERIOR, new_rightleaf_pgnum, -1);
			setRightPageNo(new_rightleaf_pgnum);
			setParent(new_parent_pgnum);
			Page new_parent_pg = new Page(binFile, new_parent_pgnum);
			new_parent_pgnum = new_parent_pg.add_left_table_child(pgnum, last_row_id);
			new_parent_pg.setRightPageNo(new_rightleaf_pgnum);
			Page new_leaf_pg = new Page(binFile, new_rightleaf_pgnum);
			new_leaf_pg.setParent(new_parent_pgnum);
			ShiftPage(new_leaf_pg);
		}
	}

	private int add_left_table_child(int LeftChildPgnum, int rowId) throws IOException {
		for (InternalTableRecord intRecord : LeftChild) {
			if (intRecord.row_id == rowId)
				return pgnum;
		}
		if (pg_type == PageType.INTERIOR) {
			List<Byte> RecordHead = new ArrayList<>();
			List<Byte> record_content = new ArrayList<>();

			RecordHead.addAll(Arrays.asList(BytesConversion.int_to_Bytes(LeftChildPgnum)));
			record_content.addAll(Arrays.asList(BytesConversion.int_to_Bytes(rowId)));

			AddNewPgRecord(RecordHead.toArray(new Byte[RecordHead.size()]),
					record_content.toArray(new Byte[record_content.size()]));
		}
		return pgnum;

	}

	public void add_index(IndexNode node) throws IOException {
		add_index(node, -1);
	}

	public void add_index(IndexNode node, int left_pgno) throws IOException {
		incoming_insert = node;
		incoming_insert.left_page_number = left_pgno;
		List<Integer> rowIds = new ArrayList<>();
		List<String> ixValues = get_index_vals();
		if (get_index_vals().contains(node.index_val.field_value)) {
			left_pgno = IdxValuePointer.get(node.index_val.field_value).left_pgno;
			incoming_insert.left_page_number = left_pgno;
			rowIds = IdxValuePointer.get(node.index_val.field_value).row_ids;
			rowIds.addAll(incoming_insert.row_ids);
			incoming_insert.row_ids = rowIds;
			del_pg_record(IdxValuePointer.get(node.index_val.field_value).pg_head_index);
			if (index_val_datatype == DataType.TEXT || index_val_datatype == null)
				SidxValues.remove(node.index_val.field_value);
			else
				IdxValues.remove(Long.parseLong(node.index_val.field_value));
		}

		rowIds.addAll(node.row_ids);

		rowIds = new ArrayList<>(new HashSet<>(rowIds));

		List<Byte> recordHead = new ArrayList<>();
		List<Byte> record_content = new ArrayList<>();
		record_content.addAll(Arrays.asList(Integer.valueOf(rowIds.size()).byteValue()));
		if (node.index_val.data_type == DataType.TEXT)
			record_content.add(Integer
					.valueOf(node.index_val.data_type.getValue() + node.index_val.field_value.length()).byteValue());
		else
			record_content.add(node.index_val.data_type.getValue());

		// index value
		record_content.addAll(Arrays.asList(node.index_val.field_value_Byte));

		// list of rowids
		for (int i = 0; i < rowIds.size(); i++) {
			record_content.addAll(Arrays.asList(BytesConversion.int_to_Bytes(rowIds.get(i))));
		}

		short payload = Integer.valueOf(record_content.size()).shortValue();
		if (pg_type == PageType.INTERIORINDEX)
			recordHead.addAll(Arrays.asList(BytesConversion.int_to_Bytes(left_pgno)));

		recordHead.addAll(Arrays.asList(BytesConversion.short_to_Bytes(payload)));

		AddNewPgRecord(recordHead.toArray(new Byte[recordHead.size()]),
				record_content.toArray(new Byte[record_content.size()]));

		fill_index_records();
		refresh_head_offset();

	}

	private void refresh_head_offset() {
		try {
			binFile.seek(pg_start + 0x10);
			for (String indexVal : get_index_vals()) {
				binFile.writeShort(IdxValuePointer.get(indexVal).pg_offset);
			}

		} catch (IOException ex) {
			System.out.println("! Error while refrshing header offset " + ex.getMessage());
		}
	}
	private void fill_table_records() {
		short payLoadSize = 0;
		byte noOfcolumns = 0;
		records = new ArrayList<TableRecord>();
		records_map = new HashMap<>();
		try {
			for (short i = 0; i < CellCnt; i++) {
				binFile.seek(pg_start + 0x10 + (i * 2));
				short cell_start = binFile.readShort();
				if (cell_start == 0)
					continue;
				binFile.seek(pg_start + cell_start);

				payLoadSize = binFile.readShort();
				int rowId = binFile.readInt();
				noOfcolumns = binFile.readByte();

				if (last_row_id < rowId)
					last_row_id = rowId;

				byte[] colDatatypes = new byte[noOfcolumns];
				byte[] record_content = new byte[payLoadSize - noOfcolumns - 1];

				binFile.read(colDatatypes);
				binFile.read(record_content);

				TableRecord record = new TableRecord(i, rowId, cell_start, colDatatypes, record_content);
				records.add(record);
				records_map.put(rowId, record);
			}
		} catch (IOException ex) {
			System.out.println("! Error while filling records from the page " + ex.getMessage());
		}
	}
	private void fill_left_children() {
		try {
			LeftChild = new ArrayList<>();

			int LeftChildPgnum = 0;
			int rowId = 0;
			for (int i = 0; i < CellCnt; i++) {
				binFile.seek(pg_start + 0x10 + (i * 2));
				short cell_start = binFile.readShort();
				if (cell_start == 0)// ignore deleted cells
					continue;
				binFile.seek(pg_start + cell_start);

				LeftChildPgnum = binFile.readInt();
				rowId = binFile.readInt();
				LeftChild.add(new InternalTableRecord(rowId, LeftChildPgnum));
			}
		} catch (IOException ex) {
			System.out.println("! Error while filling records from the page " + ex.getMessage());
		}

	}

	private void fill_index_records() {
		try {
			IdxValues = new TreeSet<>();
			SidxValues = new TreeSet<>();
			IdxValuePointer = new HashMap<>();

			int left_pgno = -1;
			byte noOfRowIds = 0;
			byte dataType = 0;
			for (short i = 0; i < CellCnt; i++) {
				binFile.seek(pg_start + 0x10 + (i * 2));
				short cell_start = binFile.readShort();
				if (cell_start == 0)// ignore deleted cells
					continue;
				binFile.seek(pg_start + cell_start);

				if (pg_type == PageType.INTERIORINDEX)
					left_pgno = binFile.readInt();

				short payload = binFile.readShort(); // payload

				noOfRowIds = binFile.readByte();
				dataType = binFile.readByte();

				if (index_val_datatype == null && DataType.get(dataType) != DataType.NULL)
					index_val_datatype = DataType.get(dataType);

				byte[] indexValue = new byte[DataType.getLength(dataType)];
				binFile.read(indexValue);

				List<Integer> lstRowIds = new ArrayList<>();
				for (int j = 0; j < noOfRowIds; j++) {
					lstRowIds.add(binFile.readInt());
				}

				IndexRecord record = new IndexRecord(i, DataType.get(dataType), noOfRowIds, indexValue, lstRowIds,
						left_pgno, RightPage, pgnum, cell_start);

				if (index_val_datatype == DataType.TEXT || index_val_datatype == null)
					SidxValues.add(record.getIndexNode().index_val.field_value);
				else
					IdxValues.add(Long.parseLong(record.getIndexNode().index_val.field_value));

				IdxValuePointer.put(record.getIndexNode().index_val.field_value, record);

			}
		} catch (IOException ex) {
			System.out.println("Error while filling records from the page " + ex.getMessage());
		}
	}

	public List<String> get_index_vals() {
		List<String> strIndexValues = new ArrayList<>();

		if (SidxValues.size() > 0)
			strIndexValues.addAll(Arrays.asList(SidxValues.toArray(new String[SidxValues.size()])));
		if (IdxValues.size() > 0) {
			Long[] lArray = IdxValues.toArray(new Long[IdxValues.size()]);
			for (int i = 0; i < lArray.length; i++) {
				strIndexValues.add(lArray[i].toString());
			}
		}

		return strIndexValues;

	}

	public boolean IsRoot() {
		return ParentPgNumber == -1;
	}

	public static PageType GetPageType(RandomAccessFile file, int pgnum) throws IOException {
		try {
			int pg_start = AppFileHandler.page_size * pgnum;
			file.seek(pg_start);
			return PageType.get(file.readByte());
		} catch (IOException ex) {
			System.out.println("Error while getting the page type " + ex.getMessage());
			throw ex;
		}
	}

	public static int add_new_pg(RandomAccessFile file, PageType pg_type, int RightPage, int ParentPgNumber) {
		try {
			int pgnum = Long.valueOf((file.length() / AppFileHandler.page_size)).intValue();
			file.setLength(file.length() + AppFileHandler.page_size);
			file.seek(AppFileHandler.page_size * pgnum);
			file.write(pg_type.getValue());
			file.write(0x00); // unused
			file.writeShort(0); // no of cells
			file.writeShort((short) (AppFileHandler.page_size)); // cell
																		// start
																		// offset

			file.writeInt(RightPage);

			file.writeInt(ParentPgNumber);

			return pgnum;
		} catch (IOException ex) {
			System.out.println("Error while adding new page" + ex.getMessage());
			return -1;
		}
	}

	public void update_record(TableRecord record, int ordinal_pos, Byte[] newValue) throws IOException {
		binFile.seek(pg_start + record.record_offset + 7);
		int value_offset = 0;
		for (int i = 0; i < ordinal_pos; i++) {
			value_offset += DataType.getLength((byte) binFile.readByte());
		}

		binFile.seek(pg_start + record.record_offset + 7 + record.col_data_types.length + value_offset);
		binFile.write(BytesConversion.Bytes_to_bytes(newValue));

	}

	// Copies all the members from the new page to the current page
	private void ShiftPage(Page newPage) {
		pg_type = newPage.pg_type;
		CellCnt = newPage.CellCnt;
		pgnum = newPage.pgnum;
		content_starting_offset = newPage.content_starting_offset;
		RightPage = newPage.RightPage;
		ParentPgNumber = newPage.ParentPgNumber;
		LeftChild = newPage.LeftChild;
		SidxValues = newPage.SidxValues;
		IdxValues = newPage.IdxValues;
		IdxValuePointer = newPage.IdxValuePointer;
		records = newPage.records;
		pg_start = newPage.pg_start;
		space_available = newPage.space_available;
	}
	public void setParent(int ParentPgNumber) throws IOException {
		binFile.seek(AppFileHandler.page_size * pgnum + 0x0A);
		binFile.writeInt(ParentPgNumber);
		this.ParentPgNumber = ParentPgNumber;
	}

	public void setRightPageNo(int rightPageNo) throws IOException {
		binFile.seek(AppFileHandler.page_size * pgnum + 0x06);
		binFile.writeInt(rightPageNo);
		this.RightPage = rightPageNo;
	}

	public void del_index(IndexNode node) throws IOException {
		del_pg_record(IdxValuePointer.get(node.index_val.field_value).pg_head_index);
		fill_index_records();
		refresh_head_offset();
	}

	public void AddNewColumn(ColumnInformation columnInfo) throws IOException {
		try {
			add_table_row(AppFileHandler.columns_table,
					Arrays.asList(new Attribute[] { new Attribute(DataType.TEXT, columnInfo.TableName),
							new Attribute(DataType.TEXT, columnInfo.col_name),
							new Attribute(DataType.TEXT, columnInfo.data_type.toString()),
							new Attribute(DataType.SMALLINT, columnInfo.ordinal_pos.toString()),
							new Attribute(DataType.TEXT, columnInfo.is_null_col ? "YES" : "NO"),
							columnInfo.is_key_primary ? new Attribute(DataType.TEXT, "PRI")
									: new Attribute(DataType.NULL, "NULL"),
							new Attribute(DataType.TEXT, columnInfo.is_unique_col ? "YES" : "NO") }));
		} catch (Exception e) {
			System.out.println("! Could not add column");
		}
	}

	public int add_table_row(String TableName, List<Attribute> attributes) throws IOException {
		List<Byte> colDataTypes = new ArrayList<Byte>();
		List<Byte> record_content = new ArrayList<Byte>();

		MetaDataOfTable metaData = null;
		if (AppFileHandler.IsDataInitialized) {
			metaData = new MetaDataOfTable(TableName);
			if (!metaData.validate_insertion(attributes))
				return -1;
		}

		for (Attribute attribute : attributes) {
			record_content.addAll(Arrays.asList(attribute.field_value_Byte));
			if (attribute.data_type == DataType.TEXT) {
				colDataTypes.add(Integer
						.valueOf(DataType.TEXT.getValue() + (new String(attribute.field_value).length())).byteValue());
			} else {
				colDataTypes.add(attribute.data_type.getValue());
			}
		}

		last_row_id++;

		short payLoadSize = Integer.valueOf(record_content.size() + colDataTypes.size() + 1).shortValue();

		List<Byte> RecordHead = new ArrayList<>();

		RecordHead.addAll(Arrays.asList(BytesConversion.short_to_Bytes(payLoadSize))); 
		RecordHead.addAll(Arrays.asList(BytesConversion.int_to_Bytes(last_row_id))); 
		RecordHead.add(Integer.valueOf(colDataTypes.size()).byteValue()); 
		RecordHead.addAll(colDataTypes); 

		AddNewPgRecord(RecordHead.toArray(new Byte[RecordHead.size()]),
				record_content.toArray(new Byte[record_content.size()]));

		IsTblRecordsRefreshed = true;
		if (AppFileHandler.IsDataInitialized) {
			metaData.record_count++;
			metaData.UpdateMetaData();
		}
		return last_row_id;
	}

}
