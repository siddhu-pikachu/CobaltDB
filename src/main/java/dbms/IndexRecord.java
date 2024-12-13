package dbms;

import java.util.List;

public class IndexRecord{
    public Byte rowids_count;
    public DataType data_type;
    public Byte[] index_value;
    public List<Integer> row_ids;
    public short pg_head_index;
    public short pg_offset;
    int left_pgno;
    int right_pgno;
    int pgnum;
    private IndexNode index_node;

    public IndexNode getIndexNode()
    {
        return index_node;
    }

    IndexRecord(short pg_head_index,DataType data_type,Byte NoOfRowIds, byte[] index_value, List<Integer> row_ids, int left_pgno,int right_pgno,int pgnum,short pg_offset){
      
        this.pg_offset = pg_offset;
        this.pg_head_index = pg_head_index;
        this.rowids_count = NoOfRowIds;
        this.data_type = data_type;
        this.index_value = BytesConversion.byte_to_Bytes(index_value);
        this.row_ids = row_ids;

        index_node = new IndexNode(new Attribute(this.data_type, index_value),row_ids);
        this.left_pgno = left_pgno;
        this.right_pgno = right_pgno;
        this.pgnum = pgnum;
    }

}