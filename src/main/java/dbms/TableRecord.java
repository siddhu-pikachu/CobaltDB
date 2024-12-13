package dbms;

import java.util.List;

import java.util.Arrays;
import java.util.ArrayList;


public class TableRecord
{
    public int row_id;
    public Byte[] col_data_types;
    public Byte[] record_content;
    private List<Attribute> attributes;
    public short record_offset;
    public short pg_head_index;

    public List<Attribute> getAttributes()
    {
        return attributes;
    }

    private void setAttributes()
    {
        attributes = new ArrayList<>();
        int pointer = 0;
        for(Byte col_data_type : col_data_types)
        {
             byte[] fieldValue = BytesConversion.Bytes_to_bytes(Arrays.copyOfRange(record_content,pointer, pointer + DataType.getLength(col_data_type)));
             attributes.add(new Attribute(DataType.get(col_data_type), fieldValue));
                    pointer =  pointer + DataType.getLength(col_data_type);
        }
    }

    TableRecord(short pg_head_index,int row_id, short record_offset, byte[] col_data_types, byte[] record_content)
    {
        this.row_id = row_id;
        this.record_content= BytesConversion.byte_to_Bytes(record_content);
        this.col_data_types = BytesConversion.byte_to_Bytes(col_data_types);
        this.record_offset =  record_offset;
        this.pg_head_index = pg_head_index;
        setAttributes();
    }
    
}
