package dbms;
import java.io.File;

public class ColumnInformation
{
    public DataType data_type;
    public String col_name;
    public boolean is_unique_col;
    public boolean is_null_col;
    public Short ordinal_pos;
    public boolean has_index;
    public String TableName;
    public boolean is_key_primary;

    public void set_key_asPrimary()
    {
        is_key_primary = true;
    }

    ColumnInformation(){}

    ColumnInformation(String TableName,DataType datatype,String column_name,boolean is_Unique,boolean is_Null,short ordinal_pos){
        this.data_type = datatype;
        this.col_name = column_name;
        this.is_unique_col = is_Unique;
        this.is_null_col = is_Null;
        this.ordinal_pos = ordinal_pos;
        this.TableName = TableName;

        this.has_index = (new File(App.getNDXFilePath(TableName, column_name)).exists());

    }   
}