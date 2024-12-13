package dbms;

public class InternalTableRecord
{
    public int LeftChildPgnum;
    public int row_id;

    public InternalTableRecord(int rowId, int leftChildPageNo){
        this.row_id = rowId;this.LeftChildPgnum = leftChildPageNo;  
    }

}
