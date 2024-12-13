package dbms;

import java.util.List;

public class IndexNode{
    public Attribute index_val;
    public List<Integer> row_ids;
    public boolean is_interiorNode;
    public int left_page_number;

    public IndexNode(Attribute indexValue,List<Integer> rowids)
    {
        this.index_val = indexValue;
        this.row_ids = rowids;
    }

}