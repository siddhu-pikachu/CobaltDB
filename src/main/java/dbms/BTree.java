package dbms;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BTree {
    Page root;
    RandomAccessFile binFile;

    public BTree(RandomAccessFile file) {
        this.binFile = file;
        this.root = new Page(binFile, AppFileHandler.GetRootPageNum(binFile));
    }

    private String binarySearch(String[] values,String searchValue,int start, int end , DataType dataType)
    {

        if(end - start <= 3)
        {
            int i =start;
            for(i=start;i <end;i++){
                if(Condition.compare(values[i], searchValue, dataType) < 0)
                    continue;
                else
                    break;
            }
            return values[i];
        }
        else{
            
                int mid = (end - start) / 2 + start;
                if(values[mid].equals(searchValue))
                    return values[mid];

                    if(Condition.compare(values[mid], searchValue, dataType) < 0)
                    return binarySearch(values,searchValue,mid + 1,end,dataType);
                else 
                    return binarySearch(values,searchValue,start,mid - 1,dataType);
            
        }
     
    }


    private int get_closest_pagenum(Page page, String value) {
        if (page.pg_type == PageType.LEAFINDEX) {
            return page.pgnum;
        } else {
            if (Condition.compare(value , page.get_index_vals().get(0),page.index_val_datatype) < 0)
                return get_closest_pagenum
                    (new Page(binFile,page.IdxValuePointer.get(page.get_index_vals().get(0)).left_pgno),
                        value);
            else if(Condition.compare(value,page.get_index_vals().get(page.get_index_vals().size()-1),page.index_val_datatype) > 0)
                return get_closest_pagenum(
                    new Page(binFile,page.RightPage),
                        value);
            else{
                String closest_value = binarySearch(page.get_index_vals().toArray(new String[page.get_index_vals().size()]),value,0,page.get_index_vals().size() -1,page.index_val_datatype);
                int i = page.get_index_vals().indexOf(closest_value);
                List<String> index_values = page.get_index_vals();
                if(closest_value.compareTo(value) < 0 && i+1 < index_values.size())
                {
                    return page.IdxValuePointer.get(index_values.get(i+1)).left_pgno;
                }
                else if(closest_value.compareTo(value) > 0)
                {
                    return page.IdxValuePointer.get(closest_value).left_pgno;
                }
                else{
                    return page.pgnum;
                }
            }
        }
    }

    public void insert(Attribute attribute,List<Integer> row_ids)
    {
        try{
            int page_num = get_closest_pagenum(root, attribute.field_value) ;
            Page page = new Page(binFile, page_num);
            page.add_index(new IndexNode(attribute,row_ids));
            }
            catch(IOException e)
            {
                 System.out.println("Failed to insert " + attribute.field_value +" into index file");
            }
    }

    public void insert(Attribute attribute,int row_id)
    {
        insert(attribute,Arrays.asList(row_id));
    }

    public void delete(Attribute attribute, int row_id)
    {
        
        try{
            int page_num = get_closest_pagenum(root, attribute.field_value) ;
            Page page = new Page(binFile, page_num);
            
            IndexNode temp_node = page.IdxValuePointer.get(attribute.field_value).getIndexNode();
            temp_node.row_ids.remove(temp_node.row_ids.indexOf(row_id));
            page.del_index(temp_node);
            if(temp_node.row_ids.size() !=0)
               page.add_index(temp_node);
            }
            catch(IOException e)
            {
                 System.out.println("Failed to delete " + attribute.field_value +" from index file");
            }

    }

    public List<Integer> get_row_ids(Condition condition)
    {
        List<Integer> row_ids = new ArrayList<>();
        Page page = new Page(binFile,get_closest_pagenum(root, condition.comparator_value));
        String[] index_values= page.get_index_vals().toArray(new String[page.get_index_vals().size()]);
        OperandType operationType = condition.getOperation();
        for(int i=0;i < index_values.length;i++)
        {
            if(condition.condition_check(page.IdxValuePointer.get(index_values[i]).getIndexNode().index_val.field_value))
                row_ids.addAll(page.IdxValuePointer.get(index_values[i]).row_ids);
        } 
        if(operationType == OperandType.LESSTHAN || operationType == OperandType.LESSTHANOREQUAL)
        {
           if(page.pg_type == PageType.LEAFINDEX)
               row_ids.addAll(getAllRowIdsLeftOf(page.ParentPgNumber,index_values[0]));
           else 
                row_ids.addAll(getAllRowIdsLeftOf(page.pgnum,condition.comparator_value));
        }   
        
        if(operationType == OperandType.GREATERTHAN || operationType == OperandType.GREATERTHANOREQUAL)
        {
         if(page.pg_type == PageType.LEAFINDEX)
            row_ids.addAll(get_all_rowids_atRight(page.ParentPgNumber,index_values[index_values.length - 1]));
            else 
              row_ids.addAll(get_all_rowids_atRight(page.pgnum,condition.comparator_value));
        }
        
        return row_ids;

    }

    private List<Integer> getAllRowIdsLeftOf(int page_num, String index_value)
    {
        List<Integer> row_ids = new ArrayList<>();
        if(page_num == -1)
             return row_ids;
        Page page = new Page(this.binFile,page_num);
        List<String> index_values = Arrays.asList(page.get_index_vals().toArray(new String[page.get_index_vals().size()]));

      
        for(int i=0;i< index_values.size() && Condition.compare(index_values.get(i), index_value, page.index_val_datatype) < 0 ;i++)
        {           
               row_ids.addAll(page.IdxValuePointer.get(index_values.get(i)).getIndexNode().row_ids);
               add_all_children_rowids(page.IdxValuePointer.get(index_values.get(i)).left_pgno, row_ids);         
        }         
         if(page.IdxValuePointer.get(index_value)!= null)
         add_all_children_rowids(page.IdxValuePointer.get(index_value).left_pgno, row_ids);
        return row_ids;
    }

    private List<Integer> get_all_rowids_atRight(int page_num, String index_value)
    {
        
        List<Integer> row_ids = new ArrayList<>();

        if(page_num == -1)
            return row_ids;
        Page page = new Page(this.binFile,page_num);
        List<String> index_values = Arrays.asList(page.get_index_vals().toArray(new String[page.get_index_vals().size()]));
        for(int i=index_values.size() - 1; i >= 0 && Condition.compare(index_values.get(i), index_value, page.index_val_datatype) > 0; i--)
        {
               row_ids.addAll(page.IdxValuePointer.get(index_values.get(i)).getIndexNode().row_ids);
                 add_all_children_rowids(page.RightPage, row_ids);
        }

        if(page.IdxValuePointer.get(index_value)!= null)
        add_all_children_rowids(page.IdxValuePointer.get(index_value).right_pgno, row_ids);

        return row_ids;
    }

    private void add_all_children_rowids(int page_num,List<Integer> row_ids)
    {
        if(page_num == -1)
            return;
        Page page = new Page(this.binFile, page_num);
            for (IndexRecord record :page.IdxValuePointer.values())
            {
                row_ids.addAll(record.row_ids);
                if(page.pg_type == PageType.INTERIORINDEX)
                 {
                    add_all_children_rowids(record.left_pgno, row_ids);
                    add_all_children_rowids(record.right_pgno, row_ids);
                 }
            }  
    }

}