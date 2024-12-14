package dbms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;
import java.io.RandomAccessFile;



public class MetaDataOfTable{

    public int record_count;
    public List<TableRecord> column_data;
    public List<ColumnInformation> column_name_attributes;
    public List<String> column_names;
    public String TableName;
    public boolean IsTableExist;
    public int root_page_number;
    public int last_rowid;

    public MetaDataOfTable(String TableName)
    {
        this.TableName = TableName;
        IsTableExist = false;
        try {

            RandomAccessFile cobaltdbTablesCatalog = new RandomAccessFile(
                App.getTBLFilePath(AppFileHandler.tables_table), "r");
            int root_page_number = AppFileHandler.GetRootPageNum(cobaltdbTablesCatalog);
           
            BPlusTree bplusTree = new BPlusTree(cobaltdbTablesCatalog, root_page_number,TableName);
            for (Integer pageNo : bplusTree.GetAllLeaves()) {
               Page page = new Page(cobaltdbTablesCatalog, pageNo);
               for (TableRecord record : page.GetPageRecords()) {
                  if (new String(record.getAttributes().get(0).field_value).equals(TableName)) {
                    this.root_page_number = Integer.parseInt(record.getAttributes().get(3).field_value);
                    record_count = Integer.parseInt(record.getAttributes().get(1).field_value);
                    IsTableExist = true;
                     break;
                  }
               }
               if(IsTableExist)
                break;
            }
   
            cobaltdbTablesCatalog.close();
            if(IsTableExist)
            {
               LoadColumnData();
            } else {
               throw new Exception("Table does not exist.");
            }
            
         } catch (Exception e) {
           System.out.println("! Error while checking Table " + TableName + " exists.");
           
         }
    }

public boolean validate_insertion(List<Attribute> row) throws IOException
 {
  RandomAccessFile table_file = new RandomAccessFile(App.getTBLFilePath(TableName), "r");
  AppFileHandler file = new AppFileHandler(table_file);
         
     
     for(int i=0;i<column_name_attributes.size();i++)
     {
     
        Condition condition = new Condition(column_name_attributes.get(i).data_type);
         condition.column_name = column_name_attributes.get(i).col_name;
         condition.column_ordinal = i;
         condition.setOperator("=");

        if(column_name_attributes.get(i).is_unique_col)
        {
         condition.setConditionValue(row.get(i).field_value);
            if(file.is_record_existing(this, Arrays.asList(column_name_attributes.get(i).col_name), condition)){
          System.out.println("! Insert failed: Column "+ column_name_attributes.get(i).col_name + " should be unique." );
               table_file.close();
            return false;
        }
      
        }     
     }
 table_file.close();
     return true;
 }


  public boolean is_column_existing(List<String> columns) {

   if(columns.size() == 0)
      return true;
      
      List<String> Icolumns =new ArrayList<>(columns);

   for (ColumnInformation column_name_attr : column_name_attributes) {
      if (Icolumns.contains(column_name_attr.col_name))
         Icolumns.remove(column_name_attr.col_name);
   }

   return Icolumns.isEmpty();
}



public void UpdateMetaData()
{

  try{
     RandomAccessFile table_file = new RandomAccessFile(
        App.getTBLFilePath(TableName), "r");
  
        Integer root_page_number = AppFileHandler.GetRootPageNum(table_file);
        table_file.close();
         
        
        RandomAccessFile cobaltdbTablesCatalog = new RandomAccessFile(
                     App.getTBLFilePath(AppFileHandler.tables_table), "rw");
      
        AppFileHandler tablesBinaryFile = new AppFileHandler(cobaltdbTablesCatalog);

        MetaDataOfTable tablesMetaData = new MetaDataOfTable(AppFileHandler.tables_table);
        
        Condition condition = new Condition(DataType.TEXT);
        condition.setColumName("TableName");
        condition.column_ordinal = 0;
        condition.setConditionValue(TableName);
        condition.setOperator("=");

        List<String> columns = Arrays.asList("record_count","root_page");
        List<String> newValues = new ArrayList<>();

        newValues.add(String.valueOf(record_count));
        newValues.add(String.valueOf(root_page_number));

        tablesBinaryFile.update_records_operation(tablesMetaData,condition,columns,newValues);
                                             
      cobaltdbTablesCatalog.close();
  }
  catch(IOException e){
     System.out.println("! Error updating metadata for " + TableName);
  }

  
}

    public List<Integer> get_ordinal_postions(List<String> columns){
				List<Integer> ordinalPostions = new ArrayList<>();
				for(String column :columns)
				{
					ordinalPostions.add(column_names.indexOf(column));
                }
                return ordinalPostions;
    }

    private void LoadColumnData() {
        try {
  
           RandomAccessFile cobaltdbColumnsCatalog = new RandomAccessFile(
            App.getTBLFilePath(AppFileHandler.columns_table), "r");
           int root_page_number = AppFileHandler.GetRootPageNum(cobaltdbColumnsCatalog);
  
           column_data = new ArrayList<>();
           column_name_attributes = new ArrayList<>();
           column_names = new ArrayList<>();
           BPlusTree bPlusOneTree = new BPlusTree(cobaltdbColumnsCatalog, root_page_number,TableName);

           for (Integer pageNo : bPlusOneTree.GetAllLeaves()) {
           
             Page page = new Page(cobaltdbColumnsCatalog, pageNo);
              
              for (TableRecord record : page.GetPageRecords()) {
                  
                 if (record.getAttributes().get(0).field_value.equals(TableName)) {
                    {
                        column_data.add(record);
                       column_names.add(record.getAttributes().get(1).field_value);
                       ColumnInformation colInfo = new ColumnInformation(
                                          TableName  
                                        , DataType.get(record.getAttributes().get(2).field_value)
                                        , record.getAttributes().get(1).field_value
                                        , record.getAttributes().get(6).field_value.equals("YES")
                                        , record.getAttributes().get(4).field_value.equals("YES")
                                        , Short.parseShort(record.getAttributes().get(3).field_value)
                                        );
                                          
                    if(record.getAttributes().get(5).field_value.equals("PRI"))
                          colInfo.set_key_asPrimary();
                        
                     column_name_attributes.add(colInfo);
                    }
                 }
              }
           }
  
           cobaltdbColumnsCatalog.close();
        } catch (Exception e) {
           System.out.println("! Error while getting column data for " + TableName);
        }
  
     }   

}