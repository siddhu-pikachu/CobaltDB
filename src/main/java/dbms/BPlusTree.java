package dbms;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;

public class BPlusTree {

    RandomAccessFile binFile;
    int root_page_number;
    String TableName;

    public BPlusTree(RandomAccessFile file, int root_page_number, String TableName) {
        this.binFile = file;
        this.root_page_number = root_page_number;
        this.TableName = TableName;
    }

    private int binarySearch(List<InternalTableRecord> values, int searchValue, int start, int end) {

        if(end - start <= 2)
        {
            int i =start;
            for(i=start;i <end;i++){
                if(values.get(i).row_id < searchValue)
                    continue;
                else
                    break;
            }
            return i;
        }
        else{
            
                int mid = (end - start) / 2 + start;
                if (values.get(mid).row_id == searchValue)
                    return mid;

                if (values.get(mid).row_id < searchValue)
                    return binarySearch(values, searchValue, mid + 1, end);
                else
                    return binarySearch(values, searchValue, start, mid - 1);
            
        }

    }

    // This method does a traversal on the B+ tree and returns the leaf pages in order
    public List<Integer> GetAllLeaves() throws IOException {

        List<Integer> leaf_pages = new ArrayList<>();
        binFile.seek(root_page_number * AppFileHandler.page_size);
        // if root is leaf page read directly return one one, no traversal required
        PageType root_page_type = PageType.get(binFile.readByte());
        if (root_page_type == PageType.LEAF) {
            if (!leaf_pages.contains(root_page_number))
                leaf_pages.add(root_page_number);
        } else {
            addition_ofLeaves(root_page_number, leaf_pages);
        }

        return leaf_pages;

    }

    private void addition_ofLeaves(int interiorPageNum, List<Integer> leaf_pages) throws IOException {
        Page interior_page = new Page(binFile, interiorPageNum);
        for (InternalTableRecord leftPage : interior_page.LeftChild) {
            if (Page.GetPageType(binFile, leftPage.LeftChildPgnum) == PageType.LEAF) {
                if (!leaf_pages.contains(leftPage.LeftChildPgnum))
                    leaf_pages.add(leftPage.LeftChildPgnum);
            } else {
                addition_ofLeaves(leftPage.LeftChildPgnum, leaf_pages);
            }
        }

        if (Page.GetPageType(binFile, interior_page.RightPage) == PageType.LEAF) {
            if (!leaf_pages.contains(interior_page.RightPage))
                leaf_pages.add(interior_page.RightPage);
        } else {
            addition_ofLeaves(interior_page.RightPage, leaf_pages);
        }

    }

    public List<Integer> GetAllLeaves(Condition condition) throws IOException {

        if (condition == null || condition.getOperation() == OperandType.NOTEQUAL
                || !(new File(App.getNDXFilePath(TableName, condition.column_name)).exists())) {
            return GetAllLeaves();
        } else {

            RandomAccessFile index_file = new RandomAccessFile(
                    App.getNDXFilePath(TableName, condition.column_name), "r");
            BTree bTree = new BTree(index_file);
            List<Integer> row_ids = bTree.get_row_ids(condition);
            Set<Integer> hash_Set = new HashSet<>();
           
            for (int row_id : row_ids) {
                hash_Set.add(get_page_num(row_id, new Page(binFile, root_page_number)));
            }
            System.out.print(" number of rows : " + row_ids.size() + " ---> ");
            for (int rowId : row_ids) {
                System.out.print(" " + rowId + " ");
            }

            System.out.println();
            System.out.println(" leaves: " + hash_Set);
            System.out.println();
            index_file.close();
            return Arrays.asList(hash_Set.toArray(new Integer[hash_Set.size()]));
        }

    }

    public int get_page_num(int rowId, Page page) {
        if (page.pg_type == PageType.LEAF)
            return page.pgnum;

        int index = binarySearch(page.LeftChild, rowId, 0, page.CellCnt - 1);

        if (rowId < page.LeftChild.get(index).row_id) {
            return get_page_num(rowId, new Page(binFile, page.LeftChild.get(index).LeftChildPgnum));
        } else {
        if( index+1 < page.LeftChild.size())
            return get_page_num(rowId, new Page(binFile, page.LeftChild.get(index+1).LeftChildPgnum));
        else
           return get_page_num(rowId, new Page(binFile, page.RightPage));
        }
    }

    public static int get_pagenum_for_insertion(RandomAccessFile file, int root_page_number) {
        Page rootPage = new Page(file, root_page_number);
        if (rootPage.pg_type != PageType.LEAF && rootPage.pg_type != PageType.LEAFINDEX)
            return get_pagenum_for_insertion(file, rootPage.RightPage);
        else
            return root_page_number;

    }

}