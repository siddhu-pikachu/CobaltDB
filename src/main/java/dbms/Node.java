package dbms;

import java.io.IOException;
import java.io.RandomAccessFile;

public interface Node {
    boolean isLeaf();
    byte getPageType();
    int getPageNumber();
}

class LeafNode implements Node {
    private final int pageNum;
    private final RandomAccessFile file;
    private Integer nextLeafPageNum;
    private final Page page;

    public LeafNode(int pageNum, RandomAccessFile file) throws IOException {
        this.pageNum = pageNum;
        this.file = file;
        this.page = new Page(file, pageNum, getPageType());
        this.nextLeafPageNum = null;
        this.page.initialize();
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public byte getPageType() {
        return 0x0d;
    }

    @Override
    public int getPageNumber() {
        return pageNum;
    }

    public Page getPage() {
        return page;
    }

    public Integer getNextLeafPageNum() {
        return nextLeafPageNum;
    }

    public void setNextLeafPageNum(Integer nextLeafPageNum) {
        this.nextLeafPageNum = nextLeafPageNum;
    }
}

class InternalNode implements Node {
    private final int pageNum;
    private final RandomAccessFile file;
    private final int[] keys;
    private final Node[] children;
    private int numKeys;

    public InternalNode(int pageNum, RandomAccessFile file) {
        this.pageNum = pageNum;
        this.file = file;
        this.keys = new int[4];        // Default order of 4
        this.children = new Node[5];    // keys.length + 1
        this.numKeys = 0;
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public byte getPageType() {
        return 0x05;
    }

    @Override
    public int getPageNumber() {
        return pageNum;
    }

    public int[] getKeys() {
        return keys;
    }

    public Node[] getChildren() {
        return children;
    }

    public int getNumKeys() {
        return numKeys;
    }

    public void setNumKeys(int numKeys) {
        this.numKeys = numKeys;
    }
}