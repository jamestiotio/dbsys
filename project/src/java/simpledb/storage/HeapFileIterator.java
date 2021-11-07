package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;

/**
 * HeapFileIterator extends AbstractDbFileIterator and is used to
 * iterate through all tuples in a DbFile.
 */
public class HeapFileIterator extends AbstractDbFileIterator {
    private int pageIndex;
    private final TransactionId tid;
    private final int tableId;
    private final int pageNum;
    private Iterator<Tuple> iterator;

    public HeapFileIterator(int tableId, TransactionId tid, int pageNum) {
        this.tid = tid;
        this.tableId = tableId;
        this.pageNum = pageNum;
        pageIndex = 0;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (iterator == null)
            return null;

        if (iterator.hasNext())
            return iterator.next();

        while (pageIndex < pageNum - 1) {
            pageIndex++;
            open();
            if (iterator == null)
                return null;
            if (iterator.hasNext())
                return iterator.next();
        }

        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        PageId pid = new HeapPageId(tableId, pageIndex);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        iterator = page.iterator();
    }

    @Override
    public void close() {
        super.close();
        iterator = null;
        pageIndex = 0;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }
}
