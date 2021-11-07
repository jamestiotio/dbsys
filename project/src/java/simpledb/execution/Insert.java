package simpledb.execution;

import java.io.IOException;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {
    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private boolean fetched = false;
    private TupleDesc td;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableId = tableId;

        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});

        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(this.tableId))) {
            throw new DbException("Child tuple descriptor does not match that of the table.");
        }
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        super.open();
        this.fetched = false;
    }

    @Override
    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.fetched) return null;

        int insertCount = 0;
        this.fetched = true;

        while (this.child.hasNext()) {
            Tuple tuple = this.child.next();
            insertCount++;
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, tuple);
            } catch (IOException e) {
                throw new DbException("Insertion failed.");
            }
        }

        Tuple insertResults = new Tuple(this.td);
        insertResults.setField(0, new IntField(insertCount));
        return insertResults;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
