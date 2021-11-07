package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int aggFieldIdx;
    private int groupByFieldIdx;
    private Aggregator.Op aggOp;
    private Aggregator aggregator;
    private OpIterator aggregateIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of fetchNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.aggFieldIdx = afield;
        this.groupByFieldIdx = gfield;
        this.aggOp = aop;
        this.aggregateIterator = null;
        this.aggregator = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return this.groupByFieldIdx;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if (this.groupByFieldIdx == Aggregator.NO_GROUPING) {
            return null;
        } else {
            return child.getTupleDesc().getFieldName(this.groupByFieldIdx);
        }
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.aggFieldIdx;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return this.child.getTupleDesc().getFieldName(this.aggFieldIdx);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.aggOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        if (this.aggregateIterator == null) {
            this.aggregator = this.createAggregator(this.child.getTupleDesc());
            this.populateAggregator();
            this.aggregateIterator = this.aggregator.iterator();
        }
        this.aggregateIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.aggregateIterator.hasNext()) {
            return this.aggregateIterator.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.aggregateIterator = this.aggregator.iterator();
        this.aggregateIterator.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        String aggFieldName = Aggregate.aggName(this.aggOp) + " (" +
                this.child.getTupleDesc().getFieldName(this.aggFieldIdx) + ")";

        if (this.groupByFieldIdx == Aggregator.NO_GROUPING) {
            return new TupleDesc(
                    new Type[]{this.child.getTupleDesc().getFieldType(this.aggFieldIdx)},
                    new String[]{aggFieldName}
            );
        } else {
            return new TupleDesc(
                    new Type[]{
                            this.child.getTupleDesc().getFieldType(this.groupByFieldIdx),
                            this.child.getTupleDesc().getFieldType(this.aggFieldIdx)
                    },
                    new String[]{
                            this.child.getTupleDesc().getFieldName(this.groupByFieldIdx),
                            aggFieldName
                    }
            );
        }
    }

    @Override
    public void close() {
        super.close();
        this.child.close();
        this.aggregateIterator.close();
        this.aggregateIterator = null;
        this.aggregator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
            case MIN:
                return "min";
            case MAX:
                return "max";
            case AVG:
                return "avg";
            case SUM:
                return "sum";
            case COUNT:
                return "count";
            default:
                return "";
        }
    }

    /**
     * Creates an aggregator based on the Type of the aggregate field
     * @param td The tuple descriptor of the child iterator
     * @return an Aggregator of a specific type (e.g. IntAggregator)
     */
    private Aggregator createAggregator(TupleDesc td) throws DbException {
        Type groupByFieldType = null;
        if (this.groupByFieldIdx != Aggregator.NO_GROUPING) {
            groupByFieldType = td.getFieldType(this.groupByFieldIdx);
        }

        if (td.getFieldType(this.aggFieldIdx) == Type.INT_TYPE) {
            return new IntegerAggregator(this.groupByFieldIdx, groupByFieldType, this.aggFieldIdx, this.aggOp);
        } else if (td.getFieldType(this.aggFieldIdx) == Type.STRING_TYPE) {
            return new StringAggregator(this.groupByFieldIdx, groupByFieldType, this.aggFieldIdx, this.aggOp);
        } else {
            throw new DbException("This type of iterator is not supported");
        }
    }

    /**
     * Merge all tuples into our aggregator and return an iterator over the group aggregate results
     */
    private void populateAggregator() throws DbException, TransactionAbortedException {
        this.child.open();

        while (this.child.hasNext()) {
            Tuple nextTuple = this.child.next();
            this.aggregator.mergeTupleIntoGroup(nextTuple);
        }
    }
}
