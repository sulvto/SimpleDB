package simpledb;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final boolean NO_GROUPING;
    private final Type gbfieldtype;
    private final int afield;
    private final Field DUMMY_FIELD = new StringField("",0);
    private final Map<Field, AtomicInteger> count;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("only supports COUNT");
        }

        this.gbfield = gbfield;
        this.NO_GROUPING = (gbfield == Aggregator.NO_GROUPING);
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        count = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbKey = NO_GROUPING ? DUMMY_FIELD : tup.getField(gbfield);
        if (gbKey != null) {
            if (!count.containsKey(gbKey)) {
                count.put(gbKey, new AtomicInteger(1));
            }
            count.get(gbKey).incrementAndGet();
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
            private Iterator<Field> child;
            private TupleDesc tupleDesc = new TupleDesc(NO_GROUPING ? new Type[]{Type.INT_TYPE} : new Type[]{gbfieldtype, Type.INT_TYPE});

            @Override
            public void open() throws DbException, TransactionAbortedException {
                child = count.keySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return child != null && child.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                Field gbField = child.next();
                Tuple tuple = new Tuple(tupleDesc);
                IntField countField = new IntField(count.get(gbField).get());
                if (NO_GROUPING) {
                    tuple.setField(0, countField);
                } else {
                    tuple.setField(0, gbField);
                    tuple.setField(1, countField);
                }
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void close() {
                child = null;
            }
        };
    }

}
