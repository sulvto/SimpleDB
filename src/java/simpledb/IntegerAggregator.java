package simpledb;

import java.io.File;
import java.util.*;
import java.util.function.BinaryOperator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final boolean NO_GROUPING;
    private final Type gbfieldtype;
    private final int aggregateField;
    private final Op op;
    private final Map<Field, List<Field>> aggregateFieldMap;
    private final Field DUMMY_FIELD = new IntField(0);

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.NO_GROUPING = gbfield == Aggregator.NO_GROUPING;
        this.gbfieldtype = gbfieldtype;
        this.aggregateField = afield;
        this.op = what;
        aggregateFieldMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbKey = NO_GROUPING ? DUMMY_FIELD : tup.getField(gbfield);
        if (gbKey != null) {
            if (!aggregateFieldMap.containsKey(gbKey)) {
                aggregateFieldMap.put(gbKey, new ArrayList<>());
            }

            aggregateFieldMap.get(gbKey).add(tup.getField(aggregateField));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
            private Iterator<Field> child;
            private final TupleDesc tupleDesc = new TupleDesc(NO_GROUPING ? new Type[]{Type.INT_TYPE} : new Type[]{gbfieldtype, Type.INT_TYPE});

            @Override
            public void open() throws DbException, TransactionAbortedException {
                child = aggregateFieldMap.keySet().iterator();
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

                Tuple result = new Tuple(tupleDesc);
                int aggregateValue;
                Field gbField = child.next();
                List<Field> fields = aggregateFieldMap.get(gbField);
                switch (op) {
                    case AVG:
                        aggregateValue = (int) fields.stream()
                                .map(field -> (IntField) field)
                                .mapToInt(IntField::getValue)
                                .average()
                                .getAsDouble();
                        break;
                    case SUM:
                        aggregateValue = fields.stream()
                                .map(field -> (IntField) field)
                                .mapToInt(IntField::getValue)
                                .sum();
                        break;
                    case COUNT:
                        aggregateValue = fields.size();
                        break;
                    case MIN:
                        aggregateValue = fields
                                .stream()
                                .map(field -> (IntField) field)
                                .mapToInt(IntField::getValue)
                                .min()
                                .getAsInt();
                        break;
                    case MAX:
                        aggregateValue = fields
                                .stream()
                                .map(field -> (IntField) field)
                                .mapToInt(IntField::getValue)
                                .max()
                                .getAsInt();
                        break;
                    default:
                        throw new DbException("unknown Op:" + op);
                }

                if (NO_GROUPING) {
                    result.setField(0, new IntField(aggregateValue));
                } else {
                    result.setField(0, gbField);
                    result.setField(1, new IntField(aggregateValue));
                }
                return result;
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
