package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op operator;
    private TupleDesc td;
    private Map<Field, Integer> groupMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException("Only Count!");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.operator = what;
        Type aggFieldType = (operator == Op.MIN || operator == Op.MAX) ? Type.STRING_TYPE : Type.INT_TYPE;
        td = gbfield == NO_GROUPING ? new TupleDesc(new Type[]{aggFieldType})
                : new TupleDesc(new Type[]{gbfieldtype, aggFieldType});
        groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField sf = (StringField) tup.getField(afield);
        String value = sf.getValue();
        Field groupField;
        if (gbfield == NO_GROUPING) {
            groupField = null;
        } else {
            groupField = tup.getField(gbfield);
        }
        if (groupField != null && groupField.getType() != gbfieldtype && gbfieldtype != null) {
            throw new IllegalArgumentException("Wrong Type!");
        }
        if (operator == Op.COUNT) {
            if (!groupMap.containsKey(groupField)) {
                groupMap.put(groupField, 1);
            } else {
                groupMap.put(groupField, groupMap.get(groupField) + 1);
            }
        } else {
            throw new IllegalArgumentException("Op type wrong!");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Field f : groupMap.keySet()) {
            Tuple tuple = new Tuple(this.td);
            if (gbfield == NO_GROUPING) {
                tuple.setField(0, new IntField(groupMap.get(f)));
            } else {
                tuple.setField(0, f);
                tuple.setField(1, new IntField(groupMap.get(f)));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

    @Override
    public TupleDesc aggregatedTupleDesc() {
        return td;
    }

    @Override
    public void clear() {
        groupMap.clear();
    }


}
