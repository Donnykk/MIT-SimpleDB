package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final TupleDesc td;
    private final Op operator;
    private Map<Field, Integer> groupMap;
    private Map<Field, List<Integer>> avgMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.operator = what;
        td = gbfield == NO_GROUPING ? new TupleDesc(new Type[]{Type.INT_TYPE})
                : new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        this.groupMap = new HashMap<>();
        this.avgMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField f = (IntField) tup.getField(afield);
        int value = f.getValue();
        Field groupField;
        if (gbfield == NO_GROUPING) {
            groupField = null;
        } else {
            groupField = tup.getField(gbfield);
        }
        if (groupField != null && groupField.getType() != gbfieldtype && gbfieldtype != null) {
            throw new IllegalArgumentException("Wrong Type!");
        }
        switch (this.operator) {
            case SUM:
                if (!groupMap.containsKey(groupField)) {
                    groupMap.put(groupField, value);
                } else {
                    groupMap.put(groupField, groupMap.get(groupField) + value);
                }
                break;
            case MAX:
                if (!groupMap.containsKey(groupField)) {
                    groupMap.put(groupField, value);
                } else {
                    groupMap.put(groupField, Math.max(groupMap.get(groupField), value));
                }
                break;
            case MIN:
                if (!groupMap.containsKey(groupField)) {
                    groupMap.put(groupField, value);
                } else {
                    groupMap.put(groupField, Math.min(groupMap.get(groupField), value));
                }
                break;
            case COUNT:
                if (!groupMap.containsKey(groupField)) {
                    groupMap.put(groupField, 1);
                } else {
                    groupMap.put(groupField, groupMap.get(groupField) + 1);
                }
                break;
            case AVG:
                if (!avgMap.containsKey(groupField)) {
                    List<Integer> list = new ArrayList<>();
                    list.add(value);
                    avgMap.put(groupField, list);
                } else {
                    avgMap.get(groupField).add(value);
                }
                break;
            default:
                throw new IllegalArgumentException("Wrong Operator!");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> list = new ArrayList<>();
        if (operator == Op.AVG) {
            for (Field f : avgMap.keySet()) {
                List<Integer> l = avgMap.get(f);
                int sum = 0, average;
                for (int i : l) {
                    sum += i;
                }
                average = sum / l.size();
                Tuple t = new Tuple(td);
                if (gbfield != NO_GROUPING) {
                    t.setField(0, f);
                    t.setField(1, new IntField(average));
                } else {
                    t.setField(0, new IntField(average));
                }
                list.add(t);
            }
        } else {
            for (Field f : groupMap.keySet()) {
                Tuple t = new Tuple(td);
                if (gbfield != NO_GROUPING) {
                    t.setField(0, f);
                    t.setField(1, new IntField(groupMap.get(f)));
                } else {
                    t.setField(0, new IntField(groupMap.get(f)));
                }
                list.add(t);
            }
        }
        return new TupleIterator(td, list);
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
