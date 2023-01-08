package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private int afield;
    private final Op what;
    private StringAggHandler handler;

    abstract class StringAggHandler {
        Map<Field,Integer> map;
        public StringAggHandler(){
            map = new HashMap<>();
        }

        public Map<Field,Integer> getAgg(){
            return map;
        }

        public abstract void handler(Field f,StringField sField);
    }

    class CountHandler extends StringAggHandler{
        @Override
        public void handler(Field gField,StringField aField) {
            map.merge(gField, 1, (old, _new) -> old + 1);
        }
    }

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;

        if (what != Op.COUNT){
            throw new IllegalArgumentException("Unsupported operation");
        }
        this.what = what;
        this.handler = new CountHandler();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gField = null;
        if (gbfield != NO_GROUPING){
            gField = tup.getField(gbfield);
        }

        StringField aField = (StringField) tup.getField(afield);
        this.handler.handler(gField,aField);
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
        // some code goes here
        TupleDesc td;
        ArrayList<Tuple> list = new ArrayList<>();
        Map<Field,Integer> map = this.handler.getAgg();

        if (gbfield == NO_GROUPING){
            td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"aggField"});
            Tuple t = new Tuple(td);
            t.setField(0,new IntField(map.get(null)));
            list.add(t);
        }else{
            td = new TupleDesc(new Type[]{this.gbfieldtype,Type.INT_TYPE},new String[]{"groupField","aggField"});
            for (Map.Entry<Field, Integer> entry : map.entrySet()) {
                Tuple t = new Tuple(td);
                t.setField(0,entry.getKey());
                t.setField(1,new IntField(entry.getValue()));
                list.add(t);
            }
        }

        return new TupleIterator(td,list);
    }

}
