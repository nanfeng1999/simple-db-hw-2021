package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private int afield;
    private final Op what;
    private IntegerAggHandler handler;

    abstract class IntegerAggHandler{
        HashMap<Field,Integer> map;
        IntegerAggHandler(){
            map = new HashMap<>();
        }

        Map<Field,Integer> getAgg(){
            return map;
        }

        abstract void handler(Field gbField, IntField aField);
    }

    class MinHandler extends IntegerAggHandler{
        @Override
        void handler(Field gbField, IntField aField) {
            map.merge(gbField,aField.getValue(), Math::min);
        }
    }

    class MaxHandler extends IntegerAggHandler{
        @Override
        void handler(Field gbField, IntField aField) {
            map.merge(gbField,aField.getValue(), Math::max);
        }
    }

    class SumHandler extends IntegerAggHandler{
        @Override
        void handler(Field gbField, IntField aField) {
            map.merge(gbField,aField.getValue(), Integer::sum);
        }
    }

    class AvgHandler extends IntegerAggHandler{
        HashMap<Field,Integer> sum = new HashMap<>();
        HashMap<Field,Integer> count = new HashMap<>();
        @Override
        void handler(Field gbField, IntField aField) {
            sum.merge(gbField,aField.getValue(), Integer::sum);
            count.merge(gbField,1, (old, _new) -> old + 1);
            map.put(gbField,sum.get(gbField)/count.get(gbField));
        }
    }

    class CountHandler extends IntegerAggHandler{
        @Override
        void handler(Field gbField, IntField aField) {
            map.merge(gbField, 1, (old, _new) -> old + 1);
        }
    }

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
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        switch(what){
            case MIN:
                this.handler = new MinHandler();
                break;
            case MAX:
                this.handler = new MaxHandler();
                break;
            case SUM:
                this.handler = new SumHandler();
                break;
            case AVG:
                this.handler = new AvgHandler();
                break;
            case COUNT:
                this.handler = new CountHandler();
                break;
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = null;
        if (gbfield != NO_GROUPING){
            gbField = tup.getField(gbfield);
        }

        handler.handler(gbField,(IntField) tup.getField(afield));
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
        // some code goes here
        TupleDesc td;
        ArrayList<Tuple> list = new ArrayList<>();

        if (gbfield == NO_GROUPING){
            td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"aggField"});
            Tuple t = new Tuple(td);
            t.setField(0,new IntField(handler.getAgg().get(null)));
            list.add(t);
        }else{
            td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},new String[]{"groupField","aggField"});
            for (Map.Entry<Field, Integer> entry : handler.getAgg().entrySet()) {
                Tuple t = new Tuple(td);
                t.setField(0,entry.getKey());
                t.setField(1,new IntField(entry.getValue()));
                list.add(t);
            }
        }

        return new TupleIterator(td,list);

    }

}
