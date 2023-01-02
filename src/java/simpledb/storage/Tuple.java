package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc td;// the description of tuple

    private RecordId recordId; // the location of this tuple on disk

    private Field[] fields; // array of field, contain data for each field
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        try{
            if (td == null){
                throw new Exception("TupleDesc is null");
            }

            if (td.numFields() < 1){
                throw new Exception(("TupleDesc has one field at least"));
            }

            this.td = td;
            this.fields = new Field[td.numFields()];

        }catch(Exception e){
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        try{
            if (0 <= i && i < fields.length){
                fields[i] = f;
            }else{
                throw new Exception("the index is out of range");
            }

        }catch(Exception e){
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        try{
            if (0 <= i && i < fields.length){
                return fields[i];
            }else{
                throw new Exception("the index is out of range");
            }

        }catch(Exception e){
            System.out.println(e.getMessage());
        }
        return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            builder.append(fields[i].toString());
            if (i != fields.length - 1) {
                builder.append("\t");
            }
        }
        return builder.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        class innerIter implements Iterator<Field>{
            private int index = 0;
            @Override
            public boolean hasNext() {
                return index < fields.length;
            }

            @Override
            public Field next() {
                Field field = fields[index];
                index ++;
                return field;
            }
        }
        return new innerIter();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.td = td;
    }
}
