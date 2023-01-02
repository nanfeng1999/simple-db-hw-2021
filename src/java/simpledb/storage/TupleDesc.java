package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        class innerIter implements Iterator<TDItem>{
            private int index = 0;
            @Override
            public boolean hasNext() {
                return index < list.size();
            }

            @Override
            public TDItem next() {
                TDItem item = list.get(index);
                index++;
                return item;
            }
        }

        return new innerIter();
    }

    private static final long serialVersionUID = 1L;

    private ArrayList<TDItem> list;
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        try{
            if(typeAr == null){
                throw new Exception("TypeAr can not be empty.");
            }

            if(typeAr.length == 0){
                throw  new Exception("TypeAr must contain at least one entry");
            }

            list = new ArrayList<>();

            for (int i = 0; i < typeAr.length; i++) {
                if (i < fieldAr.length){
                    list.add(new TDItem(typeAr[i],fieldAr[i]));
                }else{
                    list.add(new TDItem(typeAr[i],null));
                }
            }

        }catch(Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        try{
            if(typeAr == null){
                throw new Exception("TypeAr can not be empty.");
            }

            if(typeAr.length == 0){
                throw  new Exception("TypeAr must contain at least one entry");
            }

            list = new ArrayList<>();

            for (Type type : typeAr) {
                // todo: no field name is better to set null or ""?
                list.add(new TDItem(type, ""));
            }

        }catch(Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return list.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if ( i >= 0 && i < list.size()){
            return list.get(i).fieldName;
        }else{
            throw new NoSuchElementException("Index out of range");
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if ( i >= 0 && i < list.size()){
            return list.get(i).fieldType;
        }else{
            throw new NoSuchElementException("Index out of range");
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException(String.format("Not found name = %s",name));
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem tdItem : list) {
            size += tdItem.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] typeAr1 = new Type[td1.numFields()];
        String[] fieldAr1 = new String[td1.numFields()];
        for (int i = 0; i < td1.numFields(); i++) {
            typeAr1[i] = td1.getFieldType(i);
            fieldAr1[i] = td1.getFieldName(i);
        }

        Type[] typeAr2 = new Type[td2.numFields()];
        String[] fieldAr2 = new String[td2.numFields()];
        for (int i = 0; i < td2.numFields(); i++) {
            typeAr2[i] = td2.getFieldType(i);
            fieldAr2[i] = td2.getFieldName(i);
        }

        Type[] newTypeAr = Arrays.copyOf(typeAr1, typeAr1.length + typeAr2.length);
        System.arraycopy(typeAr2, 0, newTypeAr, typeAr1.length, typeAr2.length);

        String[] newFieldAr = Arrays.copyOf(fieldAr1, fieldAr1.length + fieldAr2.length);
        System.arraycopy(fieldAr2, 0, newFieldAr, fieldAr1.length, fieldAr2.length);

        TupleDesc td = new TupleDesc(newTypeAr,newFieldAr);
        return td;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        // todo: it is unnecessary, because null instanceof tupleDesc ,return false
        if ( o == null){
            return false;
        }

        if (o instanceof TupleDesc) {
            TupleDesc tdCompared = (TupleDesc) o;
            if (numFields() != tdCompared.numFields()){
                return false;
            }

            for (int i = 0; i < numFields(); i++) {
                if (this.getFieldType(i) != tdCompared.getFieldType(i)){
                    return false;
                }
            }

            return true;
        }else{
            return false;
        }

    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder printStr = new StringBuilder();
        Iterator<TDItem> iter = iterator();
        while (iter.hasNext()) {
            TDItem next =  iter.next();
            printStr.append(",").append(next.toString());
        }
        return printStr.toString();
    }
}
