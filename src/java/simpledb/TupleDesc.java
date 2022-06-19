package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
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
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    private List<TDItem> tdItemList;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        tdItemList = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            tdItemList.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tdItemList = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            tdItemList.add(new TDItem(typeAr[i], ""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException();
        }
        return tdItemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException();
        }
        return tdItemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < tdItemList.size(); i++) {
            if (tdItemList.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int count = 0;
        for (TDItem tdItem : tdItemList) {
            count += tdItem.fieldType.getLen();
        }
        return count;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] new_types = new Type[td1.numFields() + td2.numFields()];
        String[] new_fieldNames = new String[td1.numFields() + td2.numFields()];
        int num = 0;
        for (int i = 0; i < td1.numFields(); i++) {
            new_types[num] = td1.getFieldType(i);
            new_fieldNames[num] = td1.getFieldName(i);
            num++;
        }
        for (int i = 0; i < td2.numFields(); i++) {
            new_types[num] = td2.getFieldType(i);
            new_fieldNames[num] = td2.getFieldName(i);
            num++;
        }
        return new TupleDesc(new_types, new_fieldNames);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof TupleDesc) || o.equals(null))
            return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        if (tupleDesc.numFields() != this.numFields()) {
            return false;
        }
        for (int i = 0; i < this.numFields(); i++) {
            if (!(this.getFieldName(i).equals(tupleDesc.getFieldName(i))) ||
                    !(this.getFieldType(i).equals(tupleDesc.getFieldType(i)))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tdItemList);
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
        String str = null;
        for (int i = 0; i < tdItemList.size() - 1; i++) {
            str += tdItemList.get(i).fieldType + "(" + tdItemList.get(i).fieldName + "), ";
        }
        str += tdItemList.get(tdItemList.size() - 1).fieldType + "(" + tdItemList.get(tdItemList.size()) + ")";
        return str;
    }
}
