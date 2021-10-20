package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * List of collection of fields 
     */
    private ArrayList<TDItem> fieldList;

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

        @Override
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
        return fieldList.iterator();
    }

    private static final long serialVersionUID = 1L;

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
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("The type array does not contain any entries.");
        }

        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("The length of the type array is not equal to the length of the field array.");
        }

        this.fieldList = new ArrayList<>();

        for (int i = 0; i < typeAr.length; i++) {
            TDItem field = new TDItem(typeAr[i], fieldAr[i]);
            this.fieldList.add(field);
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
        this.fieldList = new ArrayList<>();

        for (int i = 0; i < typeAr.length; i++) {
            TDItem field = new TDItem(typeAr[i], "");
            this.fieldList.add(field);
        }
    }

    /**
     * Extra constructor to create a TupleDesc from a TDItem ArrayList.
     * @param tdItems
     */
    private TupleDesc(ArrayList<TDItem> tdItems) {
        if (tdItems == null || tdItems.isEmpty()) {
            throw new IllegalArgumentException("The TDItem ArrayList is empty.");
        }

        this.fieldList = new ArrayList<>(tdItems);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.fieldList.size();
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
        if (i < 0 || i > this.numFields() - 1) {
            throw new NoSuchElementException("Invalid field reference.");
        }

        return this.fieldList.get(i).fieldName;
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
        if (i < 0 || i > this.numFields() - 1) {
            throw new NoSuchElementException("Invalid field reference.");
        }

        return this.fieldList.get(i).fieldType;
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
        for (TDItem field : this.fieldList) {
            int indexOfField = this.fieldList.indexOf(field);
            if (field.fieldName != null) {
                if (field.fieldName.equals(name)) {
                    return indexOfField;
                }
            } else {
                if (name == null) {
                    return indexOfField;
                }
            }
        }

        throw new NoSuchElementException("No field with a matching name was found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;

        for (TDItem field : this.fieldList) {
            size += field.fieldType.getLen();
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
        ArrayList<TDItem> combinedItems = new ArrayList<>();

        // Encapsulation is still respected here
        combinedItems.addAll(td1.fieldList);
        combinedItems.addAll(td2.fieldList);

        return new TupleDesc(combinedItems);
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
    @Override
    public boolean equals(Object o) {
        // First, ensure that o is a TupleDesc object
        if (!(o instanceof TupleDesc)) {
            return false;
        }

        // Cast as a TupleDesc object to access specific class methods
        TupleDesc obj = (TupleDesc) o;

        // Check size
        if (this.numFields() != obj.numFields()) {
            return false;
        }

        // Check all n field types
        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getFieldType(i).equals(obj.getFieldType(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("Unimplemented.");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        StringBuilder description = new StringBuilder();

        for (int i = 0; i < this.numFields(); i++) {
            description.append(this.getFieldType(i).toString() + "(" + this.getFieldName(i) + ")" + ", ");
        }

        // Chop off the final/last ", " characters
        return description.substring(0, description.length() - 2);
    }
}
