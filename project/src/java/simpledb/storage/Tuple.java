package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
    /**
     * A Tuple contains fields, a TupleDesc object, and a recordID
     */ 
    private Field[] tupleFields;
    private TupleDesc tupleSchema;
    private RecordId recordId;
    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.tupleFields = new Field[td.numFields()];
        this.tupleSchema = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleSchema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * @return the number of fields in this Tuple
     */
    public int getNumFields() {
        return this.tupleFields.length;
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
        if (!this.isValidIndex(i)) {
            throw new IllegalArgumentException("Invalid field index value.");
        }

        this.tupleFields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if (!this.isValidIndex(i)) {
            throw new IllegalArgumentException("Invalid field index value.");
        }

        return this.tupleFields[i];
    }

    private boolean isValidIndex(int index) {
        return index >= 0 && index < this.tupleFields.length;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    @Override
    public String toString() {
        StringBuilder description = new StringBuilder();

        for (int i = 0; i < this.getNumFields(); i++) {
            if (this.tupleFields[i] == null) {
                description.append("null\t");
            } else {
                description.append(this.tupleFields[i].toString() + "\t");
            }
        }

        // Chop off the final whitespace character
        return description.substring(0, description.length() - 1);
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return Arrays.asList(this.tupleFields).iterator();
    }

    /**
     * Reset the TupleDesc of this tuple (only affecting the TupleDesc).
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.tupleSchema = td;
    }
}
