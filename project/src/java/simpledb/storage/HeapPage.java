package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;
import java.time.Instant;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {
    private final HeapPageId pid;
    private final TupleDesc td;
    private final byte[] header;
    private final Tuple[] tuples;
    private final int numSlots;
    private TransactionId dirtyTid;
    private long lastAccessTimestamp;

    private byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        this.dirtyTid = null;
        this.updateLastAccessTimestamp();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // Allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try {
            // Allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        int tupleSize = this.td.getSize();    
        int bufferPoolPageSize = Database.getBufferPool().getPageSize();
        return (int) Math.floor((bufferPoolPageSize * 8) / (tupleSize * 8 + 1));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        return (int) Math.ceil(this.getNumTuples() / 8.0);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            // Should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // If associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("Error reading empty tuple.");
                }
            }
            return null;
        }

        // Read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("Parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // Create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // This really shouldn't happen
                e.printStackTrace();
            }
        }

        // Create the tuples
        for (int i=0; i<tuples.length; i++) {

            // Empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }

            // Non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // Padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); // - numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; // All 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        RecordId rid = t.getRecordId();

        if (rid == null) throw new DbException("Tuple has already been deleted.");
        if (!rid.getPageId().equals(this.pid)) throw new DbException("Tuple does not exist on this page.");
        if (!this.isSlotUsed(rid.getTupleNumber())) throw new DbException("Tuple slot is already empty.");

        this.tuples[rid.getTupleNumber()] = null;
        this.markSlotUsed(rid.getTupleNumber(), false);
    }

    /**
     * Adds the specified tuple to the page; the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        if (this.getNumEmptySlots() == 0) throw new DbException("The page is full.");
        if (!t.getTupleDesc().equals(this.td)) {
            throw new DbException("The tuple's descriptor does match the page's tuple descriptor.");
        }

        // Find an empty slot
        int emptySlotIdx = -1;
        for (int i = 0; i < this.numSlots; i++) {
            if (!this.isSlotUsed(i)) {
                emptySlotIdx = i;
                break;
            }
        }
        this.markSlotUsed(emptySlotIdx, true);
        t.setRecordId(new RecordId(this.pid, emptySlotIdx));
        this.tuples[emptySlotIdx] = t;
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        if (dirty) {
            this.dirtyTid = tid;
        } else {
            this.dirtyTid = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return this.dirtyTid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int emptySlots = 0;
        for (int i = 0;i < numSlots; i++) {
            if (!(this.isSlotUsed(i))) {
                emptySlots++;
            }
        }
        return emptySlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        return ((header[i / 8] >> (i % 8)) & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        if (isSlotUsed(i) != value) {
            if (value) {
                header[i / 8] |= (1 << (i % 8));
            } else {
                header[i / 8] ^= (1 << (i % 8));
            }
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        ArrayList<Tuple> tuplesInUse = new ArrayList<>();
        for (int i = 0; i < this.getNumTuples(); i++) {
            if (this.isSlotUsed(i)) {
                tuplesInUse.add(this.tuples[i]);
            }
        }
        return tuplesInUse.iterator();
    }

    /**
     * Set the last page access timestamp to the current time.
     * Used for the LRU Buffer Pool replacement policy.
     */
    public void updateLastAccessTimestamp() {
        this.lastAccessTimestamp = Instant.now().toEpochMilli();
    }

    /**
     * Get the last access timestamp of the current page.
     * Used for the LRU Buffer Pool replacement policy.
     */
    public long getLastAccessTimestamp() {
        return this.lastAccessTimestamp;
    }
}
