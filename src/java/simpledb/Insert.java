package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator child;
    private int tableid;
    
    private boolean flag = false;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	this.t = t;
    	this.child = child;
    	this.tableid = tableid;
    }
    
    // return a single tuple with one integer field containing the count
    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] typeAr = new Type[1];
        String[] fieldAr = new String[1];
    	typeAr[0] = Type.INT_TYPE;
    	fieldAr[0] = "Count";
    	return new TupleDesc(typeAr, fieldAr);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        Tuple tuple = new Tuple(getTupleDesc()); // tuple to return
        int count = 0; // number of inserted records
        
        if(flag) // return null if called more than once
        	return null;
        
        flag = true;
        while(child.hasNext()) {
        	// get the next tuple
        	Tuple next = child.next();
        	// insert passed through BufferPool
        	try {
				Database.getBufferPool().insertTuple(t, tableid, next);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	// increase the number of inserted records
        	count++;
        }
        
        tuple.setField(0, new IntField(count));
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	child = children[0];
    }
}
