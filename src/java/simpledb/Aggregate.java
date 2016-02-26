package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    // schema of the tuples fed by DbIterator
    private TupleDesc td;
    
    // to construct an IntAggregator or StringAggregator
    private Aggregator aggregator;
    // Iterator for the aggregator
    private DbIterator aIterator;
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	// some code goes here
    	this.child = child;
    	td = child.getTupleDesc();
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    	
    	// construct aggregator
    	Type gbfieldtype; // the type of the group by field
    	if(gfield == Aggregator.NO_GROUPING)
    		gbfieldtype = null; // null if there is no grouping
    	else
    		gbfieldtype = td.getFieldType(gfield);
    	
    	Type afieldtype = td.getFieldType(afield);
    	if(afieldtype == Type.INT_TYPE)
    		// construct an IntAggregator
    		aggregator = new IntegerAggregator(gfield, gbfieldtype, afield, aop);
    	else 
    		// construct a StringAggregator
    		aggregator = new StringAggregator(gfield, gbfieldtype, afield, aop);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	// some code goes here
    	return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	// some code goes here
    	if(gfield == Aggregator.NO_GROUPING)
    		return null;
    	else
    		return td.getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	// some code goes here
    	return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	// some code goes here
    	return td.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	// some code goes here
    	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	// some code goes here
    	child.open();
    	while(child.hasNext())
    		aggregator.mergeTupleIntoGroup(child.next());
    	
    	aIterator = aggregator.iterator();
    	aIterator.open();
    	super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	// some code goes here
    	if(aIterator.hasNext())
    		return aIterator.next();
    	else // no more tuples
    		return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	// some code goes here
    	child.rewind();
    	aIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	// some code goes here
    	Type[] typeAr;	// array of types of fields
    	String[] fieldAr; // array of names of fields
    	
    	// if there is no group by field
    	if(afield == Aggregator.NO_GROUPING) {
    		// TupleDesc has one field: the aggregate column
    		typeAr = new Type[1];
    		fieldAr = new String[1];
    		
    		typeAr[0] = td.getFieldType(afield);
    		// informative aggregate column name
    		// aggName(aop) (child_td.getFieldName(afield))
    		fieldAr[0] = aop.toString() + " (" + td.getFieldName(afield) + ")";
    		
    		return new TupleDesc(typeAr, fieldAr);
    	}
    	// if there is a group by field
    	else {
    		// TupleDesc has two fields: 
    		// the first field will be the group by field, 
    		// the second will be the aggregate value column
    		typeAr = new Type[2];
    		fieldAr = new String[2];
    		
    		typeAr[0] = td.getFieldType(gfield);
    		fieldAr[0] = td.getFieldName(gfield);
    		
    		typeAr[1] = td.getFieldType(afield);
    		fieldAr[1] = aop.toString() + " (" + td.getFieldName(afield) + ")";
    		
    		return new TupleDesc(typeAr, fieldAr);
    	}
    }

    public void close() {
    	// some code goes here
    	child.close();
    	aggregator.iterator().close();
    	super.close();
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
