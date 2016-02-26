package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    
    private boolean grouping = true; // false if Aggregator.NO_GROUPING
    String gbfieldName = "";
    String tfieldName = "";
    
    // each tuple in the result is a pair of the form (groupValue, aggrageteValue)
    private HashMap<Field, Integer> groupbyResult;

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
    	
    	if(gbfield == Aggregator.NO_GROUPING) {
    		grouping = false;	// there is no grouping
    		this.gbfieldtype = null;
    	} else {
    		this.gbfieldtype = gbfieldtype;
    	}
    
    	this.afield = afield;
    	this.what = what;
    	
    	groupbyResult = new HashMap<Field, Integer>();
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	// same as in IntegerAggregator.java
    	// but only has COUNT aggregation
    	Field groupbyField;
    	int aggregateValue;
    	tfieldName = tup.getTupleDesc().getFieldName(afield);
    	
    	if(!grouping)
    		groupbyField = new IntField(Aggregator.NO_GROUPING);
    	else {
    		groupbyField = tup.getField(gbfield);
    		gbfieldName = tup.getTupleDesc().getFieldName(gbfield);
    	}
    	
    	if(!groupbyResult.containsKey(groupbyField))
    		groupbyResult.put(groupbyField, 0);
    	
    	aggregateValue = groupbyResult.get(groupbyField);
    	
    	aggregateValue++;
    	groupbyResult.put(groupbyField, aggregateValue);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for proj2");
    	// same as in IntegerAggregator.java
    	// but only has COUNT aggregation
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	Type[] typeAr;
    	String[] fieldAr;
    	TupleDesc td;
    	
    	if(grouping) {
    		typeAr = new Type[2];
    		fieldAr = new String[2];
    		// groupValue
    		typeAr[0] = gbfieldtype;
    		fieldAr[0] = gbfieldName;
    		// aggregateValue
    		typeAr[1] = Type.INT_TYPE;
    		fieldAr[1] = tfieldName;
    	}
    	// or a single(aggregateValue) if no grouping
    	else {
    		typeAr = new Type[1];
    		fieldAr = new String[1];
    		// aggregateValue
    		typeAr[0] = Type.INT_TYPE;
    		fieldAr[0] = tfieldName;
    	}
    	td = new TupleDesc(typeAr, fieldAr);
    	
    	for(Field groupbyField : groupbyResult.keySet()) {
    		int aggregateValue = groupbyResult.get(groupbyField);
    		
    		Tuple tuple = new Tuple(td);
    	
    		if(grouping) {
    			tuple.setField(0, groupbyField);
    			tuple.setField(1, new IntField(aggregateValue));
    		} else {
    			tuple.setField(0, new IntField(aggregateValue));
    		}
    		
    		tuples.add(tuple);
    	}
    	
    	return new TupleIterator(td, tuples);

    }

}
