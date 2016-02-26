package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    int tableid;
    int iocost;
    int tupleNum;
    HeapFile file;
    Object[] histogram;
    HashMap<Integer, Integer> min;
    HashMap<Integer, Integer> max;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
    	this.tableid = tableid;
    	this.iocost = ioCostPerPage;
    	file = (HeapFile)Database.getCatalog().getDbFile(tableid);
    	Transaction transaction = new Transaction();
    	transaction.start();
    	DbFileIterator it = file.iterator(transaction.getId());
    	max = new HashMap<Integer, Integer>();
    	min = new HashMap<Integer, Integer>();

    	try{
    		it.open();

    		//step one: to get the stats of a table
    		it.rewind();
    		while(it.hasNext()){
    			Tuple t = it.next();
    			TupleDesc td = t.getTupleDesc();
    			int n = td.numFields();
    			tupleNum++;
    			for(int i=0; i<n; i++){
    				Field field = t.getField(i);
    				if(field.getType().equals(Type.INT_TYPE)){
    					int key = i;
    					int value = ((IntField)field).getValue();
    					if(max.get(key) == null && min.get(key) == null){
    						max.put(key, value);
							min.put(key, value);
    					}else if(value < min.get(key)){
    						min.put(key, value);
    					}else if(value > max.get(key)){
    						max.put(key, value);
    					}
    				}		
    			}
    		}

    		histogram = new Object[tupleNum];

    		//step two: to build Histogram of a table 
    		it.rewind();
    		while(it.hasNext()){
    			Tuple t = it.next();
    			TupleDesc td = t.getTupleDesc();
    			for(int i=0; i<td.numFields(); i++){
    				Field field = t.getField(i);
    				if(field.getType().equals(Type.INT_TYPE)){
    					if(histogram[i] == null){
    						histogram[i] = new IntHistogram(NUM_HIST_BINS, min.get(i), max.get(i));
    					}
    					((IntHistogram)histogram[i]).addValue(((IntField)field).getValue());
    				}else if(field.getType().equals(Type.STRING_TYPE)){
    					if(histogram[i] == null){
    						histogram[i] = new StringHistogram(NUM_HIST_BINS);	
    					}
    					((StringHistogram)histogram[i]).addValue(((StringField)field).getValue());
    				}
    			}
    		}

    		it.close();
    		transaction.commit();

    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return file.numPages() * iocost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(tupleNum * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        double selectivity = -1.;
        TupleDesc td = file.getTupleDesc();
        if(td.getFieldType(field).equals(Type.INT_TYPE)){
        	selectivity = ((IntHistogram)histogram[field]).avgSelectivity();
        }else if(td.getFieldType(field).equals(Type.STRING_TYPE)){
        	selectivity = ((StringHistogram)histogram[field]).avgSelectivity();
        }
        return selectivity;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	double selectivity = -1.;
        if(constant.getType().equals(Type.INT_TYPE)){
        	int v = ((IntField)constant).getValue();
        	selectivity = ((IntHistogram)histogram[field]).estimateSelectivity(op, v);
        }
        else if(constant.getType().equals(Type.STRING_TYPE)){
        	String str = ((StringField)constant).getValue();
        	selectivity = ((StringHistogram)histogram[field]).estimateSelectivity(op, str);
        }
        return selectivity;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.tupleNum;
    }

}
