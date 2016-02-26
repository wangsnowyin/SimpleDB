package simpledb;

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int buckets; // The number of buckets in the histogram
	private int min; // The minimum value of the field
	private int max; // The maximum value of the field
	
	private int ntups = 0; // The number of tuples in the table
	
	// Attributes of the IntHistogram of a field
	private int w_b; // Width of a bucket = the range of records in one bucket
	private int[] h_b; // Heights of each bucket = the number of records in the bucket
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	
    	// Split the records range from min to max into buckets number of buckets, 
    	// each bucket with width w_b
    	this.w_b = (int)Math.ceil(((double)max - min) / buckets);
    	//System.out.println("max = " + max + ",min = " + min + ",width " + w_b + ",bucket: " + buckets); 
    	// Initialize each range with 0 records, 
    	// each bucket with height 0
    	h_b = new int[buckets];
    	for(int i = 0; i < buckets; i++)
    		h_b[i] = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	int bucket_i = 0; // which bucket the value lies in
    	// maximum and minimum
    	if(v == min)
    		bucket_i = 0;
    	else if(v == max)
    		bucket_i = buckets - 1;
    	else if(v > min && v < max)// min < v < max
    		bucket_i = (v - min) / w_b;
    	// Update that number of records in that bucket
    	h_b[bucket_i]++;
    	// Update the number of tuples in the table
    	ntups++;
    	//System.out.println("value: " + v); 
    	//System.out.println("bucket number " + bucket_i + ", number of records " + h_b[bucket_i]);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
    	
    	// Estimate the selectivity of f = v
    	if(op == Op.EQUALS) {
    		if(v == max)
    			return ((double)h_b[buckets - 1] / w_b) / ntups;
    		// The value is within the range
    		if(v >= min && v < max) {
    			// Get the height of the bucket which v lies in
    			int height_v = h_b[(v - min) / w_b];
    			// Return the predicted selectivity: (h/w)/ntups
    			return ((double)height_v / w_b) / ntups;
    		}
    		// v is out of the field range
    		else
    			return 0.0;
    	} 
    	
    	// Estimate the selectivity of f > v
    	if(op == Op.GREATER_THAN) {
    		// All elements are greater than a value less than the minimum
    		if(v < min)
    			return 1.0;
    		// None is greater than the maximum
    		if(v >= max)
    			return 0.0;
    		// v is within the range
    		if(v >=min && v < max) {
    			// Get the bucket v lies in
    			int bucket_v = (v - min) / w_b;
    			// fraction b_f = h_b / ntups of the total tuples
    			double b_f = (double)h_b[bucket_v] / ntups;
    			// Get the right endpoint of the bucket b
    			int b_right = min + w_b * (bucket_v + 1) - 1;
    			// fraction b_part is (b_right - const) / w_b
    			double b_part = (b_right - v) / w_b;
    			// the selectivity of bucket b: b_f * b_part
    			double selectivity =  b_f * b_part;
    			
    			// Selectivity of bucket b+1 to NumB-1
    			for(int i = bucket_v + 1; i < buckets; i++) 
    				selectivity += (double)h_b[i] / ntups;
    			
    			return selectivity;
    		}
    	} 
    	
    	// Estimate the selectivity of f >= v
    	if(op == Op.GREATER_THAN_OR_EQ) {
    		// sum of selectivity f > v and f = v
    		return estimateSelectivity(Op.GREATER_THAN, v) + estimateSelectivity(Op.EQUALS, v);
    	} 
    	
    	// Estimate the selectivity of f < v
    	// Similar to the greater than case,
		// looking at buckets down to 0
    	if(op == Op.LESS_THAN) {
    		// None is less than the minimum
    		if(v <= min)
    			return 0.0;
    		// All are less than a value greater than the maximum
    		if(v > max)
    			return 1.0;
    		// v is within the range
    		if(v > min && v <= max) {
    			// Get the bucket v lies in
    			int bucket_v = (v - min) / w_b;
    			// fraction b_f = h_b / ntups of the total tuples
    			double b_f = (double)h_b[bucket_v] / ntups;
    			// Get the left endpoint of the bucket b
    			int b_left = min + w_b * bucket_v;
    			// fraction b_part is (b_right - const) / w_b
    			double b_part = (v - b_left) / w_b;
    			// the selectivity of bucket b: b_f * b_part
    			double selectivity =  b_f * b_part;
    			
    			// Selectivity of bucket 0 to b-1
    			for(int i = 0; i < bucket_v; i++) 
    				selectivity += (double)h_b[i] / ntups;
    			
    			return selectivity;
    		}
    	}
    	
    	// Estimate the selectivity of f <= v
    	if(op == Op.LESS_THAN_OR_EQ) {
    		// sum of selectivity f < v and f = v
    		return estimateSelectivity(Op.LESS_THAN, v) + estimateSelectivity(Op.EQUALS, v);
    	} 	
    	
    	// Estimate the selectivity of f contains v
    	if(op == Op.LIKE) {
    		// for integers, equals to the selectivity f = v
    		return estimateSelectivity(Op.EQUALS, v);
    	}
    	
    	// Estimate the selectivity of f != v
    	if(op == Op.NOT_EQUALS) {
    		// all elements except f = v
    		return 1.0 - estimateSelectivity(Op.EQUALS, v);
    	}
    	
    	else
    		return -1.0;

    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
    	String histogram = "Histogram with elements from " + min + " to " + max + "\n";
    	histogram += "Split into " + buckets + " buckets, each with width " + w_b + "\n";
    	histogram += "Number of records in each bucket: ";
    	for(int i = 0; i < buckets; i++)
    		histogram += h_b[i] + " ";
    	return histogram;
    }
}
