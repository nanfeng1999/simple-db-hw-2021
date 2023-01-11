package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.HashMap;
import java.util.Map;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int min;
    private final int max;
    private final int buckets;
    private final double width;
    private int totalNum;
    private final Map<Integer,Integer> map;
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
        this.max = max;
        this.min = min;
        this.width = (double) (max - min + 1) / buckets;
        this.totalNum = 0;
        this.map = new HashMap<>();
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v < min || v > max){
            return;
        }
        int index = (int)((v - min) / width);
        map.merge(index,1,(old,_new)->old+1);
        totalNum++;
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
        double ret = 0.0;
        switch (op) {
            case EQUALS:
                ret = estimateEqualSelectivity(v);
                break;
            case GREATER_THAN:
                ret = estimateGreaterThanSelectivity(v);
                break;
            case LESS_THAN:
                ret = 1 - estimateEqualSelectivity(v) - estimateGreaterThanSelectivity(v);
                break;
            case LESS_THAN_OR_EQ:
                ret = 1 - estimateGreaterThanSelectivity(v);
                break;
            case GREATER_THAN_OR_EQ:
                ret = estimateEqualSelectivity(v) + estimateGreaterThanSelectivity(v);
                break;
            case NOT_EQUALS:
                ret = 1 - estimateEqualSelectivity(v);
                break;
        }

        return ret;
    }

    private double estimateEqualSelectivity(int v){
        if (v < min || v > max || totalNum == 0){
            return 0.0;
        }

        int index = (int)((v - min) / width);

        if (index == buckets - 1){
            return (double) map.getOrDefault(index,0) / ((totalNum - width*(buckets-1)) * totalNum);
        }

        return (double) map.getOrDefault(index,0) / (width * totalNum);
    }

    private double estimateGreaterThanSelectivity(int v){
        if (v > max){
            return 0.0;
        }

        if (v < min){
            return 1.0;
        }

        double ret = 0.0;
        int index = (int)((v - min) / width);

        if (index == buckets - 1){
            ret = (double) (max - v) * map.getOrDefault(index,0) / ((totalNum - width*(buckets-1)) * totalNum);
        } else if (index < buckets - 1) {
            ret = (index * width + min - v) * map.getOrDefault(index,0) / (width * totalNum);
            index ++;
            while (index < buckets){
                ret += (double) map.getOrDefault(index,0) / totalNum;
                index++;
            }
        }

        return ret;
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
        StringBuilder tmp = new StringBuilder();
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            tmp.append(String.format("%.2f - %.2f:\t%d",
                    entry.getKey() * width + min,
                    Math.min((entry.getKey() + 1) * width - 1 + min,max) ,
                    entry.getValue()));
            tmp.append("\n");
        }

        return tmp.toString();
    }
}
