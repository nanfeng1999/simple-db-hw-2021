package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int min;
    private final int max;
    private final int buckets;
    private final double width;
    private int numTuples;
    private final int[] map; // todo:can use array replace it
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
        // todo:The previous intervals are all left closed and right open, and the last interval is left closed and right closed
    	// some code goes here
        this.buckets = buckets;
        this.max = max;
        this.min = min;
        this.width = (double) (max - min) / buckets;
        this.numTuples = 0;
        this.map = new int[buckets];
    }

    private int getIdx(int v){
        if (v == max){
            return buckets - 1;
        }else{
            return (int) ((v-min)/width);
        }
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

        map[getIdx(v)]++;
        numTuples++;
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
                ret = 1 - estimateLessThanSelectivity(v) - estimateEqualSelectivity(v);
                break;
            case LESS_THAN:
                ret = estimateLessThanSelectivity(v);
                break;
            case LESS_THAN_OR_EQ:
                ret = estimateLessThanSelectivity(v) + estimateEqualSelectivity(v);
                break;
            case GREATER_THAN_OR_EQ:
                ret = 1 - estimateLessThanSelectivity(v);
                break;
            case NOT_EQUALS:
                ret = 1 - estimateEqualSelectivity(v);
                break;
        }

        return ret;
    }

    private double estimateEqualSelectivity(int v){
        if (v < min || v > max || numTuples == 0){
            return 0.0;
        }

        // todo: Note that the selectivity estimate for width less than 1 is simply the number of intervals divided by the total
        return (double) map[getIdx(v)] / (((int)width + 1) * numTuples);
    }

    private double estimateLessThanSelectivity(int v){
        if (v > max){
            return 1.0;
        }

        if (v <= min){
            return 0.0;
        }

        double ret = 0.0;
        int index = getIdx(v);

        for (int i = 0; i < index; i++) {
            ret += (double) map[i]/numTuples;
        }

        ret += (map[index] + 0.0) * (v - min - index * width )/(width * numTuples);

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
        for (int i = 0; i < map.length; i++) {
            tmp.append(String.format("%.2f - %.2f:\t%d",
                    i * width + min,
                    (i + 1) * width + min ,
                    map[i]));
            tmp.append("\n");
        }

        return tmp.toString();
    }
}
