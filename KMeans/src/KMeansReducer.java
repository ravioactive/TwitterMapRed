import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ravioactive on 4/24/2014.
 */
public class KMeansReducer extends Reducer<IntWritable, LongWritable, IntWritable, DoubleWritable> {

    private static final transient Logger LOG = LoggerFactory.getLogger(KMeansReducer.class);

    int iteration = -1;
    Map<Integer, Double> centroids = new HashMap<Integer, Double>();
    public static enum Convergence {
        DIVERGENT, CONVERGENT
    }

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        iteration = conf.getInt("iteration", -1);
        System.out.println("Setting up Reducer " + iteration);
        centroids = KMeansUtil.readCentroids(conf);
        System.out.println("Reducer " + iteration + " Received " + centroids.size() + " centroids as");
        KMeansUtil.showOutput(centroids);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Double currentCentroid = centroids.get(key.get());
        System.out.println("Reducer " + iteration + " Current Centroid: " + currentCentroid);
        Double newCentroid = getNewCentroid(values);
        System.out.println("Reducer " + iteration + " Reducer for centroid " + key.get() + " which is " + centroids.get(key.get()));
        if(newCentroid != null) {
            System.out.println("Reducer " + iteration + " New centroid calculated for " + key.get() + " is " + newCentroid);
            if(belowThreshold(currentCentroid, newCentroid)) {
                context.getCounter(Convergence.CONVERGENT).increment(1);
                System.out.println("Reducer " + iteration + " REDUCER CONVERGED!" + context.getCounter(Convergence.CONVERGENT).getValue() + " times");
            }
            context = KMeansUtil.writeCentroid(context, key.get(), newCentroid);
            DoubleWritable op = new DoubleWritable(newCentroid);
            context.write(key, op);
        } else {
            System.out.println("Reducer " + iteration + " New Centroid calculated is NULL!");
        }
    }

    private boolean belowThreshold(Double currentCentroid, Double newCentroid) {
        return Math.abs(currentCentroid - newCentroid) <= KMeansUtil.THRESHOLD;
    }

    private Double getNewCentroid(Iterable<LongWritable> values) {
        Double average = null;
        int numValues = 0;
        long total = 0L;
        for(LongWritable val : values) {
            total += val.get();
            numValues++;
        }

        if(numValues > 0) {
            average = (double) (total/numValues);
        }

        return average;
    }

}
