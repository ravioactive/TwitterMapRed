import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ravioactive on 4/24/2014.
 */
public class KMeansUtil {
    private static final transient Logger LOG = LoggerFactory.getLogger(KMeansUtil.class);

    public static final double THRESHOLD = 0.005;
    public static final String KEY_COUNTER = "CENTROID_";
    public static final String KEY_GROUP = "KMEANS";

    public static final String CONF_NUM_CENTROIDS = "iteration.centroids.count";
    public static final String CONF_CENTROID_KEY = "iteration.centroid.";

    public static Map<Integer, Double> getInitialCentroids(int numClusters) {
        Map<Integer, Double> initCentroids = new HashMap<Integer, Double>();
        int i = 1;
        long v = 0;
        while(i <= numClusters) {
            initCentroids.put(i, (double)v);
            v += 1000*(++i);
        }
        return initCentroids;
    }

    public static Map<Integer, Double> readCentroids(Job job) throws IOException {
        //LOG.info("KMeansUtil: Reading centroids from Job");
        Map<Integer, Double> iterCentroids = new HashMap<Integer, Double>();
        int num = job.getConfiguration().getInt(CONF_NUM_CENTROIDS, -1);
        //LOG.info("KMeansUtil: Have to read " + num + " centroids");
        int i = 1;
        while(i <= num) {
            long value = job.getCounters().findCounter(KEY_GROUP, KEY_COUNTER + i).getValue();
            double dblValue = value/1000;
            //LOG.info("KMeansUtil: Centroid found for counter " + KEY_COUNTER + i + " : " + value + " as " + dblValue);
            iterCentroids.put(i, dblValue);
            i++;
        }

        return iterCentroids;
    }

    public static Reducer.Context writeCentroid(Reducer.Context context, int key, Double value) {
        long undecimalVal = undecimal(value);
        System.out.println("KMeansUtil: Modifying counter in Reducer for centroid " + key + " from " +
                context.getCounter(KEY_GROUP, KEY_COUNTER + key).getValue() +
                " to " + value + " as " + undecimalVal);
        context.getCounter(KEY_GROUP, KEY_COUNTER + key).setValue(undecimalVal);
        return context;
    }

    private static long undecimal(double decimalVal) {
        long undecimalVal = -1;
        try {
            DecimalFormat df = new DecimalFormat("#.###");
            df.setMaximumFractionDigits(3);
            df.setRoundingMode(RoundingMode.UP);
            String formattedVal = df.format(decimalVal);
            double formattedDecimalVal = Double.valueOf(formattedVal);
            undecimalVal = (long)(formattedDecimalVal * 1000);
        } catch (Exception e) {
            undecimalVal = -1;
        }
        return undecimalVal;
    }

    public static void writeCentroids(Configuration conf, Map<Integer, Double> tempCentroids) {
        int num = tempCentroids.size();
        conf.setInt(CONF_NUM_CENTROIDS, num);
        //System.out.println("KMeansUtil: Setting " + num + " number of counters in Configuration.");
        int i = 1;
        for(Integer entry : tempCentroids.keySet()) {
            double decimalVal = tempCentroids.get(entry);
            conf.setDouble(CONF_CENTROID_KEY + i, decimalVal);
            //System.out.println("KMeansUtil: Setting centroid in configuration for " + CONF_CENTROID_KEY + i + " : " + decimalVal);
            i++;
        }
    }

    public static Map<Integer, Double> readCentroids(Configuration conf) {
        Map<Integer, Double> iterCentroids = new HashMap<Integer, Double>();
        int num = conf.getInt(CONF_NUM_CENTROIDS, -1);
        System.out.println("KMeansUtil: Reading " + num + " centroids from Configuration");
        boolean isNeg = false;
        if(num > 0) {
            int i = 1;
            while(i <= num) {
                double centroid = conf.getDouble(CONF_CENTROID_KEY + i, -1.0);
                if(centroid >= 0) {
                    System.out.println("KMeansUtil: Found Centroid " + i + " : " + centroid + " in Configuration");
                    iterCentroids.put(i, centroid);
                    i++;
                    isNeg = false;
                } else {
                    if(!isNeg) {
                        System.out.println("KMeansUtil: Centroid not found for " + CONF_CENTROID_KEY + i);
                        isNeg = true;
                    } else {
                        System.out.print("."+i);
                    }
                    i++;
                }
            }
        }
        return iterCentroids;
    }

    public static void showOutput(Map<Integer, Double> tempCentroids) {
        for(Map.Entry<Integer, Double> entry : tempCentroids.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}
