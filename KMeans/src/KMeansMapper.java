import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ravioactive on 4/24/2014.
 */
public class KMeansMapper extends Mapper<Object, Text, IntWritable, LongWritable> {
    private static final transient Logger LOG = LoggerFactory.getLogger(KMeansMapper.class);

    Map<Integer, Double> centroids = new HashMap<Integer, Double>();
    int iteration = -1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        iteration = conf.getInt("iteration", -1);
        System.out.println("Setting up Mapper " + iteration);
        centroids = KMeansUtil.readCentroids(conf);
        System.out.println("Mapper " + iteration + " Received " + centroids.size() + " centroids as");
        KMeansUtil.showOutput(centroids);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        LongWritable followerCount = parseEntryToValue(value);

        int clusterIndex = -1;
        Double minDistance = Double.MAX_VALUE;
        if(followerCount!=null) {
            System.out.println("Mapper " + iteration + " map() got value " + value.toString() + " parsed to " + followerCount.get());
            for(Integer idx : centroids.keySet()) {
                Double distance = Math.abs(centroids.get(idx) - followerCount.get());
                if(distance < minDistance) {
                    minDistance = distance;
                    clusterIndex = idx;
                }
                System.out.println("Mapper " + iteration + " Distance of " + centroids.get(idx) + " from " + followerCount.get() + " is " + distance);
            }
            System.out.println("Mapper " + iteration + " Closest centroid is " + centroids.get(clusterIndex) + " for " + followerCount.get());
            context.write(new IntWritable(clusterIndex), followerCount);
        } else {
            LOG.error("Mapper " + iteration + " follower count received is NULL.");
        }
    }

    LongWritable parseEntryToValue(Text fileEntry) {
        LongWritable followerCount = null;
        try {
            String userEntry = fileEntry.toString();
            String parts[] = userEntry.split(",");

            Long followerCountValue = Long.parseLong(parts[1].trim());
            followerCount = new LongWritable(followerCountValue);
        } catch(Exception e) {
            followerCount = null;
        }

        return followerCount;
    }

}
