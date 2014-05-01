import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by ravioactive on 4/24/2014.
 */

public class KMeans {

    public static final String INPUT_PATH = "/input";
    public static final String OUTPUT_PATH = "/output";
    public static final int NUM_CLUSTERS = 10;

    private static final transient Logger LOG = LoggerFactory.getLogger(KMeans.class);


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
        LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));

        String inputPath = "/input";
        String outputPath = "/output";

		/* FileOutputFormat wants to create the output directory itself.
		 * If it exists, delete it:
		 */

        int K = 10;
        boolean isConverged = false;
        int maxIterations = 50;
        boolean iterationFinished = true;
        int numIteration = 0;
        Map<Integer, Double> tempCentroids = KMeansUtil.getInitialCentroids(NUM_CLUSTERS);
        LOG.info("======== Job Info ========");
        LOG.info("Clusters: "+NUM_CLUSTERS);
        LOG.info("initial centroids: ");
        KMeansUtil.showOutput(tempCentroids);

        while(!isConverged && iterationFinished && (numIteration < maxIterations)) {
            numIteration++;
            LOG.info("=========== ITERATION " + numIteration + " ===========");
            conf.setInt("iteration", numIteration);
            KMeansUtil.writeCentroids(conf, tempCentroids);
            Job job = Job.getInstance(conf);
            job.setJarByClass(KMeans.class);

            job.setMapperClass(KMeansMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
            String iterOutputPath = OUTPUT_PATH + numIteration;
            deleteFolder(conf, iterOutputPath);
            FileOutputFormat.setOutputPath(job, new Path(iterOutputPath));

            iterationFinished = job.waitForCompletion(true);
            long convergeCount = job.getCounters().findCounter(KMeansReducer.Convergence.CONVERGENT).getValue();
            if(convergeCount == NUM_CLUSTERS) {
                isConverged = true;
                LOG.info("=========== KMEANS HAS CONVERGED IN ITERATION " + numIteration + " ===========");
            }

            LOG.info("=========== ITERATION: " + numIteration + " OUTPUT =========== ");
            tempCentroids = KMeansUtil.readCentroids(job);
            KMeansUtil.showOutput(tempCentroids);
        }

        LOG.info("=========================== CLUSTER MEANS ===========================");
        KMeansUtil.showOutput(tempCentroids);

    }

    /**
     * Delete a folder on the HDFS. This is an example of how to interact
     * with the HDFS using the Java API. You can also interact with it
     * on the command line, using: hdfs dfs -rm -r /path/to/delete
     *
     * @param conf a Hadoop Configuration object
     * @param folderPath folder to delete
     * @throws java.io.IOException
     */
    private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(folderPath);
        if(fs.exists(path)) {
            fs.delete(path,true);
        }
    }
}
