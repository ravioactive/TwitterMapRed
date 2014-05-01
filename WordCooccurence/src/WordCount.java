import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WordCount {

	private static final transient Logger LOG = LoggerFactory
			.getLogger(WordCount.class);

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		/* Set the Input/Output Paths on HDFS */
		String inputPathOriginal = "/input";
		String outputPathOriginal = "/output";
		String outputPathPair = "/output/pair";
		String outputPathStripes = "/output/stripes";

		/*
		 * FileOutputFormat wants to create the output directory itself. If it
		 * exists, delete it:
		 */
		deleteFolder(conf, outputPathOriginal);
		deleteFolder(conf, outputPathPair);
		deleteFolder(conf, outputPathStripes);
		deleteFolder(conf, outputPathOriginal);

		// createFolder(conf, outputPathOriginal);
		// createFolder(conf, outputPathFinalSort);
		// createFolder(conf, inputPathFinalSort);

		Job job = Job.getInstance(conf);
		Job job2 = null;
		job.setJarByClass(WordCount.class);
		job.setMapperClass(PairMapper.class);
		// job.setCombinerClass(PairReducer.class);
		job.setReducerClass(PairReducer.class);
		job.setNumReduceTasks(2);
		job.setPartitionerClass(PairPartitioner.class);
		job.setSortComparatorClass(PairComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPathOriginal));
		FileOutputFormat.setOutputPath(job, new Path(outputPathPair));
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

		if (job.waitForCompletion(true)) {
			job2 = Job.getInstance(conf);
			job2.setJarByClass(WordCount.class);
			job2.setMapperClass(StripeMapper.class);
			job2.setCombinerClass(StripeReducer.class);
			job2.setReducerClass(StripeReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path(inputPathOriginal));
			FileOutputFormat.setOutputPath(job2, new Path(outputPathStripes));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}

	/**
	 * Delete a folder on the HDFS. This is an example of how to interact with
	 * the HDFS using the Java API. You can also interact with it on the command
	 * line, using: hdfs dfs -rm -r /path/to/delete
	 * 
	 * @param conf
	 *            a Hadoop Configuration object
	 * @param folderPath
	 *            folder to delete
	 * @throws IOException
	 */
	private static void deleteFolder(Configuration conf, String folderPath)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}
}