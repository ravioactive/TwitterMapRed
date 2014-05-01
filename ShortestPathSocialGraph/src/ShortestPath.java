import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ShortestPath {

	private static final transient Logger LOG = LoggerFactory
			.getLogger(ShortestPath.class);

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		/* Set the Input/Output Paths on HDFS */
		String inputPathOriginal = "/input";
		String outputPathOriginal = "/output";
		/*
		 * FileOutputFormat wants to create the output directory itself. If it
		 * exists, delete it:
		 */
		deleteFolder(conf, outputPathOriginal);

		// createFolder(conf, outputPathOriginal);
		// createFolder(conf, outputPathFinalSort);
		// createFolder(conf, inputPathFinalSort);
		boolean isCompleted = false;
		int count = 0;
		while (isCompleted == false) {
			String input = null;
			String output = null;
			if (count == 0) {
				input = inputPathOriginal;
			} else {
				input = outputPathOriginal + "/" + count;
			}
			count++;
			output = outputPathOriginal + "/" + count;
			deleteFolder(conf, output);
			Job job = Job.getInstance(conf);
			job.setJarByClass(ShortestPath.class);
			job.setMapperClass(NodeMapper.class);
			job.setReducerClass(NodeReducer.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			boolean isJobCompleted = job.waitForCompletion(true);
			if (isJobCompleted) {
				isCompleted = (job.getCounters().getGroup("group")
						.findCounter("isModified").getValue() > 0 ? false
						: true);
			}
		}
		/*FileSystem fs = FileSystem.get(conf);
		fs.rename(new Path(outputPathOriginal + "/" + count), new Path(
				outputPathOriginal));
		for (int i = 0; i < count; i++) {
			deleteFolder(conf, outputPathOriginal + "/" + count);
		}*/
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

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