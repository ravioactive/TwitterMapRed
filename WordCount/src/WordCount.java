import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
		String outputPathOriginal = "/output/original";
		String inputPathFinalSort = "/input/final_sort";
		String outputPathFinalSort = "/output/final_sort";
		String inputPathFinalTweet = "/input/final_tweet";
		String outputPathFinalTweet = "/output/final_tweet";
		String outputPathFinalTweetSort = "/output/final_tweet/sort";
		String inputPathFinalHash = "/input/final_hash";
		String outputPathFinalHash = "/output/final_hash";
		String outputPathFinalHashSort = "/output/final_hash/sort";
		String output = "/output";
		/*
		 * FileOutputFormat wants to create the output directory itself. If it
		 * exists, delete it:
		 */
		deleteFolder(conf, outputPathOriginal);
		deleteFolder(conf, outputPathFinalSort);
		deleteFolder(conf, outputPathFinalTweet);
		deleteFolder(conf, outputPathFinalHash);
		deleteFolder(conf, outputPathFinalTweetSort);
		deleteFolder(conf, outputPathFinalHashSort);
		deleteFolder(conf, inputPathFinalSort);
		deleteFolder(conf, inputPathFinalTweet);
		deleteFolder(conf, inputPathFinalHash);
		deleteFolder(conf, output);

		// createFolder(conf, outputPathOriginal);
		// createFolder(conf, outputPathFinalSort);
		// createFolder(conf, inputPathFinalSort);

		Job job = Job.getInstance(conf);
		Job job2 = null, job3 = null, job4 = null;
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPathOriginal));
		FileOutputFormat.setOutputPath(job, new Path(outputPathOriginal));

		if (job.waitForCompletion(true)) {
			job2 = Job.getInstance(conf);
			job2.setJarByClass(WordCount.class);
			job2.setMapperClass(SortingMapper.class);
			job2.setCombinerClass(SortReducer.class);
			job2.setReducerClass(SortReducer.class);
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path(outputPathOriginal));
			FileOutputFormat.setOutputPath(job2, new Path(outputPathFinalSort));
		}

		if (job2.waitForCompletion(true)) {
			job3 = Job.getInstance(conf);
			job3.setJarByClass(WordCount.class);
			job3.setMapperClass(HashTagMapper.class);
			job3.setCombinerClass(HashTagReducer.class);
			job3.setReducerClass(HashTagReducer.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job3, new Path(inputPathOriginal));
			FileOutputFormat.setOutputPath(job3, new Path(outputPathFinalHash));
			if(job3.waitForCompletion(true)){
				job3 = Job.getInstance(conf);
				job3.setJarByClass(WordCount.class);
				job3.setMapperClass(SortingMapper.class);
				job3.setCombinerClass(SortReducer.class);
				job3.setReducerClass(SortReducer.class);
				job3.setOutputKeyClass(IntWritable.class);
				job3.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job3, new Path(outputPathFinalHash));
				FileOutputFormat.setOutputPath(job3, new Path(outputPathFinalHashSort));
			}
		}
		
		if (job3.waitForCompletion(true)) {
			job4 = Job.getInstance(conf);
			job4.setJarByClass(WordCount.class);
			job4.setMapperClass(TweetMapper.class);
			job4.setCombinerClass(TweetReducer.class);
			job4.setReducerClass(TweetReducer.class);
			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job4, new Path(inputPathOriginal));
			FileOutputFormat.setOutputPath(job4, new Path(outputPathFinalTweet));
			if(job4.waitForCompletion(true)){
				job4 = Job.getInstance(conf);
				job4.setJarByClass(WordCount.class);
				job4.setMapperClass(SortingMapper.class);
				job4.setCombinerClass(SortReducer.class);
				job4.setReducerClass(SortReducer.class);
				job4.setOutputKeyClass(IntWritable.class);
				job4.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job4, new Path(outputPathFinalTweet));
				FileOutputFormat.setOutputPath(job4, new Path(outputPathFinalTweetSort));
			}
			System.exit(job4.waitForCompletion(true) ? 0 : 1);
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
	/*
	 * private static void createFolder(Configuration conf, String folderPath)
	 * throws IOException { FileSystem fs = FileSystem.get(conf); Path path =
	 * new Path(folderPath); if (fs.exists(path)) { fs.delete(path, true); }
	 * fs.create(path); }
	 */
}