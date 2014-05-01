import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SortingMapper extends Mapper<Object, Text, IntWritable, Text> {

	private IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		if (itr.countTokens() == 2) {
			word.set(itr.nextToken());
			one.set(Integer.parseInt(itr.nextToken()));
			context.write(one, word);
		}
	}
}