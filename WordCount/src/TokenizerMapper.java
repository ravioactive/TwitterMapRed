import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String temp_url = value.toString();
		String temp = temp_url.replaceAll("http[s]?://t.co/[A-Za-z0-9]+", " ");
		temp = temp.replaceAll("[^A-Za-z0-9@#]", " ");
		temp = temp.replaceAll("#+", "#");
		StringTokenizer itr = new StringTokenizer(temp);
		while (itr.hasMoreTokens()) {
			String tempStr = itr.nextToken();
			if (!StopWords.stopWords.contains(tempStr.toLowerCase())) {
				word.set(tempStr);
				context.write(word, one);
			}
		}
	}
}