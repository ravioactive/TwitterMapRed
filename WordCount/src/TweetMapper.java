import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	static Pattern p = Pattern.compile("@[^\\s]+");

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String temp_url = value.toString();
		String temp = temp_url.replaceAll("http[s]?://t.co/[A-Za-z0-9]+", " ");
		temp = temp.replaceAll("[^A-Za-z0-9@#]", " ");
		temp = temp.replaceAll("#+", "#");
		Matcher m = p.matcher(temp);
		while(m.find()){
			word.clear();
			word.set(m.group());
			context.write(word, one);
		}
	}
}