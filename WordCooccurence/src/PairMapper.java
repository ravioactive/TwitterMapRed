import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.StringTokenizer;

public class PairMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	private final static DoubleWritable one = new DoubleWritable(1);
	private Text word = new Text();
	private DoubleWritable sumValue = new DoubleWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		ArrayList<String> words = new ArrayList<String>();
		String temp_url = value.toString();
		String temp = temp_url.replaceAll("http[s]?://t.co/[A-Za-z0-9]+", " ");
		temp = temp.replaceAll("[^A-Za-z0-9@#]", " ");
		temp = temp.replaceAll("#+", "#");
		HashSet<String> set = new HashSet<String>();
		StringTokenizer itr = new StringTokenizer(temp);
		while (itr.hasMoreTokens()) {
			String tempStr = itr.nextToken();
			if (!StopWords.stopWords.contains(tempStr.toLowerCase())) {
				words.add(tempStr);
			}
		}
		Collections.sort(words);
		for (int i = 0; i < words.size(); i++) {
			int sum = 0;
			if (set.contains(words.get(i)) || "RT".equals(words.get(i))) {
				continue;
			} else {
				set.add(words.get(i));
				for (int j = 0; j < words.size(); j++) {
					if (i == j || "RT".equals(words.get(j))) {
						continue;
					}
					sum += 1;
					word.clear();
					word.set(words.get(i) + "," + words.get(j));
					context.write(word, one);
				}
				word.clear();
				word.set(words.get(i) + ",*");
				sumValue.set(sum);
				context.write(word, sumValue);
			}
		}
	}
}