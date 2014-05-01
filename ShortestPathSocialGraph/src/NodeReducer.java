import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NodeReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {
	Text word = new Text();

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long lowest = -1;
		long orgDistance = -1;
		String node = null;
		for (Text t : values) {
			String data[] = t.toString().split(",");
			if ("node".equals(data[0])) {
				node = data[1];
			} else if ("distance".equals(data[0])) {
				long distance = Long.parseLong(data[1]);
				if (lowest == -1) {
					lowest = distance;
				} else {
					if (lowest > distance) {
						lowest = distance;
					}
				}
			} else if ("org_distance".equals(data[0])) {
				orgDistance = Long.parseLong(data[1]);
			}
		}
		if (orgDistance > lowest && lowest > -1){
			context.getCounter("group", "isModified").increment(1);
		} else {
			lowest = orgDistance;
		}
			

		word.clear();
		word.set(lowest + " " + node);
		context.write(key, word);
	}

}