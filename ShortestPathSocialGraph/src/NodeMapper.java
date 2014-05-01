import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NodeMapper extends Mapper<Object, Text, LongWritable, Text> {

	private Text word = new Text();
	private LongWritable l = new LongWritable();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		if (!"".equals(value.toString())) {
			String data[] = value.toString().split("[ \t]+");
			long nodeId = Long.parseLong(data[0]);
			long distance = Long.parseLong(data[1]);
			String adjacencyList[] = data[2].split(":");

			word.clear();
			word.set("node," + data[2]);
			l.set(nodeId);
			context.write(l, word);

			word.clear();
			word.set("org_distance," + distance);
			context.write(l, word);

			distance++;
			word.clear();
			word.set("distance," + distance);
			for (int i = 0; i < adjacencyList.length; i++) {
				l.set(Long.parseLong(adjacencyList[i]));
				context.write(l, word);
			}
		}
	}
}