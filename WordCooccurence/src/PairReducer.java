import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();
	private DoubleWritable sumValue = new DoubleWritable();

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double sum = 0;
		double finalSum = 0;

		for (DoubleWritable val : values) {
			sum += val.get();
		}
		String temp = key.toString().split(",")[1];
		if ("*".equals(temp)) {
			sumValue.set(sum);
			result.set(sum);
		} else {
			finalSum = (sum / (sumValue.get()));
			result.set(finalSum);
		}
		context.write(key, result);
	}
}