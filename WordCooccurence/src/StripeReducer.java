import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeReducer extends Reducer<Text, Text, Text, Text> {
	private Text t = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		HashMap<String, Double> finalMap = new HashMap<String, Double>();
		double sum = 0;
		for (Text val : values) {
			String data[] = val.toString().split(";");
			for (int i = 0; i < data.length; i++) {
				if (!"".equals(data[i])) {
					String newData[] = data[i].split(",");
					sum += Double.parseDouble(newData[1]);
					if (finalMap.containsKey(newData[0])) {
						double d = finalMap.get(newData[0])
								+ Double.parseDouble(newData[1]);
						finalMap.put(newData[0], d);
					} else {
						finalMap.put(newData[0], Double.parseDouble(newData[1]));
					}
				}
			}
			
			/*Set<Writable> keySet = val.keySet();
			Iterator<Writable> it = keySet.iterator();
			while (it.hasNext()) {
				Text temp = (Text) it.next();
				sum += ((DoubleWritable) val.get(temp)).get();
				if (finalMap.containsKey(temp)) {
					DoubleWritable data = new DoubleWritable(
							(((DoubleWritable) finalMap.get(temp)).get() + ((DoubleWritable) val
									.get(temp)).get()));
					finalMap.put(temp, data);
				} else {
					finalMap.put(temp, ((DoubleWritable) val.get(temp)));
				}
			}*/
		}
		/*Set<Writable> finalKeySet = finalMap.keySet();
		Iterator<Writable> it1 = finalKeySet.iterator();
		while (it1.hasNext()) {
			Text temp = (Text) it1.next();
			DoubleWritable data = new DoubleWritable(
					(((DoubleWritable) finalMap.get(temp)).get() / sum));
			finalMap.put(temp, data);
			// Text te = new Text();
			// te.set(temp+" "+data);
			// context.write(key, te);
		}*/
		
		t.clear();
		t.set(getMapData(finalMap, sum));
		context.write(key, t);
	}

	public String getMapData(HashMap<String, Double> m, double sum) {
		if (m != null && m.size() > 0) {
			String returnString = "";
			Set<String> keySet = m.keySet();
			Iterator<String> it = keySet.iterator();
			while (it.hasNext()) {
				String temp = it.next();
				double value = (m.get(temp) / sum);
				returnString += temp + "," + value + ";";
			}
			return returnString;
		}
		return "";
	}
}