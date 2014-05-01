import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class StripeMapper extends Mapper<Object, Text, Text, Text> {

	private HashMap<String, Double> map = new HashMap<String, Double>();
	private Text word = new Text();
	private Text t = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		ArrayList<String> words = new ArrayList<String>();
		String temp_url = value.toString();
		HashSet<String> set = new HashSet<String>();
		String temp = temp_url.replaceAll("http[s]?://t.co/[A-Za-z0-9]+", " ");
		temp = temp.replaceAll("[^A-Za-z0-9@#]", " ");
		temp = temp.replaceAll("#+", "#");
		StringTokenizer itr = new StringTokenizer(temp);
		while (itr.hasMoreTokens()) {
			String tempStr = itr.nextToken();
			if (!StopWords.stopWords.contains(tempStr.toLowerCase())) {
				words.add(tempStr);
			}
		}
		Collections.sort(words);
		for (int i = 0; i < words.size(); i++) {
			map.clear();
			word.clear();
			word.set(words.get(i));
			if (set.contains(words.get(i)) || "RT".equals(words.get(i))) {
				continue;
			} else {
				set.add(words.get(i));
				for (int j = 0; j < words.size(); j++) {
					String m = null;
					if (i != j && (!"RT".equals(words.get(j)))) {
						m = words.get(j);
						if (map.containsKey(m)) {
							double data = map.get(m) + 1;
							map.put(m, data);
						} else {
							map.put(m, 1d);
						}
					} else {
						continue;
					}

				}
				t.clear();
				t.set(getMapData(map));
				context.write(word, t);
			}
		}
	}

	public String getMapData(HashMap<String, Double> m) {
		if (m != null && m.size() > 0) {
			String returnString = "";
			Set<String> keySet = m.keySet();
			Iterator<String> it = keySet.iterator();
			while (it.hasNext()) {
				String temp = it.next();
				double value = m.get(temp);
				returnString += temp + "," + value + ";";
			}
			return returnString;
		}
		return "";
	}
}