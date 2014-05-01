import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairComparator extends WritableComparator implements
		WritableComparable<Text> {

	protected PairComparator() {
		super(Text.class, true);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int compareTo(Text o) {
		String data1[] = this.toString().split(",");
		String data2[] = o.toString().split(",");
		int compareValue = data1[0].compareTo(data2[0]);
		if (compareValue == 0) {
			if (data1[1].equals("*") && data2[1].equals("*")) {
				return 0;
			} else if (data1[1].equals("*")) {
				return -1;
			} else if (data2[1].equals("*")) {
				return 1;
			} else {
				return data1[1].compareTo(data2[1]);
			}
		} else {
			return compareValue;
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text t1 = (Text) w1;
		Text t2 = (Text) w2;
		String data1[] = t1.toString().split(",");
		String data2[] = t2.toString().split(",");
		int compareValue = data1[0].compareTo(data2[0]);
		if (compareValue == 0) {
			if (data1[1].equals("*") && data2[1].equals("*")) {
				return 0;
			} else if (data1[1].equals("*")) {
				return -1;
			} else if (data2[1].equals("*")) {
				return 1;
			} else {
				return data1[1].compareTo(data2[1]);
			}
		} else {
			return compareValue;
		}
	}

}
