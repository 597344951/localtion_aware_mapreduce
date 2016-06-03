package xx.local.mr.zy;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HfgjReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// int count = 0;
		// values.iterator().
		String dateStr = "";
		for (Text text : values) {
			// context.write(arg0, text);
			dateStr = text.toString();
			break;
		}
		context.write(arg0, new Text(dateStr));
		// super.reduce(arg0, arg1, arg2);
	}

	// @Override
	// protected void setup(Reducer<Text, Text, Text, Text>.Context context)
	// throws IOException, InterruptedException {
	// // TODO Auto-generated method stub
	//
	// }

	// public void reduce(Text key, Iterator<Text> values,
	// OutputCollector<Text, Text> output, Reporter reporter)
	// throws IOException {
	// // int count = 0;
	// // StringBuffer timestr=new StringBuffer();
	// while (values.hasNext()) {
	// // count++;
	// // timestr.append(values.next().toString());
	// // timestr.append(",");
	// output.collect(key, new Text(values.next().toString()));
	// }
	// // if (count >= 3) {
	// // // output.collect(key, new Text(count+","+timestr.substring(0,
	// // timestr.length()-1)));
	// // }
	// }

}
