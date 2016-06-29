/**
 * Project Name:BigCount
 * File Name:RlReduce.java
 * Package Name:com.zltel.data.rl
 * Date:2016年3月21日下午4:39:58
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.splitH;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ClassName:RlReduce <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年3月21日 下午4:39:58 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class HfrlReduce extends Reducer<Text, Text, Text, Text> {
	// public enum Counters {
	// LINES
	// }

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		long ucount = 0;
		long dcount = 0;
		int gp = 0;
		int cp = 0;
		int tp = 0;
		Map<String, String> immap = new HashMap<String, String>();
		for (Text text : values) {
			String[] tmp = text.toString().split(",");
			if (tmp.length == 4) {
				String imsi = tmp[1];
				ucount += Long.parseLong(tmp[2]);
				dcount += Long.parseLong(tmp[3]);
				immap.put(imsi, imsi);
				gp = 1;
			} else if (tmp.length == 1) {
				String imsi = tmp[0];
				immap.put(imsi, imsi);
				cp = 2;
			}
		}
		tp = gp + cp;
		context.write(key, new Text(ucount + "," + dcount + "," + immap.size()
				+ "," + tp));

	}
}
