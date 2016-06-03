/**
 * Project Name:BigCount
 * File Name:SyReduce.java
 * Package Name:xx.local.mr.localUp
 * Date:2016年4月7日下午7:14:27
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.localUp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ClassName:SyReduce <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年4月7日 下午7:14:27 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class SyReduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
	private static String talName = null;

	@Override
	protected void setup(Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		talName = conf.get("mytable");

	}

	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String[] strs = value.toString().split(",");

			if (strs.length >= 12) {
				ImmutableBytesWritable table = new ImmutableBytesWritable();
				table.set(Bytes.toBytes(talName));
				Put ingPut = new Put(Bytes.toBytes(key.toString()));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("twog_cell_count"), Bytes.toBytes(strs[0]));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("twog_user_count"), Bytes.toBytes(strs[3]));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("twog_up_count"), Bytes.toBytes(strs[1]));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("twog_down_count"), Bytes.toBytes(strs[2]));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("threeg_cell_count"), Bytes.toBytes(strs[4]));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("threeg_user_count"), Bytes.toBytes(strs[7]));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("threeg_up_count"), Bytes.toBytes(strs[5]));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("threeg_down_count"), Bytes.toBytes(strs[6]));

				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fourg_cell_count"), Bytes.toBytes(strs[8]));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fourg_user_count"), Bytes.toBytes(strs[11]));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fourg_up_count"), Bytes.toBytes(strs[9]));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fourg_down_count"), Bytes.toBytes(strs[10]));

				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("total_cell_count"), Bytes
						.toBytes((Long.parseLong(strs[0]) + Long.parseLong(strs[4]) + Long.parseLong(strs[8])) + ""));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("total_user_count"), Bytes
						.toBytes((Long.parseLong(strs[3]) + Long.parseLong(strs[7]) + Long.parseLong(strs[11])) + ""));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("total_up_count"), Bytes
						.toBytes((Long.parseLong(strs[1]) + Long.parseLong(strs[5]) + Long.parseLong(strs[9])) + ""));
				ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("total_down_count"), Bytes
						.toBytes((Long.parseLong(strs[2]) + Long.parseLong(strs[6]) + Long.parseLong(strs[10])) + ""));

				for (int i = 12; i < strs.length; i++) {
					String str = strs[i];
					String[] st = str.split("\\#\\*\\#");
					ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(st[0] + st[1]), Bytes.toBytes(st[2]));
				}
				context.write(table, ingPut);

			}
		}

	}
}
