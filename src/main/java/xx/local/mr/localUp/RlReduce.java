/**
 * Project Name:BigCount
 * File Name:RlReduce.java
 * Package Name:com.zltel.data.rl
 * Date:2016年3月21日下午4:39:58
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
public class RlReduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
  private static String talName = null;

  @Override
  protected void setup(
      Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    talName = conf.get("mytable");

  }

  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    for (Text value : values) {
      String[] strs = value.toString().split(",");
      if (strs.length == 4) {
        ImmutableBytesWritable table = new ImmutableBytesWritable();
        table.set(Bytes.toBytes(talName));

        Put ingPut = new Put(Bytes.toBytes(key.toString()));
        ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("u"),
            Bytes.toBytes(strs[0]));
        ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("d"),
            Bytes.toBytes(strs[1]));
        ingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("um"),
            Bytes.toBytes(strs[2]));
        context.write(table, ingPut);
      }

    }

  }
}
