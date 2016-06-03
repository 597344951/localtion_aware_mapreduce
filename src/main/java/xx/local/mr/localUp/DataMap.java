/**
 * Project Name:BigCount
 * File Name:FileMap.java
 * Package Name:xx.local.mr.upfile
 * Date:2016年3月28日下午5:06:38
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.localUp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ClassName:FileMap <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年3月28日 下午5:06:38 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class DataMap extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  protected void map(LongWritable key, Text value,
      Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException {
    context.write(new Text(key.toString()), value);
  }

  // public static String queryTime(String time, int gap) throws ParseException
  // {
  // SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
  // SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
  // Date date = sdf.parse(time);
  // long dl = date.getTime();
  // long dd = sdf1.parse(sdf1.format(date)).getTime();
  // long dy = dl % (gap * 60 * 1000);
  // String tid = String.format("%08d", dl - dy - dd);
  // return tid + "_" + sdf1.format(date);
  //
  // }
}
