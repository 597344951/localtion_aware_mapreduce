/**
 * Project Name:BigCount
 * File Name:RlMap.java
 * Package Name:com.zltel.data.rl
 * Date:2016年3月21日下午4:39:43
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.localUp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ClassName:RlMap <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年3月21日 下午4:39:43 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class RlMap extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] str = value.toString().split("\\t");
    if (str.length == 2) {
      context.write(new Text(str[0]), new Text(str[1]));
    }

  }

}
