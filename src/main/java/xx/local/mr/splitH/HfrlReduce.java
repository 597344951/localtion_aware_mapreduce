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
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

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
public class HfrlReduce extends MapReduceBase implements
    Reducer<Text, Text, Text, Text> {
  // public enum Counters {
  // LINES
  // }

  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    long ucount = 0;
    long dcount = 0;
    int gp = 0;
    int cp = 0;
    int tp = 0;
    Map<String, String> immap = new HashMap<String, String>();
    while (values.hasNext()) {
      String[] tmp = values.next().toString().split(",");
      if (tmp.length == 4) {
        String imsi = tmp[1];
        ucount += Long.parseLong(tmp[2]);
        dcount += Long.parseLong(tmp[3]);
        immap.put(imsi, imsi);
        gp = 1;
      } else if (tmp.length == 1) {
        // reporter.getCounter(Counters.LINES).increment(1);
        // output.(Counters.LINES).increment(1);
        String imsi = tmp[0];
        immap.put(imsi, imsi);
        cp = 2;
      }
    }
    tp = gp + cp;
    output.collect(key, new Text(ucount + "," + dcount + "," + immap.size()
        + "," + tp));

  }
}
