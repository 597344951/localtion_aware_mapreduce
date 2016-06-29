/**
 * Project Name:BigCount
 * File Name:RlMap.java
 * Package Name:com.zltel.data.rl
 * Date:2016年3月21日下午4:39:43
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.splitH;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import com.google.protobuf.TextFormat.ParseException;

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
public class HfrlMap extends Mapper<LongWritable, Text, Text, Text> {
	private static int jg = 5;

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		try {
			String line = value.toString();
			String[] tmp = line.split("\\t");
			String[] colum = null;
			String twci = null;
			String thci = null;
			String fci = null;
			String stime = null;
			String imsi = null;
			String up_bytes = null;
			String down_bytes = null;
			String rat = null;
			String lastci = null;
			String type = null;
			if (tmp.length > 1) {
				String[] ss = tmp[0].trim().split("_");
				if (ss.length != 2) {
					return;
				}
				StringBuffer ismibuf = new StringBuffer();
				ismibuf.append(ss[0]);
				colum = (tmp[1].trim() + "1").split("\\|");
				if (colum.length >= 39) {
					stime = ss[1];
					imsi = ismibuf.reverse().toString();
					twci = Integer.parseInt("".equals(colum[11]) ? "0"
							: colum[11], 16)
							+ "";
					thci = Integer.parseInt("".equals(colum[12]) ? "0"
							: colum[12], 16)
							+ "";
					up_bytes = colum[21];
					down_bytes = colum[22];
					fci = Integer.parseInt(
							"".equals(colum[colum.length - 2]) ? "0"
									: colum[colum.length - 2], 16)
							+ "";
					rat = colum[7];

					String etime = null;
					try {
						etime = queryTime(stime, jg);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					if ("6".equals(rat)) {
						lastci = fci;
						type = "4";
					} else if ("1".equals(rat)) {
						lastci = thci;
						type = "3";
					} else if ("2".equals(rat)) {
						lastci = twci;
						type = "2";
					}
					String rkey = etime + "_" + type + "_" + lastci;
					context.write(new Text(rkey), new Text(rat + "," + imsi
							+ "," + up_bytes + "," + down_bytes));

				} else {

					colum = (tmp[1].trim() + "1").split(",");
					stime = ss[1];
					if (colum.length > 12) {
						String cdr_id = colum[12];
						twci = colum[9];
						imsi = ismibuf.reverse().toString();
						if (stime != null && stime.length() > 0) {
							try {
								String etime = queryTime(stime, jg);
								if ("0".equals(cdr_id) || "2".equals(cdr_id)
										|| "4".equals(cdr_id)) {
									type = "2";
									String rkey = etime + "_" + type + "_"
											+ twci;
									context.write(new Text(rkey),
											new Text(imsi));
								} else if ("1".equals(cdr_id)
										|| "3".equals(cdr_id)
										|| "5".equals(cdr_id)) {
									type = "3";
									String rkey = etime + "_" + type + "_"
											+ twci;
									context.write(new Text(rkey),
											new Text(imsi));
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static String queryTime(String time, int gap) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
		Date date = sdf.parse(time);
		long dl = date.getTime();
		long dd = sdf1.parse(sdf1.format(date)).getTime();
		long dy = dl % (gap * 60 * 1000);
		String tid = String.format("%08d", dl - dy - dd);
		return tid + "_" + sdf1.format(date);

	}

}
