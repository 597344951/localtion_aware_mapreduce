/**
 * Project Name:BigCount
 * File Name:FileReduce.java
 * Package Name:xx.local.mr.upfile
 * Date:2016年3月30日下午1:58:35
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.localUp;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ClassName:FileReduce <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年3月30日 下午1:58:35 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class DataReduce extends
		TableReducer<Text, Text, ImmutableBytesWritable> {
	private static String gntable = "";
	private static String cstable = "";

	public enum Counters {
		LINES
	}

	@Override
	protected void setup(
			Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		String mytables = conf.get("mytable");
		if (mytables.length() > 0) {
			String[] tbs = mytables.split(",");
			if (tbs.length > 1) {
				if ("1".equals(tbs[0])) {
					gntable = tbs[1];

				}
				if ("2".equals(tbs[0])) {
					cstable = tbs[1];
				}
			}

		}
		// Connection conn = ConnectionFactory.createConnection(conf);
		// if (cstable.length() > 0) {
		// TableName userTable = TableName.valueOf(cstable);
		// Table cstb = conn.getTable(userTable);
		// cstb.setWriteBufferSize(6 * 1024 * 1024);
		// } else {
		// TableName userTable = TableName.valueOf(gntable);
		// Table gntb = conn.getTable(userTable);
		// gntb.setWriteBufferSize(6 * 1024 * 1024);
		// }

	}

	@Override
	protected void reduce(
			Text arg0,
			Iterable<Text> arg1,
			Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
			throws IOException, InterruptedException {
		for (Text text : arg1) {
			String[] strs = text.toString().trim().split("\\t");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			SimpleDateFormat sdfd = new SimpleDateFormat("yyyyMMdd");
			byte[] rowkey = null;
			String[] colum = null;
			String tid = "";
			if (strs.length > 1) {
				String[] ss = strs[0].trim().split("_");
				if (ss.length != 2) {
					return;
				}
				colum = (strs[1].trim() + "1").split("\\|");
				if (colum.length >= 39) {

					if (gntable.length() == 0) {
						context.getCounter(Counters.LINES).increment(1);
						return;
					}
					try {
						Date date = sdf.parse(sdf.format(new Date(Long
								.parseLong(colum[0]) * 1000)));
						long rti = date.getTime()
								- sdfd.parse(sdfd.format(date)).getTime();
						tid = String.format("%08d", rti);

					} catch (Exception e) {
						e.printStackTrace();
					}
					String ci = null;
					String fci = null;
					String rat = null;
					String lastci = null;
					String type = null;
					String trci = null;
					ci = Integer.parseInt("".equals(colum[11]) ? "0"
							: colum[11], 16)
							+ "";
					trci = Long.parseLong("".equals(colum[12]) ? "0"
							: colum[12], 16)
							+ "";
					fci = Integer.parseInt(
							"".equals(colum[colum.length - 2]) ? "0"
									: colum[colum.length - 2], 16)
							+ "";

					rat = colum[7];
					if ("6".equals(rat)) {
						lastci = fci;
						type = "4";
					} else if ("1".equals(rat)) {
						lastci = trci;
						type = "2";
					} else if ("2".equals(rat)) {
						lastci = ci;
						type = "3";
					}
					rowkey = (ss[0] + "_" + tid + "_" + type + "_" + lastci)
							.getBytes();
					ImmutableBytesWritable imrowkey = new ImmutableBytesWritable(
							Bytes.toBytes(gntable));
					Put put = new Put(rowkey);
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("stime"), Bytes.toBytes(sdf
									.format(new Date(
											Long.parseLong(colum[0]) * 1000))));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("etime"), Bytes.toBytes(sdf
									.format(new Date(
											Long.parseLong(colum[1]) * 1000))));
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cnum"),
							Bytes.toBytes(colum[2]));
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("imei"),
							Bytes.toBytes(colum[3]));
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("imsi"),
							Bytes.toBytes(colum[4]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("nsapi"), Bytes.toBytes(colum[5]));
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("apn"),
							Bytes.toBytes(colum[6]));
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rat"),
							Bytes.toBytes(colum[7]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("ggsnip"), Bytes.toBytes(colum[8]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("sgsnip"), Bytes.toBytes(colum[9]));
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lac"),
							Bytes.toBytes(Integer.parseInt(
									"".equals(colum[10]) ? "0" : colum[10], 16)
									+ ""));
					if ("1".equals(rat)) {
						put.addColumn(
								Bytes.toBytes("info"),
								Bytes.toBytes("ci"),
								Bytes.toBytes(Integer.parseInt(
										"".equals(colum[12]) ? "0" : colum[12],
										16) + ""));
					} else if ("2".equals(rat)) {
						put.addColumn(
								Bytes.toBytes("info"),
								Bytes.toBytes("ci"),
								Bytes.toBytes(Integer.parseInt(
										"".equals(colum[11]) ? "0" : colum[11],
										16) + ""));
					}

					// put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ci"),
					// Bytes.toBytes(Integer.parseInt(
					// "".equals(colum[11]) ? "0" : colum[11], 16)
					// + ""));

					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("userip"), Bytes.toBytes(colum[13]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("serip"), Bytes.toBytes(colum[14]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("protype"), Bytes.toBytes(colum[15]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("uport"), Bytes.toBytes(colum[16]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("sport"), Bytes.toBytes(colum[17]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("appid"), Bytes.toBytes(colum[18]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("up_flow_packets"),
							Bytes.toBytes(colum[19]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("down_flow_packets"),
							Bytes.toBytes(colum[20]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("up_bytes"), Bytes.toBytes(colum[21]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("down_bytes"),
							Bytes.toBytes(colum[22]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("end_flag"), Bytes.toBytes(colum[23]));
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("host"),
							Bytes.toBytes(colum[24]));
					String uris = "";
					for (int i = 0; i < colum.length - 39; i++) {
						int index = 25 + i;
						uris += colum[index];
					}
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("uri"),
							Bytes.toBytes(uris));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("response_code"),
							Bytes.toBytes(colum[colum.length - 13]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("response_delay"),
							Bytes.toBytes(colum[colum.length - 12]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("content_len"),
							Bytes.toBytes(colum[colum.length - 11]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("up_speed"),
							Bytes.toBytes(colum[colum.length - 10]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("down_speed"),
							Bytes.toBytes(colum[colum.length - 9]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("charging_id"),
							Bytes.toBytes(colum[colum.length - 8]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("Method"),
							Bytes.toBytes(colum[colum.length - 7]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("Referer"),
							Bytes.toBytes(colum[colum.length - 6]));
					put.addColumn(Bytes.toBytes("info"),
							Bytes.toBytes("UserAgent"),
							Bytes.toBytes(colum[colum.length - 5]));
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dur"),
							Bytes.toBytes(colum[colum.length - 4]));
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("flac"),
							Bytes.toBytes(Integer.parseInt(""
									.equals(colum[colum.length - 3]) ? "0"
									: colum[colum.length - 3], 16)
									+ ""));
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fci"),
							Bytes.toBytes(Integer.parseInt(""
									.equals(colum[colum.length - 2]) ? "0"
									: colum[colum.length - 2], 16)
									+ ""));
					// put.addColumn(Bytes.toBytes("info"),
					// Bytes.toBytes("end_flag"),
					// Bytes.toBytes(colum[38]));
					context.write(imrowkey, put);
				} else {
					colum = strs[1].trim().split(",");
					if (ss[0].length() > 0 && colum.length > 15) {
						if (cstable.length() == 0) {
							context.getCounter(Counters.LINES).increment(1);
							return;
						}
						try {
							Date date = sdf.parse(colum[6]);
							long rti = date.getTime()
									- sdfd.parse(sdfd.format(date)).getTime();
							tid = String.format("%08d", rti);
							rowkey = (ss[0] + "_" + tid).getBytes();
						} catch (Exception e) {
							e.printStackTrace();
						}
						ImmutableBytesWritable imrowkey = new ImmutableBytesWritable(
								Bytes.toBytes(cstable));
						Put put = new Put(rowkey);

						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("cimsi"), Bytes.toBytes(colum[0]));

						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("cdimsi"),
								Bytes.toBytes(colum[1]));
						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("cnum"), Bytes.toBytes(colum[2]));

						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("cdnum"), Bytes.toBytes(colum[3]));
						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("cimei"), Bytes.toBytes(colum[4]));

						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("cdimei"),
								Bytes.toBytes(colum[5]));

						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("stime"), Bytes.toBytes(colum[6]));

						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("etime"), Bytes.toBytes(colum[7]));
						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("flac"), Bytes.toBytes(colum[8]));

						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("fci"), Bytes.toBytes(colum[9]));
						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("clac"), Bytes.toBytes(colum[10]));
						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("cci"), Bytes.toBytes(colum[11]));
						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("cdrid"),
								Bytes.toBytes(colum[12]));
						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("ctype"),
								Bytes.toBytes(colum[13]));
						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("result"),
								Bytes.toBytes(colum[14]));
						put.addColumn(Bytes.toBytes("info"),
								Bytes.toBytes("switch"),
								Bytes.toBytes(colum[15]));
						context.write(imrowkey, put);
					}
				}

			}

		}

	}
}
