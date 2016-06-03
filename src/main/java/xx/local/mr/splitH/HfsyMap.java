/**
 * Project Name:BigCount
 * File Name:RlMap.java
 * Package Name:com.zltel.data.rl
 * Date:2016年3月21日下午4:39:43
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.splitH;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class HfsyMap extends Mapper<LongWritable, Text, Text, Text> {
	public enum Counters {
		LINES
	}

	private Map<String, String> xzqciMap = new HashMap<String, String>();
	private Map<String, String> fgsciMAP = new HashMap<String, String>();
	private Map<String, String> apiMap = new HashMap<String, String>();
	private Map<String, String> hostsMap = new HashMap<String, String>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = null;
		FileSystem fs = null;
		try {
			conf = new Configuration();
			fs = FileSystem.get(conf);
			BufferedReader brt = new BufferedReader(new InputStreamReader(
					fs.open(new Path("/user/zltel/hfqs/conf/xzqfgsci.txt")),
					"UTF-8"));
			String line = null;
			while ((line = brt.readLine()) != null) {
				if (line != null && line.length() > 0) {
					String[] strs = line.trim().split(",");
					if (strs.length == 4) {
						String xzqbh = strs[1].trim();
						String fgsbh = strs[2].trim();
						String ci = strs[3].trim();
						if (xzqbh.matches("\\d+")) {
							xzqciMap.put(ci, xzqbh);
						}
						if (fgsbh.matches("\\d+")) {
							fgsciMAP.put(ci, fgsbh);
						}
					}
				}
			}
			brt.close();

			BufferedReader apiBuf = new BufferedReader(new InputStreamReader(
					fs.open(new Path("/user/zltel/hfqs/conf/APPID.txt")),
					"UTF-8"));
			String aline = null;
			while ((aline = apiBuf.readLine()) != null) {
				if (aline != null && aline.length() > 0) {
					String[] strs = aline.trim().split(",");
					if (strs.length == 2) {
						apiMap.put(strs[0].trim(), strs[1].trim());
					}
				}
			}
			apiBuf.close();
			BufferedReader hostBuf = new BufferedReader(new InputStreamReader(
					fs.open(new Path("/user/zltel/hfqs/conf/host.txt")),
					"UTF-8"));
			String hline = null;
			while ((hline = hostBuf.readLine()) != null) {
				if (hline != null && hline.length() > 0) {
					String[] strs = hline.trim().split(",");
					if (strs.length == 2) {
						hostsMap.put(strs[0].trim(), strs[1].trim());
					}
				}
			}
			hostBuf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHHmmss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("HH_yyyyMMdd");
		String line = value.toString();
		String[] tmp = line.split("\\t");
		String[] colum = null;
		String ci = null;
		String twci = null;
		String trci = null;
		String fci = null;
		String stime = null;
		String imsi = null;
		String up_bytes = null;
		String down_bytes = null;
		String rat = null;
		String lastci = null;
		String type = null;
		String appid = null;
		String hosts = null;
		if (tmp.length > 1) {
			String[] ss = tmp[0].trim().split("_");
			if (ss.length != 2) {
				return;
			}
			StringBuffer ismibuf = new StringBuffer();
			ismibuf.append(ss[0]);
			colum = (tmp[1].trim() + "1").split("\\|");
			if (colum.length >= 38) {
				stime = ss[1];
				if (stime != null && stime.length() > 0) {
					try {
						Date date = sdfh.parse(stime);
						if (date.getTime() % (3600 * 1000) < 60 * 60 * 1000) {
							imsi = ismibuf.reverse().toString();
							twci = Long.parseLong("".equals(colum[11]) ? "0"
									: colum[11], 16)
									+ "";
							trci = Long.parseLong("".equals(colum[12]) ? "0"
									: colum[12], 16)
									+ "";
							up_bytes = colum[21];
							down_bytes = colum[22];
							String ffci = colum[colum.length - 2];
							if (ffci != null && ffci.length() > 2) {
								fci = Long.parseLong(ffci, 16) + "";
							}
							// fci = Integer.parseInt(
							// "".equals(.subSequence(0,
							// colum[colum.length - 2].length() - 2)) ? "0"
							// : colum[colum.length - 2].subSequence(0,
							// colum[colum.length - 2].length() - 2), 16)
							// + "";
							rat = colum[7];
							appid = colum[18];
							hosts = colum[24];
							String etime = sdf2.format(date);
							if ("6".equals(rat)) {
								lastci = fci;
								type = "4";
							} else if ("2".equals(rat)) {
								lastci = twci;
								type = "2";
							} else if ("1".equals(rat)) {
								lastci = trci;
								type = "3";
							}
							if (xzqciMap.get(lastci) != null) {
								context.getCounter(Counters.LINES).increment(1);
								String xzq = xzqciMap.get(lastci);
								String appname = apiMap.get(appid);
								String hostname = hostsMap.get(hosts);

								String rkey = etime
										+ "_"
										+ String.format("%04d",
												Integer.parseInt(xzq));
								context.write(new Text(rkey), new Text(type
										+ "," + imsi + "," + up_bytes + ","
										+ down_bytes + "," + appname + ","
										+ hostname + "," + lastci));
							}
							if (fgsciMAP.get(lastci) != null) {
								context.getCounter(Counters.LINES).increment(1);
								String xzq = fgsciMAP.get(lastci);
								String appname = apiMap.get(appid);
								String hostname = hostsMap.get(hosts);

								String rkey = etime
										+ "_"
										+ String.format("%04d",
												Integer.parseInt(xzq));
								context.write(new Text(rkey), new Text(type
										+ "," + imsi + "," + up_bytes + ","
										+ down_bytes + "," + appname + ","
										+ hostname + "," + lastci));
							}
						}
					} catch (Exception e) {
						e.printStackTrace();

					}
				}
			} else {

				colum = (tmp[1].trim() + "1").split(",");
				stime = ss[1];
				if (colum.length > 12) {
					String cdr_id = colum[12];
					ci = colum[9];
					imsi = ismibuf.reverse().toString();
					if (stime != null && stime.length() > 0) {
						try {
							Date date = sdfh.parse(stime);
							String etime = sdf2.format(date);
							if (date.getTime() % (3600 * 1000) < 60 * 60 * 1000) {

								if ("0".equals(cdr_id) || "2".equals(cdr_id)
										|| "4".equals(cdr_id)) {
									type = "2";
									if (xzqciMap.get(ci) != null) {
										String xzq = xzqciMap.get(ci);
										String rkey = etime
												+ "_"
												+ String.format("%04d",
														Integer.parseInt(xzq));
										context.write(new Text(rkey), new Text(
												imsi + "," + type + "," + ci));
									}
								} else if ("1".equals(cdr_id)
										|| "3".equals(cdr_id)
										|| "5".equals(cdr_id)) {
									type = "3";
									if (fgsciMAP.get(ci) != null) {
										String xzq = fgsciMAP.get(ci);
										String rkey = etime
												+ "_"
												+ String.format("%04d",
														Integer.parseInt(xzq));
										context.write(new Text(rkey), new Text(
												imsi + "," + type + "," + ci));
									}
								}

							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}

			}

		}

	}

	// public static String queryTime(String time, int gap) throws
	// ParseException
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
