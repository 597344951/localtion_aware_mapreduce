/**
 * Project Name:BigCount
 * File Name:RlReduce.java
 * Package Name:com.zltel.data.rl
 * Date:2016年3月21日下午4:39:58
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.splitH;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class HfsyReduce extends Reducer<Text, Text, Text, Text> {
	private Map<String, Integer> xzqMap = new HashMap<String, Integer>();
	private Map<String, Integer> fgsMap = new HashMap<String, Integer>();

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			BufferedReader brt = new BufferedReader(new InputStreamReader(
					fs.open(new Path("/user/zltel/hfqs/conf/xzqfgsci.txt")),
					"UTF-8"));
			String line = null;
			while ((line = brt.readLine()) != null) {
				if (line != null && line.length() > 0) {
					String[] strs = line.trim().split(",");
					if (strs.length == 4) {
						String type = strs[0].trim();
						String xzqbh = String.format("%04d",
								Integer.parseInt(strs[1].trim()));
						String fgsbh = String.format("%04d",
								Integer.parseInt(strs[2].trim()));
						if (xzqbh.matches("\\d+")) {
							String key = type + xzqbh;
							if (key != null && key.length() > 0) {
								if (xzqMap.get(key) == null) {
									xzqMap.put(key, 1);
								} else {
									int nu = xzqMap.get(key) + 1;
									xzqMap.put(key, nu);
								}
							}
						}
						if (fgsbh.matches("\\d+")) {
							String key = type + fgsbh;
							if (key != null && key.length() > 0) {
								if (fgsMap.get(key) == null) {
									fgsMap.put(key, 1);
								} else {
									int nu = fgsMap.get(key) + 1;
									fgsMap.put(key, nu);
								}
							}
						}
					}
				}
			}
			brt.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		long twu = 0;
		long twd = 0;
		long tru = 0;
		long trd = 0;
		long fru = 0;
		long frd = 0;

		String xzq = key.toString().split("_")[2];
		String twci = "0";
		if (xzqMap.get("2" + xzq) != null) {
			twci = xzqMap.get("2" + xzq) + "";
		} else if (fgsMap.get("2" + xzq) != null) {
			twci = fgsMap.get("2" + xzq) + "";
		}

		String thci = "0";
		if (xzqMap.get("3" + xzq) != null) {
			thci = xzqMap.get("3" + xzq) + "";
		} else if (fgsMap.get("3" + xzq) != null) {
			thci = fgsMap.get("3" + xzq) + "";
		}
		String frci = "0";
		if (xzqMap.get("4" + xzq) != null) {
			frci = xzqMap.get("4" + xzq) + "";
		} else if (fgsMap.get("4" + xzq) != null) {
			frci = fgsMap.get("4" + xzq) + "";
		}

		// Map<String, String> immap = new HashMap<String, String>();
		Map<String, String> twuserMap = new HashMap<String, String>();
		Map<String, String> truserMap = new HashMap<String, String>();
		Map<String, String> fruserMap = new HashMap<String, String>();
		// Map<String, Long> hostMap = new HashMap<String, Long>();
		Map<String, Long> appMap = new HashMap<String, Long>();
		Map<String, Long> appllMap = new HashMap<String, Long>();
		// Map<String, String> twciMap = new HashMap<String, String>();
		// Map<String, String> trciMap = new HashMap<String, String>();
		// Map<String, String> frciMap = new HashMap<String, String>();
		for (Text text : values) {

			String[] tmp = text.toString().split(",");
			if (tmp.length == 7) {
				String type = tmp[0];
				String imsi = tmp[1];
				String up_bytes = tmp[2];
				String down_bytes = tmp[3];
				String appid = tmp[4];
				String hosts = tmp[5];
				// String lastci = tmp[6];
				if ("2".equals(type)) {
					twuserMap.put(imsi, imsi);
					twu += Long.parseLong(up_bytes);
					twd += Long.parseLong(down_bytes);
					// twciMap.put(lastci, lastci);
				} else if ("3".equals(type)) {
					truserMap.put(imsi, imsi);
					tru += Long.parseLong(up_bytes);
					trd += Long.parseLong(down_bytes);
					// trciMap.put(lastci, lastci);
				} else if ("4".equals(type)) {
					fruserMap.put(imsi, imsi);
					fru += Long.parseLong(up_bytes);
					frd += Long.parseLong(down_bytes);
					// frciMap.put(lastci, lastci);
				}
				long tt = Long.parseLong("".equals(up_bytes) ? "0" : up_bytes)
						+ Long.parseLong("".equals(down_bytes) ? "0"
								: down_bytes);
				if (hosts != null && !"null".equals(hosts)) {
					if (appMap.get(hosts) == null) {
						appMap.put(hosts, (long) 1);
					} else {
						long hostnum = appMap.get(hosts) + 1;
						appMap.put(hosts, hostnum);
					}
					if (appllMap.get(hosts) == null) {
						appllMap.put(hosts, tt);
					} else {
						long llcount = appllMap.get(hosts) + tt;
						appllMap.put(hosts, llcount);
					}

				}
				if (appid != null && !"null".equals(appid)) {
					if (appMap.get(appid) == null) {
						appMap.put(appid, (long) 1);
					} else {
						long appnum = appMap.get(appid) + 1;
						appMap.put(appid, appnum);
					}
					if (appllMap.get(appid) == null) {
						appllMap.put(appid, tt);
					} else {
						long llcount = appllMap.get(appid) + tt;
						appllMap.put(appid, llcount);
					}

				}
			} else if (tmp.length == 3) {
				String imsi = tmp[0];
				String type = tmp[1];
				// String ci = tmp[2];
				if ("2".equals(type)) {
					twuserMap.put(imsi, imsi);
					// twciMap.put(ci, ci);
				} else if ("3".equals(type)) {
					truserMap.put(imsi, imsi);
					// trciMap.put(ci, ci);
				} else if ("4".equals(type)) {
					fruserMap.put(imsi, imsi);
				}
			}
		}
		StringBuffer strBuf = new StringBuffer();
		strBuf.append(twci + "," + twu + "," + twd + "," + twuserMap.size()
				+ "," + thci + "," + tru + "," + trd + "," + truserMap.size()
				+ "," + frci + "," + fru + "," + frd + "," + fruserMap.size());
		// List<String> hostList = queryTop(hostMap);
		// for (int i = 0; i < hostList.size() && i < 10; i++) {
		// strBuf.append(",host#*#" + (i + 1) + "#*#" + hostList.get(i));
		// }
		// output.collect(key, new Text(strBuf.toString()));
		List<String> appList = queryTop(appMap);
		for (int i = 0; i < appList.size() && i < 10; i++) {
			strBuf.append(",app#*#" + (i + 1) + "#*#" + appList.get(i));
		}
		List<String> appllList = queryTop(appllMap);
		for (int i = 0; i < appllList.size() && i < 10; i++) {
			strBuf.append(",appll#*#" + (i + 1) + "#*#" + appllList.get(i));
		}

		context.write(key, new Text(strBuf.toString()));

	}

	private List<String> queryTop(Map<String, Long> map) {
		List<String> list = new ArrayList<String>();

		for (Map.Entry<String, Long> entry : map.entrySet()) {
			list.add(entry.getKey() + "#*#" + entry.getValue());
		}

		Collections.sort(list, new Comparator<String>() {

			public int compare(String o1, String o2) {
				Long s1 = Long.parseLong(o1.split("\\#\\*\\#")[1]);
				Long s2 = Long.parseLong(o2.split("\\#\\*\\#")[1]);
				return s2.compareTo(s1);
			}
		});
		return list;

	}
}
