package com.zltel.location_aware.userlife.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.common.utils.time.DateTimeUtil;
import com.zltel.location_aware.userlife.bean.CiCountInfo;
import com.zltel.location_aware.userlife.bean.TopPointer;
import com.zltel.location_aware.userlife.service.UserlifeService;

public class UserLifeBinJiangImportMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangImportMap.class);
	private String week;
	private String month;
	private ImmutableBytesWritable table;
	private boolean savePoints = true;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		DateTimeUtil dtu = DateTimeUtil.getInstince();
		week = String.valueOf(dtu.getWeekOfMonth());
		week = "1";
		String SAVE_MONTH = context.getConfiguration().get("SAVE_MONTH");
		if (StringUtil.isNotNullAndEmpty(SAVE_MONTH)) {
			month = SAVE_MONTH;
		} else {
			dtu.setMonthOfNow(-1);// 前一个月
			month = String.valueOf(dtu.getMonth());
		}

		table = new ImmutableBytesWritable();
		Configuration _conf = context.getConfiguration();
		table.set(Bytes.toBytes(_conf.get(TableOutputFormat.OUTPUT_TABLE)));
		String savepoints = _conf.get("savePoints");
		if (StringUtil.isNotNullAndEmpty(savepoints)) {
			savePoints = "0".equals(savepoints.trim()) ? false : true;
		}

		// logout.info("params: week=" + week + ",month=" + month +
		// ",savePoints=" + savepoints);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		String[] strs = value.toString().trim().split("\t");
		// logout.info("输入信息:" + value.toString());
		try {
			if (strs.length >= 2) {
				String[] ss = strs[0].split("_");
				String imsi = ss[0];
				String type = ss[1];
				String content = strs[1];
				// logout.info("imsi:" + imsi + " , type:" + type);
				// logout.info("正文:" + content);
				JSONArray ja = JSON.parseArray(content);
				List<TopPointer> tps = new ArrayList<TopPointer>();
				// context.progress();
				for (int i = 0; i < ja.size() && i < 5; i++) {
					JSONObject jo = ja.getJSONObject(i);

					String _lat = jo.getString("lat");
					String _lng = jo.getString("lng");
					if (StringUtil.isNullOrEmpty(_lat, _lng)) {
						logout.warn("数据不全，跳过:" + jo.toJSONString());
						continue;
					}
					TopPointer tp = JSON.toJavaObject(jo, TopPointer.class);
					tps.add(tp);
					context.progress();
				}
				List<Put> puts = UserlifeService.createPuts(imsi, tps, Integer.valueOf(type), week, month);
				if (puts != null && !puts.isEmpty()) {
					logout.info(imsi + " " + type + " Put 数:" + puts.size());
					for (Put put : puts) {
						context.write(table, put);
					}
				}

			}
		} catch (Exception e) {
			logout.error(e.getMessage() + " " + context, e);
		}
	}

	public static void main(String[] args) {
		CiCountInfo cic = new CiCountInfo();
		List<Map<String, Object>> appTop = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("tag", "App");
		map.put("value", 456);
		appTop.add(map);
		cic.setAppTop(appTop);

		String json = JSON.toJSONString(cic);
		System.out.println(json);
		cic = JSON.toJavaObject(JSON.parseObject(json), CiCountInfo.class);
		System.out.println(cic.getAppTop());
	}
}
