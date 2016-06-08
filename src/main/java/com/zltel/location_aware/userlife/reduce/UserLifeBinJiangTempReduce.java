package com.zltel.location_aware.userlife.reduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.main.UserLifeBinJiangMain;
import com.zltel.location_aware.userlife.service.UserlifeService;

/**
 * 临时 reduce 为了研究单纯数据是否会导致内存溢出问题
 * 
 * @author Wangch
 *
 */
public class UserLifeBinJiangTempReduce extends Reducer<Text, Text, Text, Text> {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangTempReduce.class);

	public static Map<String, Map<String, Object>> _cacheMap = null;

	String timerange = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.
	 * Reducer.Context)
	 */
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		timerange = context.getConfiguration().get("timerange");

		if (StringUtil.isNotNullAndEmpty(timerange) && StringUtil.isNum(timerange)) {
			UserlifeService.TIMERANGE = Integer.valueOf(timerange);
		}

		if (_cacheMap == null) {
			_cacheMap = new HashMap<String, Map<String, Object>>();
			// 读取 gis 数据
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = null;
			try {
				// 输出全部文件内容
				in = fs.open(new Path(UserLifeBinJiangMain.GISPATH));
				// 用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上
				ByteArrayOutputStream bo = new ByteArrayOutputStream();
				IOUtils.copyBytes(in, bo, 50, false);
				String txt = new String(bo.toByteArray(), "UTF-8");
				if (StringUtil.isNotNullAndEmpty(txt)) {
					String[] lines = txt.split("\r\n");
					logout.info("读取 到gis 行数: " + lines.length);
					for (String line : lines) {
						String[] cs = line.split("\\|\\|");
						String ci = cs[5];
						String lac = cs[4];
						String lat = cs[9];
						String lng = cs[9];
						String cell = cs[7];

						Map<String, Object> _map = new HashMap<String, Object>();
						_map.put("CI", ci);
						_map.put("CELL", cell);
						_map.put("LAT", lat);
						_map.put("LNG", lng);
						_map.put("LAC", lac);
						_cacheMap.put(ci, _map);
						// logout.info(" 读取到的数据: " + _map);
					}
				}
			} finally {
				IOUtils.closeStream(in);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> iterator, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		logout.info("start reduce----------------------------------");

		String _imsi = key.toString();
		List<Pointer> points = new ArrayList<Pointer>();
		for (Text value : iterator) {
			try {
				context.progress();
				String json = value.toString();
				Pointer p = JSON.toJavaObject(JSON.parseObject(json), Pointer.class);
				if (!p.avaliable()) {
					logout.info(_imsi + ",跳过不合法数据:" + json);
					continue;
				}
				p = check(p);
				if (p != null) {
					points.add(p);
					// logout.info("转换后的数据:" + JSON.toJSONString(p));
				} else {
					logout.warn("没有识别到数据! " + json);
				}
			} catch (Exception e) {
				logout.error(e.getMessage(), e);
			}
		}
		try {
			if (!points.isEmpty()) {
				// String imsi = key.toString();
				logout.info(key.toString() + "  输入点：" + points.size());
				context.progress();
			}

		} catch (Exception e) {
			logout.error("reduce error: " + e.getMessage(), e);
			// throw new IOException(e);
		}
	}

	public Pointer check(Pointer pointer) throws ClassNotFoundException, SQLException {
		Map<String, Object> map = _cacheMap.get(pointer.getCi().trim());
		if (map != null) {
			String lat = String.valueOf(map.get("LAT"));
			String lng = String.valueOf(map.get("LNG"));
			String cell = String.valueOf(map.get("CELL"));
			pointer.setLat(lat);
			pointer.setLng(lng);
			pointer.setCell(cell);
			return pointer;
		}
		return null;
	}

}
