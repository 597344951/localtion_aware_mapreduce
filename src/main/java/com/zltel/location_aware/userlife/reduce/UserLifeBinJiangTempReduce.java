package com.zltel.location_aware.userlife.reduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

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
	/**
	 * 最大 多少个点
	 */
	public static int MAX_POINT_COUNT = 50000;
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangTempReduce.class);

	public static Map<String, Map<String, Object>> _cacheMap = null;
	/** 上一次合并后的大小 **/
	private long lastcount = 0;
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
			// UserlifeService.TIMERANGE = Integer.valueOf(timerange);
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
						String lng = cs[8];// 119.636602
						String lat = cs[9]; // 30
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
		// List<Pointer> points = new ArrayList<Pointer>();
		TreeSet<Pointer> _points = new TreeSet<Pointer>(UserlifeService.getComparator());
		boolean jump = false;
		long jumpCount = 0;
		long totalCount = 0;
		for (Text value : iterator) {
			try {
				String json = value.toString();
				Pointer p = JSON.toJavaObject(JSON.parseObject(json), Pointer.class);
				context.progress();
				if (!p.avaliable()) {
					logout.info(_imsi + ",跳过不合法数据:" + json);
					continue;
				}
				p = check(p);
				if (p != null) {
					totalCount++;
					if (!jump && MAX_POINT_COUNT != -1 && _points.size() > MAX_POINT_COUNT) {
						jump = true;
					}
					if (!jump) {
						checkMerg(p, _points);
						// logout.info("转换后的数据:" + JSON.toJSONString(p));
					} else {
						jumpCount++;
					}
				} else {
					logout.info("没有识别到数据! " + json);
				}
			} catch (Exception e) {
				logout.error(e.getMessage(), e);
			}
		}
		try {
			if (!_points.isEmpty()) {
				List<Pointer> points = new ArrayList<Pointer>(_points);
				List<Pointer> pts = UserlifeService.prepareInit(points);
				for (Pointer _p : pts) {
					// 计算分数
					long score = _p.calcScore(UserlifeService.WORTH);
					_p.setScore(score);
					_p.setTime(_p.timeRange());
					context.write(key, new Text(JSON.toJSONString(_p)));
				}
			}
		} catch (Exception e) {
			logout.error("reduce error: " + e.getMessage(), e);
			// throw new IOException(e);
		}
	}

	/**
	 * 判断点是否 可以合并,如果可以 则合并,不可以则保存
	 * 
	 * @param bfPointer
	 *            前一个点
	 * @param p
	 *            当前点
	 * @param points
	 *            点 集合
	 * @param lastcount
	 *            上一次合并点数
	 * @return 设置前一个点
	 */
	private void checkMerg(Pointer p, TreeSet<Pointer> points) {
		if (p == null) {
			return;
		}
		// points.add(p);
		// 队列达到最大值时 合并
		int ps = points.size();
		// 添加超过 X个点 或者 处于临界状态
		// if (ps - lastcount > 6000 || ps >= MAX_POINT_COUNT) {
		UserlifeService.mergePointers(points, p);
		lastcount = points.size();
		// }
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
