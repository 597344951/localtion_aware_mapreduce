package com.zltel.location_aware.userlife.reduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
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
import com.zltel.location_aware.userlife.bean.TopPointer;
import com.zltel.location_aware.userlife.main.UserLifeBinJiangMain;
import com.zltel.location_aware.userlife.service.UserlifeService;

public class UserLifeBinJiangReduce extends Reducer<Text, Text, Text, Text> {
	/**
	 * 最大 多少个点
	 */
	public static int MAX_POINT_COUNT = 50000;
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangReduce.class);

	public static Map<String, Map<String, Object>> _cacheMap = null;
	public static Map<String, Map<String, Object>> _imeiMap = null;

	private String OUT_FILE_PATH = null;
	String timerange = null;
	/** 上一次合并后的大小 **/
	private long lastcount = 0;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.
	 * Reducer.Context)
	 */
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		String keep_points = context.getConfiguration().get("KEEP_POINTS");
		if (StringUtil.isNotNullAndEmpty(keep_points)) {
			UserlifeService.KEEP_POINTS = "0".equals(keep_points.trim()) ? false : true;
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
						String Province = cs[0];
						String city = cs[1];
						String community = cs[2];
						String community_type = cs[3];

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

						_map.put("PROVINCE", Province);
						_map.put("CITY", city);
						_map.put("COMMUNITY", community);
						_map.put("COMMUNITY_TYPE", community_type);
						_cacheMap.put(ci, _map);
						// logout.info(" 读取到的数据: " + _map);
					}
				}
			} finally {
				IOUtils.closeStream(in);
			}
		}
		if (_imeiMap == null) {
			_imeiMap = new HashMap<String, Map<String, Object>>();
			// 读取 gis 数据
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = null;
			try {
				// 输出全部文件内容
				in = fs.open(new Path(UserLifeBinJiangMain.IMEI_PATH));
				// 用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上
				ByteArrayOutputStream bo = new ByteArrayOutputStream();
				IOUtils.copyBytes(in, bo, 50, false);
				String txt = new String(bo.toByteArray(), "UTF-8");
				if (StringUtil.isNotNullAndEmpty(txt)) {
					String[] lines = txt.split("\r\n");
					for (String line : lines) {
						String[] cs = line.split("\\|\\|");
						if (cs.length < 3) {
							continue;
						}
						String imsi = cs[0];
						String tel_fac = cs[1];
						String tel_type = cs[2];

						Map<String, Object> _map = new HashMap<String, Object>();
						_map.put("imsi", imsi);
						_map.put("tel_fac", tel_fac);
						_map.put("tel_type", tel_type);

						_imeiMap.put(imsi, _map);
					}
				}
			} finally {
				IOUtils.closeStream(in);
			}
		}

		OUT_FILE_PATH = context.getConfiguration().get("result_out_path");
		OUT_FILE_PATH = OUT_FILE_PATH + "jobLog.txt";

		String pcl = context.getConfiguration().get("POINTER_CALC_COUNT_LIMIT");
		if (StringUtil.isNum(pcl)) {
			int _pcl = Integer.valueOf(pcl);
			if (_pcl > 10000) {
				MAX_POINT_COUNT = _pcl;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.
	 * Reducer.Context)
	 */
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

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

		long _time = System.currentTimeMillis();
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
		// 合并 数据 时间
		_time = System.currentTimeMillis() - _time;
		try {
			if (!_points.isEmpty()) {
				int sz = _points.size();
				if (jump) {
					writeMsg(_imsi + " too big , count:" + (sz + jumpCount), context);
				}
				String imsi = key.toString();
				List<Pointer> points = new ArrayList<Pointer>(_points);
				List<Pointer> pts = UserlifeService.prepareInit(points);
				totalCount = pts.size();// 更改为合并后的点数目
				logout.info(imsi + "  聚合+过滤后的点：" + pts.size());
				String _k = imsi + "_" + UserlifeService.TYPE_HOME;
				List<TopPointer> home_tps = UserlifeService.analyseHome(pts, totalCount, _time);
				context.write(new Text(_k), new Text(JSON.toJSONString(home_tps)));
				context.progress();
				_k = imsi + "_" + UserlifeService.TYPE_WORK;
				List<TopPointer> work_tps = UserlifeService.analyseWork(pts, totalCount, _time);
				context.write(new Text(_k), new Text(JSON.toJSONString(work_tps)));
				context.progress();
				_k = imsi + "_" + UserlifeService.TYPE_FUN;
				List<TopPointer> fun_tps = UserlifeService.analyseFun(pts, totalCount, _time);
				context.write(new Text(_k), new Text(JSON.toJSONString(fun_tps)));
				context.progress();
			}

		} catch (Exception e) {
			logout.error("reduce error: " + e.getMessage(), e);
			throw new IOException(e);
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
	 * @return 设置前一个点
	 */
	private void checkMerg(Pointer p, TreeSet<Pointer> points) {
		if (p == null) {
			return;
		}
		// points.add(p);
		// 队列达到最大值时 合并
		// int ps = points.size();
		// 添加超过 X个点 或者 处于临界状态
		// if (ps - lastcount > 3000 || ps >= MAX_POINT_COUNT) {
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

	/**
	 * 输出 错误信息
	 * 
	 * @param msg
	 * @param context
	 * @throws IOException
	 */
	public void writeMsg(String msg, Reducer<Text, Text, Text, Text>.Context context) throws IOException {
		try {
			FileSystem fs = FileSystem.get(URI.create(OUT_FILE_PATH), context.getConfiguration());
			Path op = new Path(OUT_FILE_PATH);
			OutputStream out = null;
			if (fs.exists(op)) {
				out = fs.append(op);
			} else {
				// 创建文件
				out = fs.create(op);
			}
			try {
				if (out != null) {
					out.write(msg.concat("  \r\n").getBytes());
					out.flush();
				}
			} finally {
				try {
					if (out != null)
						out.close();
				} catch (Exception e) {
					logout.error("", e);
				}
			}
		} catch (Exception e) {
			logout.error("", e);
		}
	}
}
