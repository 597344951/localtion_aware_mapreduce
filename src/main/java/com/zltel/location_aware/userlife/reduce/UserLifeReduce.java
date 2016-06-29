package com.zltel.location_aware.userlife.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.bean.TopPointer;
import com.zltel.location_aware.userlife.service.UserlifeService;
import com.zltel.location_aware.userlife.utils.PointsLocationCheckUtil;

public class UserLifeReduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
	private static Logger logout = LoggerFactory.getLogger(UserLifeReduce.class);
	String week = null;
	String month = null;
	String timerange = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.
	 * Reducer.Context)
	 */
	@Override
	protected void setup(Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
			throws IOException, InterruptedException {
		timerange = context.getConfiguration().get("timerange");
		week = context.getConfiguration().get("week");
		month = context.getConfiguration().get("month");

		if (StringUtil.isNotNullAndEmpty(timerange) && StringUtil.isNum(timerange)) {
			// UserlifeService.TIMERANGE = Integer.valueOf(timerange);
		}
		logout.info("params, 合并时间间隔:" + timerange + " ,分值:" + UserlifeService.WORTH);
		logout.info("params,输入week:" + week + " ,month:" + month);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	protected void reduce(Text key, Iterable<Text> iterator,
			Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
			throws IOException, InterruptedException {
		try {
			String _imsi = key.toString();
			List<Pointer> points = new ArrayList<Pointer>();
			for (Text value : iterator) {
				try {
					String json = value.toString();
					Pointer p = JSON.toJavaObject(JSON.parseObject(json), Pointer.class);
					if (!p.avaliable()) {
						logout.info(_imsi + ",跳过不合法数据:" + json);
						continue;
					}
					p = PointsLocationCheckUtil.check(p);
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
			if (!points.isEmpty()) {
				String imsi = key.toString();
				logout.info(key.toString() + "  输入点：" + points.size());
				// logout.info(imsi + "\n" + JSON.toJSONString(points));
				List<Pointer> pts = UserlifeService.prepareInit(points);
				logout.info(imsi + "  聚合+过滤后的点：" + pts.size());
				analyseHome(imsi, pts, context);
				analyseWork(imsi, pts, context);
				analyseFun(imsi, pts, context);
			}

		} catch (Exception e) {
			logout.error("reduce error: " + e.getMessage(), e);
			throw new IOException(e);
		}
	}

	private void analyseHome(String imsi, List<Pointer> pts,
			Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws Exception {
		List<TopPointer> tps = UserlifeService.analyseHome(pts, 0);
		List<Put> puts = UserlifeService.createPuts(imsi, tps, UserlifeService.TYPE_HOME, week, month);
		if (puts != null && !puts.isEmpty()) {
			logout.info("获取到居住地点,IMSI:" + imsi + " ,size:" + puts.size());
			for (Put put : puts) {
				context.write(null, put);
			}
		} else {
			logout.error("没有获取到居住地点，IMSI:" + imsi + " ,points:" + JSON.toJSONString(pts));
		}
	}

	private void analyseWork(String imsi, List<Pointer> pts,
			Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws Exception {
		List<TopPointer> tps = UserlifeService.analyseWork(pts, 0);
		List<Put> puts = UserlifeService.createPuts(imsi, tps, UserlifeService.TYPE_WORK, week, month);
		if (puts != null && !puts.isEmpty()) {
			logout.info("获取到工作地点,IMSI:" + imsi + " ,size:" + puts.size());
			for (Put put : puts) {
				context.write(null, put);
			}
		} else {
			logout.error("没有获取到工作地点，IMSI:" + imsi + " ,points:" + JSON.toJSONString(pts));
		}
	}

	private void analyseFun(String imsi, List<Pointer> pts,
			Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws Exception {
		List<TopPointer> tps = UserlifeService.analyseFun(pts, 0);
		List<Put> puts = UserlifeService.createPuts(imsi, tps, UserlifeService.TYPE_FUN, week, month);
		if (puts != null && !puts.isEmpty()) {
			logout.info("获取到娱乐地点,IMSI:" + imsi + " ,size:" + puts.size());
			for (Put put : puts) {
				context.write(null, put);
			}
		} else {
			logout.error("没有获取到工作地点，IMSI:" + imsi + " ,points:" + JSON.toJSONString(pts));
		}
	}

	public static void main(String[] args) {
		String s = "{\"ci\":\"65535\",\"etime\":\"20160505145945\",\"lac\":\"55058\",\"lat\":0,\"lng\":0,\"nettype\":\"2/3\",\"score\":0,\"source\":\"cs\",\"stime\":20160505145945,\"timecost\":1000,\"weekday\":true,\"weight\":0}";

		Pointer p = JSON.toJavaObject(JSON.parseObject(s), Pointer.class);
		System.out.println(p);
		if (!p.avaliable() || p._stime() == null || p._etime() == null) {
			logout.info("跳过不合法数据:");
		}
	}
}
