package com.zltel.location_aware.userlife.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.bean.TopPointer;
import com.zltel.location_aware.userlife.doublesort.DoubleSortKey;
import com.zltel.location_aware.userlife.service.UserlifeService;
import com.zltel.location_aware.userlife.utils.LogUtil;

/**
 * 用户 标签 测试数据 生成
 * 
 * @author Wangch
 *
 */
public class UserLifeBJ_DoubleSort_Reduce extends Reducer<DoubleSortKey, Text, Text, Text> {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBJ_DoubleSort_Reduce.class);
	// 引用 其它reduce 的方法
	private UserLifeBinJiangReduce bjr = null;

	/** 调试信息记录位置 **/
	private String DEBUG_FILE;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(DoubleSortKey key, Iterable<Text> values,
			Reducer<DoubleSortKey, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		List<Pointer> _points = new ArrayList<Pointer>();
		boolean jump = false;
		long jumpCount = 0;
		long totalCount = 0;
		String imsiStr = key.getFirstKey().toString();
		String _imsi = imsiStr.split("_")[0];
		String typestr = imsiStr.split("_")[1];
		int _type = Integer.valueOf(typestr);

		// home
		long _time = System.currentTimeMillis();
		Pointer _p = null;
		long _wc = 0l;
		long _rc = 0l;
		for (Text value : values) {
			try {
				String json = value.toString();
				Pointer p = JSON.toJavaObject(JSON.parseObject(json), Pointer.class);
				UserlifeService.markTags(p);
				context.progress();
				if (!p.avaliable() || StringUtil.isNullOrEmpty(p.getTag())) {
					continue;
				}
				p = bjr.check(p);
				if (p != null) {
					totalCount++;
					if (!jump && bjr.MAX_POINT_COUNT != -1 && _points.size() > bjr.MAX_POINT_COUNT) {
						jump = true;
					}
					if (!jump) {
						checkMerg(p, _points);
						// logout.info("转换后的数据:" + JSON.toJSONString(p));
					} else {
						jumpCount++;
					}
				} else {
					// logout.info("没有识别到数据! " + json);
				}
				if (_p != null && p != null) {
					if (_p._stime().getTime() > p._stime().getTime()) {
						_wc++;
					} else {
						_rc++;
					}
				}
				_p = p;
			} catch (Exception e) {
				logout.error(e.getMessage(), e);
			}
		}
		logout.info("Reduce : " + key.getFirstKey().toString() + " -> " + _points.size() + "/" + totalCount + " 正确次数："
				+ _rc + ",错误次数" + _wc);
		if (_wc > 0) {
			LogUtil.error("Reduce : " + key.getFirstKey().toString() + " -> " + _points.size() + "/" + totalCount
					+ " 正确次数：" + _rc + ",错误次数" + _wc, DEBUG_FILE, context.getConfiguration());
		}
		// 合并 数据 时间
		_time = System.currentTimeMillis() - _time;
		try {
			if (!_points.isEmpty()) {
				int sz = _points.size();
				if (jump) {
					this.bjr.writeMsg(_imsi + " too big , count:" + (sz + jumpCount), context.getConfiguration());
					LogUtil.error(_imsi + " too big , count:" + (sz + jumpCount), DEBUG_FILE,
							context.getConfiguration());
				}
				String imsi = _imsi;
				UserlifeService.sortPointers(_points);
				final List<Pointer> pts = _points;
				totalCount = pts.size();// 更改为合并后的点数目
				logout.info(imsi + "  聚合+过滤后的点：" + pts.size());

				boolean old = false;

				if (old) {
					if (_type == UserlifeService.TYPE_HOME) {
						String _k = imsi + "_" + UserlifeService.TYPE_HOME;
						List<TopPointer> home_tps = UserlifeService.analyseHome(pts, totalCount, _time);
						context.write(new Text(_k), new Text(JSON.toJSONString(home_tps)));
						context.progress();
					} else if (_type == UserlifeService.TYPE_WORK) {
						String _k = imsi + "_" + UserlifeService.TYPE_WORK;
						List<TopPointer> work_tps = UserlifeService.analyseWork(pts, totalCount, _time);
						context.write(new Text(_k), new Text(JSON.toJSONString(work_tps)));
						context.progress();
					} else if (_type == UserlifeService.TYPE_FUN) {
						String _k = imsi + "_" + UserlifeService.TYPE_FUN;
						List<TopPointer> fun_tps = UserlifeService.analyseFun(pts, totalCount, _time);
						context.write(new Text(_k), new Text(JSON.toJSONString(fun_tps)));
						context.progress();
					}
				} else {
					String dis = null;
					if (_type == UserlifeService.TYPE_HOME) {
						dis = "home";
					} else if (_type == UserlifeService.TYPE_WORK) {
						dis = "work";
					} else {
						dis = "fun";
					}
					List<TopPointer> tops = UserlifeService.analyseByGridCluster(_type, key, pts, totalCount, _time);

					context.write(key.getFirstKey(), new Text(toJsonArrayStr(tops, dis)));
				}
			}

		} catch (Exception e) {
			logout.error("reduce error: " + e.getMessage(), e);
			throw new IOException(e);
		} finally {
		}
	}

	private String toJsonArrayStr(List<TopPointer> tops, String dis) {
		StringBuffer sb = new StringBuffer();
		boolean isf = true;
		// sb.append("\"" + dis + "\":");
		sb.append("[");
		for (TopPointer tp : tops) {
			if (isf) {
				isf = false;
			} else {
				sb.append(",");
			}
			sb.append(JSON.toJSON(tp));
		}
		sb.append("]");

		return sb.toString();
	}

	private void checkMerg(Pointer p, List<Pointer> _points) {
		if (p == null) {
			return;
		}
		UserlifeService.mergePointers(_points, p);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.
	 * Reducer.Context)
	 */
	@Override
	protected void setup(Reducer<DoubleSortKey, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		bjr = new UserLifeBinJiangReduce();
		bjr.initParams(context.getConfiguration());
		DEBUG_FILE = context.getConfiguration().get("result_out_path");
		if (StringUtil.isNullOrEmpty(DEBUG_FILE)) {
			DEBUG_FILE = "userlife/";
		}
		DEBUG_FILE = DEBUG_FILE + "debugLog.txt";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.
	 * Reducer.Context)
	 */
	@Override
	protected void cleanup(Reducer<DoubleSortKey, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}

}
