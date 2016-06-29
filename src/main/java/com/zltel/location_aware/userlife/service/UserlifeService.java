package com.zltel.location_aware.userlife.service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.common.utils.hbase.HBaseUtil;
import com.zltel.common.utils.hbase.HBaseUtil.ColumnData;
import com.zltel.common.utils.reflect.ReflectUtil;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.common.utils.time.DateTimeUtil;
import com.zltel.dbscan.bean.Point;
import com.zltel.dbscan.service.Dbscan;
import com.zltel.location_aware.userlife.bean.CiCountInfo;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.bean.TopPointer;
import com.zltel.location_aware.userlife.reduce.UserLifeBinJiangReduce;

/**
 * 服务提供类
 * 
 * @author Wangch
 *
 */
public class UserlifeService {
	private static final Logger logout = LoggerFactory.getLogger(UserlifeService.class);

	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	public static final SimpleDateFormat daysdf = new SimpleDateFormat("yyyyMMdd");
	public static final SimpleDateFormat timesdf = new SimpleDateFormat("HHmmss");
	/** 合并 点的时间间隔 **/
	private static int MERGETIME = 60 * 1000;
	/** 聚合时间的 临界值 ,低于此事件的将被过滤 **/
	private static int TIMERANGE = 1 * 1000;
	public static final int WORTH = 1;

	public static final int TYPE_HOME = 2;
	public static final int TYPE_WORK = 1;
	public static final int TYPE_FUN = 3;
	/** 聚合保留 多少个结果 **/
	public static final int KEEP_COUNT = 5;

	public static final int MAX_POINTER_COUNT = 50;
	/**
	 * 是否保留 数据点
	 */
	public static boolean KEEP_POINTS = false;

	private static final Map<String, String> _map = ConfigUtil.resolveConfigProFile("userlife.properties");
	static {
		String mg = ConfigUtil.getConfigValue(_map, "userlife.MERGETIME", "30000");
		String tg = ConfigUtil.getConfigValue(_map, "userlife.TIMERANGE", "1000");
		MERGETIME = Integer.valueOf(mg);
		TIMERANGE = Integer.valueOf(tg);
	}

	public static void sortPointers(List<Pointer> pointers) {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		Collections.sort(pointers, getComparator());
	}

	public static void sortCiCountInfo(List<CiCountInfo> cilist) {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		Collections.sort(cilist, new Comparator<CiCountInfo>() {

			public int compare(CiCountInfo o1, CiCountInfo o2) {
				long l2 = o2.getScore();
				long l1 = o1.getScore();
				return Long.compare(l2, l1);
			}

		});
	}

	/**
	 * 按分数排序点 倒叙排序
	 * 
	 * @param pointers
	 */
	public static void sortPointersByScore(List<Pointer> pointers) {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		Collections.sort(pointers, new Comparator<Pointer>() {

			public int compare(Pointer o1, Pointer o2) {
				long l2 = o2.getScore();
				long l1 = o1.getScore();

				return Long.compare(l2, l1);
			}

		});
	}

	/**
	 * 按分数降序排序
	 * 
	 * @param tpointers
	 */
	public static void sortTopPointers(List<TopPointer> tpointers) {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		Collections.sort(tpointers, new Comparator<TopPointer>() {

			public int compare(TopPointer o1, TopPointer o2) {
				long l1 = o1.getScore();
				long l2 = o2.getScore();
				return Long.compare(l2, l1);
			}

		});
	}

	/**
	 * 按 时间 升序排序
	 * 
	 * @return
	 */
	public static Comparator<Pointer> getComparator() {
		return new Comparator<Pointer>() {

			public int compare(Pointer o1, Pointer o2) {
				// 20160419080217
				try {
					Date dt1 = o1._stime();
					Date dt2 = o2._stime();

					return dt1.compareTo(dt2);
				} catch (Exception e) {
					logout.error("比较出错", e);
				}
				return -1;
			}
		};
	}

	/**
	 * 合并 相同位置、类型 的点，时间范围在1min的点
	 * 
	 * @param pointers
	 * @return
	 */
	public static List<Pointer> mergePointers(List<Pointer> pointers) {
		List<Pointer> _rets = new ArrayList<Pointer>();

		Pointer bp = null;
		for (Pointer p : pointers) {
			if (bp != null) {
				if (isSame(bp, p)) {
					bp.setEtime(p.getEtime());
				} else {
					// logout.info(" 合并点： " + bp.getStime() + " " +
					// bp.getEtime());
					_rets.add(bp);
					bp = p;
				}
			} else {
				bp = p;
			}
		}
		_rets.add(bp);

		return _rets;
	}

	/**
	 * 合并点,并移除 合并掉的点
	 * 
	 * @param pointers
	 * @return
	 */
	public static void mergePointers(TreeSet<Pointer> pointers) {
		List<Pointer> rvlist = new ArrayList<Pointer>();
		Pointer bp = null;
		for (Pointer p : pointers) {
			if (bp != null) {
				if (isSame(bp, p)) {
					// bp.setEtime(p.getEtime());
					bp.setStime(bp._stime().getTime() > p._stime().getTime() ? p.getStime() : bp.getStime());
					bp.setEtime(bp._etime().getTime() > p._etime().getTime() ? bp.getEtime() : p.getEtime());
					bp.setGroupCount(bp.getGroupCount() + 1);
					rvlist.add(p);
				} else {
					bp = p;
				}
			} else {
				bp = p;
			}
		}
		for (Pointer p : rvlist) {
			pointers.remove(p);
		}

	}

	/**
	 * 是否 相同(相同来源，相同CI，相同业务类型)
	 * 
	 * @param p1
	 * @param p2
	 * @return
	 */
	public static boolean isSame(Pointer p1, Pointer p2) {
		if (p1.getCi().equals(p2.getCi()) && p1.getSource().equals(p2.getSource())) {
			boolean merg = false;
			if (p1.getBusiness_type() != null && p2.getBusiness_type() != null) {
				// 业务类型 必须一致
				if (p1.getBusiness_type().equals(p2.getBusiness_type())) {
					merg = true;
				} else {
					merg = false;
				}
			} else {
				merg = true;
			}
			if (merg) {
				// 相关的 appid 必须一致
				if (p1.getAppid() != null && p2.getAppid() != null) {
					if (p1.getAppid().equals(p2.getAppid())) {
						merg = true;
					} else {
						merg = false;
					}
				} else {
					merg = true;
				}
			}
			if (merg && (Math.abs(p2._stime().getTime() - p1._etime().getTime()) <= MERGETIME)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 过滤 使用时长过低的点,并 根据 添加标记
	 * 
	 * @param pointers
	 * @return
	 */
	public static List<Pointer> filter(List<Pointer> pointers) {
		List<Pointer> _rets = new ArrayList<Pointer>();
		for (Pointer p : pointers) {
			if (p.timeRange() >= TIMERANGE) {
				try {
					Date dt = daysdf.parse(daysdf.format(p._stime()));
					long time = p._stime().getTime() - dt.getTime();
					// A 00:00-6:30 1.2 NS-2 夜间睡眠时段
					if (time >= 0 && time <= (6 * 60 + 30) * 60 * 1000) {
						p.setTag("NS-2");
						p.setWeight(1.2f);
						p.setTagDescript("夜间睡眠时段");
					}
					// B 6:30-9:00 0.4 MR 上午通勤时段
					if (time > (6 * 60 + 30) * 60 * 1000 && time <= (9 * 60) * 60 * 1000) {
						p.setTag("MR");
						p.setWeight(0.4f);
						p.setTagDescript("上午通勤时段");
					}
					// C 9:01-11:00 0.8 MW 上午工作时段
					if (time > (9 * 60) * 60 * 1000 && time <= (11 * 60) * 60 * 1000) {
						p.setTag("MW");
						p.setWeight(0.8f);
						p.setTagDescript("上午工作时段");
					}
					// D 11:00-13:30 0.6 NL 中午午餐时段
					if (time > (11 * 60) * 60 * 1000 && time <= (13 * 60 + 30) * 60 * 1000) {
						p.setTag("NL");
						p.setWeight(0.6f);
						p.setTagDescript("中午午餐时段");
					}
					// E 13:30-16:30 1 AW 下午工作时段
					if (time > (13 * 60 + 30) * 60 * 1000 && time <= (16 * 60 + 30) * 60 * 1000) {
						p.setTag("AW");
						p.setWeight(1f);
						p.setTagDescript("下午工作时段");
					}
					// F 16:31-19:00 0.5 ER 下午通勤时段
					if (time > (16 * 60 + 30) * 60 * 1000 && time <= (19 * 60) * 60 * 1000) {
						p.setTag("ER");
						p.setWeight(0.5f);
						p.setTagDescript("下午通勤时段");
					}
					// G 19:01-20:30 0.5 DE 傍晚就餐时段
					if (time > (19 * 60 + 30) * 60 * 1000 && time <= (20 * 60 + 30) * 60 * 1000) {
						p.setTag("DE");
						p.setWeight(0.5f);
						p.setTagDescript("傍晚就餐时段");
					}
					// H 20:01-22:30 0.8 BT 晚上睡前时段
					if (time > (20 * 60 + 30) * 60 * 1000 && time <= (22 * 60 + 30) * 60 * 1000) {
						p.setTag("BT");
						p.setWeight(0.8f);
						p.setTagDescript("晚上睡前时段");
					}
					//
					// I 22:30-24:00 1 NS-1 夜间驻留时段
					if (time > (22 * 60 + 30) * 60 * 1000 && time <= (24 * 60) * 60 * 1000) {
						p.setTag("NS-1");
						p.setWeight(1f);
						p.setTagDescript("夜间驻留时段");
					}
					if (StringUtil.isNotNullAndEmpty(p.getTag())) {
						_rets.add(p);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {

			}
		}
		return _rets;
	}

	/**
	 * 计算 居住地
	 * 
	 * @param pointers
	 * @throws Exception
	 */
	public static List<TopPointer> analyseHome(List<Pointer> pointers, long totalCount) throws Exception {
		// 去除, "MR", "DE"
		String[] tags = { "NS-2", "NS-1", "BT" };
		List<Pointer> ps = new ArrayList<Pointer>();
		for (Pointer p : pointers) {
			for (String tag : tags) {
				if (p.getTag().equals(tag)) {
					ps.add(p);
					break;
				}
			}
		}
		logout.info("过滤全部 居住地点 的点: " + ps.size());
		return preanalyse(ps, totalCount);
	}

	/**
	 * 计算工作地点
	 * 
	 * @param pointers
	 * @throws Exception
	 */
	public static List<TopPointer> analyseWork(List<Pointer> pointers, long totalCount) throws Exception {
		// , "NL", "ER", "DE"
		String[] tags = { "MW", "AW" };
		List<Pointer> ps = new ArrayList<Pointer>();
		for (Pointer p : pointers) {
			for (String tag : tags) {
				// 工作日 + 符合的标签
				if (p.getTag().equals(tag) && p.isWeekday()) {
					ps.add(p);
					break;
				}
			}
		}
		logout.info("过滤全部 工作地点 的点: " + ps.size());
		return preanalyse(ps, totalCount);
	}

	/**
	 * 计算 娱乐地点
	 * 
	 * @param pointers
	 * @return
	 * @throws Exception
	 */
	public static List<TopPointer> analyseFun(List<Pointer> pointers, long totalCount) throws Exception {
		// , "NL", "ER", "DE"
		String[] tags = { "MW", "AW" };
		List<Pointer> ps = new ArrayList<Pointer>();
		for (Pointer p : pointers) {
			for (String tag : tags) {
				// 工作日 + 符合的标签
				if (p.getTag().equals(tag) && !p.isWeekday()) {
					ps.add(p);
					break;
				}
			}
		}
		logout.info("过滤全部 工作地点 的点: " + ps.size());
		return preanalyse(ps, totalCount);
	}

	private static List<TopPointer> preanalyse(List<Pointer> ps, long totalCount) throws Exception {
		List<TopPointer> toppts = new ArrayList<TopPointer>();
		long _timecost = System.currentTimeMillis();
		List<List<Point>> points = Dbscan.getInstince().getResult(ps);
		logout.debug(" DBSCAN 聚合点结果:" + points.size() + " \n" + JSON.toJSONString(points));
		_timecost = System.currentTimeMillis() - _timecost;
		for (int gi = 0; gi < points.size(); gi++) {
			Map<String, Integer> _cm = new HashMap<String, Integer>();
			Map<String, Long> _dsr = new TreeMap<String, Long>();// 每天分数分布
			List<Pointer> _pers = new ArrayList<Pointer>();
			// 分组
			List<Point> _ps = points.get(gi);
			if (_ps.isEmpty()) {
				continue;
			}
			long totalScore = 0;
			for (Point _p : _ps) {
				Pointer _per = _p.getPointer();
				long score = _per.calcScore(WORTH);
				totalScore += score;
				_per.setScore(score);
				_pers.add(_per);

				Integer v = _cm.get(_per.getTag());
				if (v == null) {
					v = 1;
				} else {
					v++;
				}
				_cm.put(_per.getTag(), v);

				String _ds = daysdf.format(_per._stime());
				Long _v = _dsr.get(_ds);
				if (_v == null) {
					_v = 0l;
				} else {
					_v += score;
				}
				_dsr.put(_ds, _v);
			}

			List<Map<String, Object>> _dp = new ArrayList<Map<String, Object>>();
			// 格式化 统计结果
			for (Map.Entry<String, Integer> entry : _cm.entrySet()) {
				Map<String, Object> _m = new HashMap<String, Object>();
				_m.put("tag", entry.getKey());
				_m.put("count", entry.getValue());
				_m.put("percent", entry.getValue() * 100 / _ps.size());
				_dp.add(_m);
			}
			List<Map<String, Object>> _dp2 = new ArrayList<Map<String, Object>>();
			if (totalScore > 0) {
				for (Map.Entry<String, Long> entry : _dsr.entrySet()) {
					Map<String, Object> _m = new HashMap<String, Object>();
					_m.put("time", entry.getKey());
					_m.put("score", entry.getValue());
					_m.put("percent", entry.getValue() * 100 / totalScore);
					_dp2.add(_m);
				}
			}

			// logout.info("聚合点 分数：" + totalScore + " ,点数:" + _pers.size());
			TopPointer tp = new TopPointer();
			tp.setPointers(_pers);
			tp.setScore(totalScore);
			tp.setPcount(_pers.size());
			tp.setTotalCount(totalCount);// 设置总点数
			tp.setDistributionPoint(JSON.toJSONString(_dp));
			tp.setDayScoreRank(JSON.toJSONString(_dp2));
			tp.setDbc_time(_timecost);

			// 根据比例，重新计算分数
			filterDayScore(_dp2, _pers);

			CountPointersInfo(tp, _ps);

			toppts.add(tp);
		}
		analyse(toppts);
		sortTopPointers(toppts);

		// tp.setPointers(sortAndSubLength(_pers));
		return subResult(toppts);
	}

	/**
	 * 按小区 统计集合信息
	 * 
	 * @param toppts
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	public static void CountPointersInfo(TopPointer tp, List<Point> _ps) throws Exception {
		String imsi = null;
		String imei = null;
		Map<String, Map<String, Integer>> appM = new HashMap<String, Map<String, Integer>>();
		Map<String, Map<String, Integer>> hostM = new HashMap<String, Map<String, Integer>>();
		Map<String, Map<String, Integer>> urlM = new HashMap<String, Map<String, Integer>>();
		Map<String, Map<String, Long>> cm = new HashMap<String, Map<String, Long>>();
		if (tp.getPointers() == null) {
			return;
		}
		for (Pointer pt : tp.getPointers()) {
			if (StringUtil.isNullOrEmpty(imsi) && StringUtil.isNotNullAndEmpty(pt.getImsi())) {
				imsi = pt.getImsi();
			}
			if (StringUtil.isNullOrEmpty(imei) && StringUtil.isNotNullAndEmpty(pt.getImei())) {
				imei = pt.getImei();
			}

			String k = null;
			Long v = 0l;

			k = "score";
			v = pt.getScore();
			_mapValue(cm, k, v, pt.getCi());

			k = "count";
			v = (long) pt.getGroupCount();
			v = v == 0 ? 1 : v;
			_mapValue(cm, k, v, pt.getCi());

			if (Pointer.BUSSINESS_DATA.equals(pt.getBusiness_type())) {
				String _word = CiCountInfo.numMap.get(pt.getNettype());
				if (StringUtil.isNotNullAndEmpty(_word)) {
					k = _word + "G_up_bytes";
					if (StringUtil.isNotNullAndEmpty(pt.getUp_bytes())) {
						v = Long.valueOf(pt.getUp_bytes());
					} else {
						v = 0l;
					}
					_mapValue(cm, k, v, pt.getCi());
					k = _word + "G_down_bytes";
					if (StringUtil.isNotNullAndEmpty(pt.getUp_bytes())) {
						v = Long.valueOf(pt.getUp_bytes());
					} else {
						v = 0l;
					}
					_mapValue(cm, k, v, pt.getCi());
				}

				String appid = pt.getAppid();
				String host = pt.getHost();
				String url = pt.getUrl();
				Map<String, Integer> appCount = appM.get(pt.getCi());
				Map<String, Integer> hostCount = hostM.get(pt.getCi());
				Map<String, Integer> urlCount = urlM.get(pt.getCi());
				if (appCount == null) {
					appCount = new HashMap<String, Integer>();
					appM.put(pt.getCi(), appCount);
				}
				if (hostCount == null) {
					hostCount = new HashMap<String, Integer>();
					hostM.put(pt.getCi(), hostCount);
				}
				if (urlCount == null) {
					urlCount = new HashMap<String, Integer>();
					urlM.put(pt.getCi(), urlCount);
				}

				if (StringUtil.isNotNullAndEmpty(appid)) {
					Integer _v = appCount.get(appid);
					if (_v == null) {
						_v = 0;
					}
					_v++;
					appCount.put(appid, _v);
				}
				if (StringUtil.isNotNullAndEmpty(host)) {
					Integer _v = hostCount.get(host);
					if (_v == null) {
						_v = 0;
					}
					_v++;
					hostCount.put(host, _v);
				}
				if (StringUtil.isNotNullAndEmpty(url)) {
					Integer _v = urlCount.get(url);
					if (_v == null) {
						_v = 0;
					}
					_v++;
					urlCount.put(url, _v);
				}

			} else {
				// 其它业务
				String bt = pt.getBusiness_type();
				if (Pointer.BUSSINESS_SMS.equals(bt)) {
					k = "sms_count";
					v = pt.timeRange();
				} else if (Pointer.BUSSINESS_LOCATION.equals(bt)) {
					k = "location_count";
					v = pt.timeRange();
				} else if (Pointer.BUSSINESS_VOICE.equals(bt)) {
					k = "voice_count";
					v = pt.timeRange();
				} else {
					k = null;
					v = 0l;
				}
				if (k == null)
					continue;
				_mapValue(cm, k, v, pt.getCi());
			}
		}

		List<CiCountInfo> ciCountInfos = new ArrayList<CiCountInfo>();
		// 遍历统计结果
		for (Map.Entry<String, Map<String, Long>> entry : cm.entrySet()) {
			String ci = entry.getKey();
			Map<String, Long> _count = entry.getValue();
			CiCountInfo _CiCountInfo = new CiCountInfo();
			_CiCountInfo.setCi(ci);
			if (UserLifeBinJiangReduce._cacheMap != null)
				_CiCountInfo.setCiinfo(UserLifeBinJiangReduce._cacheMap.get(ci));

			for (Map.Entry<String, Long> _entry2 : _count.entrySet()) {
				// 获取 属性 set方法，并调用
				Method method = ReflectUtil.getFieldSetterMethod(_entry2.getKey(), CiCountInfo.class, Long.class);
				if (method != null && _entry2.getValue() != null)
					method.invoke(_CiCountInfo, _entry2.getValue());
			}

			Map<String, Integer> appCount = appM.get(ci);
			Map<String, Integer> hostCount = hostM.get(ci);
			Map<String, Integer> urlCount = urlM.get(ci);

			List<Map<String, Object>> _urlL = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> _appL = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> _hostL = new ArrayList<Map<String, Object>>();
			if (appCount != null) {

				for (Map.Entry<String, Integer> e : appCount.entrySet()) {
					Map<String, Object> m = new HashMap<String, Object>();
					m.put("tag", e.getKey());
					m.put("value", e.getValue());
					_appL.add(m);
				}
			}
			if (hostCount != null) {

				for (Map.Entry<String, Integer> e : hostCount.entrySet()) {
					Map<String, Object> m = new HashMap<String, Object>();
					m.put("tag", e.getKey());
					m.put("value", e.getValue());
					_hostL.add(m);
				}
			}
			if (urlCount != null) {
				for (Map.Entry<String, Integer> e : urlCount.entrySet()) {
					Map<String, Object> m = new HashMap<String, Object>();
					m.put("tag", e.getKey());
					m.put("value", e.getValue());
					_urlL.add(m);
				}
			}

			_appL = sortMapAndSubLength(_appL);
			_hostL = sortMapAndSubLength(_hostL);
			_urlL = sortMapAndSubLength(_urlL);

			_CiCountInfo.setAppTop(_appL);
			_CiCountInfo.setHostCount(_hostL);
			_CiCountInfo.setUrlTop(_urlL);

			ciCountInfos.add(_CiCountInfo);
		}
		sortCiCountInfo(ciCountInfos);

		// 生成机型号
		String phone_model = phone_model(imei);
		tp.setImsi(imsi);
		tp.setImei(imei);
		tp.setPhone_model(phone_model);
		tp.setCiCountInfos(ciCountInfos);

	}

	private static void _mapValue(Map<String, Map<String, Long>> cm, String k, Long v, String ci) {
		Map<String, Long> _cm = cm.get(ci);
		if (_cm == null) {
			_cm = new HashMap<String, Long>();
		}
		Long _v = _cm.get(k);
		if (_v == null) {
			_v = 0l;
		}
		_cm.put(k, _v + v);
		cm.put(ci, _cm);
	}

	/**
	 * 根据 imei 获取 设备型号
	 * 
	 * @param imei
	 * @return 未识别 返回"未知"
	 */
	private static String phone_model(String imei) {
		if (StringUtil.isNullOrEmpty(imei) || UserLifeBinJiangReduce._imeiMap == null)
			return "未知";
		for (int ls : new int[] { 7, 8, 6, 9 }) {
			String _imei_ = imei.substring(0, ls);
			Map<String, Object> data = UserLifeBinJiangReduce._imeiMap.get(_imei_);
			if (data != null) {
				String pm = data.get("tel_fac") + "-" + data.get("tel_type");
				return pm;
			}
		}
		return "未知";
	}

	/**
	 * 如果某一天 数据比重 超过 平均比重
	 * 
	 * @param _dp2
	 * @param _pers
	 */
	private static void filterDayScore(List<Map<String, Object>> _dp2, List<Pointer> _pers) {

	}

	private static List<TopPointer> subResult(List<TopPointer> tps) {
		// 截取固定结果
		int ts = tps.size();
		int end = ts > KEEP_COUNT ? KEEP_COUNT : ts;
		List<TopPointer> ret = tps.subList(0, end);
		for (TopPointer tp : ret) {
			if (KEEP_POINTS) {
				tp.setPointers(sortAndSubLength(tp.getPointers()));
			} else {
				tp.setPointers(null);
			}
		}

		return ret;
	}

	/**
	 * 分析 合并点 出新的点 (按 所占比例 重新计算 点位置)
	 * 
	 * @param topointers
	 */
	private static void analyse(List<TopPointer> topointers) {
		Map<String, Pointer> ciMap = new HashMap<String, Pointer>();
		// 相同 ci 合并
		for (TopPointer tp : topointers) {
			if (tp.getScore() == 0) {
				// 不符合条件的
				continue;
			}
			long totalScore = 0;
			List<Pointer> ps = tp.getPointers();
			for (Pointer p : ps) {
				Pointer _p = ciMap.get(p.getCi());
				totalScore += p.getScore();
				if (_p == null) {
					_p = new Pointer();
					_p.setScore(p.getScore());
					_p.setLat(p.getLat());
					_p.setLng(p.getLng());
				} else {
					_p.setScore(_p.getScore() + p.getScore());// 权值相加
				}
				ciMap.put(p.getCi(), _p);
			}

			tp.setScore(totalScore);
			float _lat = 0;
			float _lng = 0;
			for (Entry<String, Pointer> entry : ciMap.entrySet()) {
				Pointer _per = entry.getValue();
				_lat += _per.get_lat() * _per.getScore() / tp.getScore();
				_lng += _per.get_lng() * _per.getScore() / tp.getScore();
			}
			tp.set_lat(_lat);
			tp.set_lng(_lng);

			ciMap.clear();
		}
	}

	/**
	 * 点 预处理（排序，合并，过滤）
	 * 
	 * @param pointers
	 * @return
	 */
	public static List<Pointer> prepareInit(List<Pointer> pointers) {
		sortPointers(pointers);
		List<Pointer> nl = UserlifeService.mergePointers(pointers);
		nl = UserlifeService.filter(pointers);
		return nl;
	}

	/**
	 * 创建插入 对象
	 * 
	 * @param tps
	 * @param type
	 * @return
	 */
	public static List<Put> createPuts(String imsi, List<TopPointer> tps, int type, String week, String month) {
		if (StringUtil.isNullOrEmpty(imsi)) {
			return null;
		}
		if (!StringUtil.isNum(imsi)) {
			logout.error("IMSI 必须是 纯数字! " + imsi);
			return null;
		}
		if (tps == null || tps.isEmpty()) {
			return null;
		}
		StringBuffer sbk = new StringBuffer();
		sbk.append(StringUtil.toFix(Long.valueOf(StringUtil.reverse(imsi)), 15));
		sbk.append("_");
		if (StringUtil.isNotNullAndEmpty(week)) {
			sbk.append(week);
		} else {
			sbk.append(DateTimeUtil.getCurrentDayOfWeek());// 周数
		}
		sbk.append("_");
		if (StringUtil.isNotNullAndEmpty(month)) {
			sbk.append(StringUtil.toFix(Integer.valueOf(month.trim()), 2));
		} else {
			sbk.append(StringUtil.toFix(DateTimeUtil.getCurrentMonth_H(), 2));
		}

		sbk.append("_");
		sbk.append(type);
		sbk.append("_");

		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 5 && i < tps.size(); i++) {
			TopPointer tp = tps.get(i);
			List<ColumnData> cds = new ArrayList<HBaseUtil.ColumnData>();
			cds.add(new ColumnData("info", "lat", String.valueOf(tp.get_lat())));
			cds.add(new ColumnData("info", "lng", String.valueOf(tp.get_lng())));
			cds.add(new ColumnData("info", "score", String.valueOf(tp.getScore())));
			cds.add(new ColumnData("info", "pcount", String.valueOf(tp.getPcount())));
			cds.add(new ColumnData("info", "totalCount", String.valueOf(tp.getTotalCount())));
			cds.add(new ColumnData("info", "distributionPoint", String.valueOf(tp.getDistributionPoint())));
			cds.add(new ColumnData("info", "dayScoreRank", String.valueOf(tp.getDayScoreRank())));
			cds.add(new ColumnData("info", "dbc_time", String.valueOf(tp.getDbc_time())));

			// imsi;
			cds.add(new ColumnData("info", "imsi", String.valueOf(tp.getImsi())));
			// imei;
			cds.add(new ColumnData("info", "imei", String.valueOf(tp.getImei())));
			// phone_model;
			cds.add(new ColumnData("info", "phone_model", String.valueOf(tp.getPhone_model())));

			List<Pointer> _pts = tp.getPointers();
			if (_pts != null && _pts.size() > 0) {
				cds.add(new ColumnData("info", "points", JSON.toJSONString(tp.getPointers())));
			}
			// ciCountInfos
			List<CiCountInfo> _cis = tp.getCiCountInfos();
			if (_cis != null && !_cis.isEmpty()) {
				cds.add(new ColumnData("info", "ciCountInfos", JSON.toJSONString(_cis)));
			}

			puts.add(HBaseUtil.createPut(sbk.toString() + (i + 1), cds));
		}

		return puts;
	}

	/**
	 * 合并点，并截取长度
	 * 
	 * @param pointers
	 * @return
	 */
	public static List<Pointer> sortAndSubLength(List<Pointer> pointers) {
		Map<String, Pointer> map = new HashMap<String, Pointer>();
		for (Pointer ptr : pointers) {
			Pointer _p = map.get(ptr.getCi());
			if (_p == null) {
				map.put(ptr.getCi(), ptr);
			} else {
				_p.setScore(ptr.getScore() + _p.getScore());
				_p.setGroupCount(_p.getGroupCount() + 1);
				List<String> _tr = _p.getTimeRanges();
				if (_tr == null) {
					_tr = new ArrayList<String>();
				}
				_tr.add(ptr.getStime() + "-" + ptr.getEtime());
				_p.setTimeRanges(_tr);
			}
		}
		List<Pointer> list = new ArrayList<Pointer>(map.values());
		int ls = list.size();
		sortPointersByScore(list);
		return list.subList(0, ls > MAX_POINTER_COUNT ? MAX_POINTER_COUNT : ls);
	}

	public static List<Map<String, Object>> sortMapAndSubLength(List<Map<String, Object>> list) {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		Collections.sort(list, new Comparator<Map<String, Object>>() {

			public int compare(Map<String, Object> o1, Map<String, Object> o2) {
				Object _v1 = o1.get("value");
				Object _v2 = o2.get("value");

				if (_v1 != null && _v2 != null && StringUtil.isNum(String.valueOf(_v1))
						&& StringUtil.isNum(String.valueOf(_v2))) {
					Integer _i1 = Integer.valueOf(_v1.toString());
					Integer _i2 = Integer.valueOf(_v2.toString());
					return Integer.compare(_i2, _i1);// 倒叙
				}
				return 0;
			}

		});
		return list.subList(0, list.size() >= 5 ? 5 : list.size());
	}

	public static void main(String[] args) {
		String s = JSON.toJSONString(null);
		System.out.println(s);
	}
}
