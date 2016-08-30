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
import com.zltel.common.utils.reflect.BeanUtil;
import com.zltel.common.utils.reflect.ReflectUtil;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.common.utils.time.DateTimeUtil;
import com.zltel.dbscan.bean.Point;
import com.zltel.dbscan.service.Dbscan;
import com.zltel.gridcluster.bean.GridGroup;
import com.zltel.gridcluster.bean.GroupParams;
import com.zltel.gridcluster.service.GridClusterService;
import com.zltel.location_aware.userlife.bean.CiCountInfo;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.bean.TopPointer;
import com.zltel.location_aware.userlife.doublesort.DoubleSortKey;
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
	public static final int KEEP_COUNT = 3;
	/** 点保留数 **/
	public static final int MAX_POINTER_COUNT = 20;
	/** 小区 Top保留数 **/
	public static final int CI_COUNTINFO_SIZE = 20;

	/**
	 * 是否保留 数据点
	 */
	public static boolean KEEP_POINTS = false;

	private static String[] HOME_TAGS = null;
	private static String[] WORK_TAGS = null;
	private static String[] FUN_TAGS = null;

	private static final Map<String, String> _map = ConfigUtil.resolveConfigProFile("userlife.properties");
	static {
		String mg = ConfigUtil.getConfigValue(_map, "userlife.MERGETIME", "30000");
		String tg = ConfigUtil.getConfigValue(_map, "userlife.TIMERANGE", "1000");

		MERGETIME = Integer.valueOf(mg);
		TIMERANGE = Integer.valueOf(tg);

		String str = null;
		// Home Tags
		str = ConfigUtil.getConfigValue(_map, "home.tags", "NS-2,NS-1,BT");
		HOME_TAGS = str.split(",");

		// Work Tags
		str = ConfigUtil.getConfigValue(_map, "work.tags", "MW,AW");
		WORK_TAGS = str.split(",");
		// Fun Tags
		str = ConfigUtil.getConfigValue(_map, "fun.tags", "MW,AW");
		FUN_TAGS = str.split(",");
	}

	public static void sortPointers(List<Pointer> pointers) {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		Collections.sort(pointers, getComparator());
	}

	/**
	 * 根据 小区分数 高低排序
	 * 
	 * @param cilist
	 */
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
	 * 合并点 重新设计合并规则,在有序列表中，新增点 连续判断是否能被合并，能则更改合并数据，否则加入队列
	 * 
	 * @param pointers
	 * @return
	 */
	public static void mergePointers(TreeSet<Pointer> pointers, Pointer _pointer) {
		boolean merg = false;
		for (Pointer _p : pointers) {
			// 开始时间 和 结束时间 相差小于合并间隔
			if (related(_p, _pointer)) {
				if (isSame(_p, _pointer)) {
					// 开始时间取 小的一个
					_p._stime(new Date(Math.min(_p._stime().getTime(), _p._stime().getTime())));
					// 结束时间取 大的一个
					_p._etime(new Date(Math.max(_p._etime().getTime(), _p._etime().getTime())));

					_p.setGroupCount(_p.getGroupCount() + 1);
					merg = true;
				}

			} else {
				continue;
			}

		}
		// 没有合并， 添加入队列
		if (!merg) {
			pointers.add(_pointer);
		}
	}

	/**
	 * 针对 有序的 List 进行合并
	 * 
	 * @param pointers
	 *            有序的 升序 List
	 * @param _pointer
	 *            要添加的 点
	 */
	public static void mergePointers(List<Pointer> pointers, Pointer _pointer) {
		if (pointers == null || _pointer == null) {
			return;
		}
		// sortPointers(pointers);
		boolean merg = false;
		// 使用 折半查找 提高性能

		int start = getMinRelateIndex(pointers, _pointer);
		int end = pointers.size();

		for (int idx = start; !merg && idx < end; idx++) {
			Pointer _p = pointers.get(idx);
			// 开始时间 和 结束时间 相差小于合并间隔
			if (related(_p, _pointer)) {
				if (isSame(_p, _pointer)) {
					// 开始时间取 小的一个
					_p._stime(new Date(Math.min(_p._stime().getTime(), _pointer._stime().getTime())));
					// 结束时间取 大的一个
					_p._etime(new Date(Math.max(_p._etime().getTime(), _pointer._etime().getTime())));

					_p.setGroupCount(_p.getGroupCount() + 1);
					merg = true;
				}
			} else {
				continue;
			}
		}
		// 没有合并， 添加入队列
		if (!merg) {
			pointers.add(_pointer);
		}
	}

	/**
	 * 从 队列 末尾开始比较，直到 不相关
	 * 
	 * @param pointers
	 * @param _pointer
	 * @return
	 */
	private static int getMinRelateIndex(List<Pointer> pointers, Pointer _pointer) {
		for (int i = pointers.size() - 1; i >= 0; i--) {
			if (i > 0 && !related(_pointer, pointers.get(i))) {
				// logout.info("返回 最小相邻:" + i);
				return i;
			}
		}
		return 0;
	}

	/**
	 * 两个点 是否相关, 如果 开始时间-结束时间 的绝对值小于 合并间隔，则认为有关
	 * 
	 * @param p1
	 * @param p2
	 * @return
	 */
	public static boolean related(Pointer p1, Pointer p2) {
		// 相互距离小于门限值
		boolean ret = Math.abs(p1._stime().getTime() - p2._etime().getTime()) <= MERGETIME
				|| Math.abs(p1._etime().getTime() - p2._stime().getTime()) <= MERGETIME
				// 相交的情况
				|| Math.abs(p1._stime().getTime() - p2._stime().getTime()) <= MERGETIME
				|| Math.abs(p1._etime().getTime() - p2._etime().getTime()) <= MERGETIME;
		return ret;
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
			if (merg && related(p1, p2)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 给 点根据 开始时间 打上标记
	 * 
	 * @param pointer
	 * @return
	 */
	public static Pointer markTags(Pointer p) {
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
				p.setWeight(0.9f);
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
				p.setWeight(0.4f);
				p.setTagDescript("下午通勤时段");
			}
			// G 19:01-20:30 0.5 DE 傍晚就餐时段
			if (time > (19 * 60 + 30) * 60 * 1000 && time <= (20 * 60 + 30) * 60 * 1000) {
				p.setTag("DE");
				p.setWeight(0.4f);
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		return p;
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
				markTags(p);
				if (StringUtil.isNotNullAndEmpty(p.getTag())) {
					_rets.add(p);
				}
			} else {

			}
		}
		return _rets;
	}

	/**
	 * 是否 居住地 标签
	 * 
	 * @param p
	 * @return
	 */
	public static boolean isHomeTag(Pointer p) {
		// 去除,, "MR", "DE"
		String[] tags = HOME_TAGS;
		for (String tag : tags) {
			if (tag.trim().equals(p.getTag())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 是否工作标签
	 * 
	 * @param p
	 * @return
	 */
	public static boolean isWorkTag(Pointer p) {
		// "MR", "NL", "ER", "DE"
		String[] tags = WORK_TAGS;
		for (String tag : tags) {
			// 工作日 + 符合的标签
			if (tag.trim().equals(p.getTag()) && p.isWeekday()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 是否 娱乐标签
	 * 
	 * @param p
	 * @return
	 */
	public static boolean isFunTag(Pointer p) {
		String[] tags = FUN_TAGS;
		for (String tag : tags) {
			// 工作日 + 符合的标签
			if (tag.trim().equals(p.getTag()) && !p.isWeekday()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 计算 居住地
	 * 
	 * @param pointers
	 * @throws Exception
	 */
	public static List<TopPointer> analyseHome(List<Pointer> pointers, long totalCount, long _time) throws Exception {
		String imsi = null;
		List<Pointer> ps = new ArrayList<Pointer>();
		for (Pointer p : pointers) {
			if (imsi == null && p.getImsi() != null) {
				imsi = p.getImsi();
			}
			if (isHomeTag(p)) {
				ps.add(p);
			}
		}
		logout.info("过滤全部 居住地点 的点: " + ps.size());
		return preanalyse(ps, totalCount, _time);
	}

	/**
	 * 计算工作地点
	 * 
	 * @param pointers
	 * @throws Exception
	 */
	public static List<TopPointer> analyseWork(List<Pointer> pointers, long totalCount, long _time) throws Exception {
		List<Pointer> ps = new ArrayList<Pointer>();
		for (Pointer p : pointers) {
			// 工作日 + 符合的标签
			if (isWorkTag(p)) {
				ps.add(p);
			}
		}
		logout.info("过滤全部 工作地点 的点: " + ps.size());
		return preanalyse(ps, totalCount, _time);
	}

	/**
	 * 计算 娱乐地点
	 * 
	 * @param pointers
	 * @return
	 * @throws Exception
	 */
	public static List<TopPointer> analyseFun(List<Pointer> pointers, long totalCount, long _time) throws Exception {

		List<Pointer> ps = new ArrayList<Pointer>();
		for (Pointer p : pointers) {
			// 工作日 + 符合的标签
			if (isFunTag(p)) {
				ps.add(p);
			}
		}
		logout.info("过滤全部 工作地点 的点: " + ps.size());
		return preanalyse(ps, totalCount, _time);
	}

	private static List<TopPointer> preanalyse(List<Pointer> ps, long totalCount, long _time) throws Exception {
		List<TopPointer> toppts = new ArrayList<TopPointer>();
		long _timecost = System.currentTimeMillis();
		List<List<Point>> points = Dbscan.getInstince().getResult(ps);
		logout.debug(" DBSCAN 聚合点结果:" + points.size() + " \n");
		_timecost = System.currentTimeMillis() - _timecost;
		for (int gi = 0; gi < points.size(); gi++) {
			Map<String, Integer> _cm = new HashMap<String, Integer>();
			Map<String, Long> _dsr = new TreeMap<String, Long>();// 每天分数分布
			List<Pointer> _pers = new ArrayList<Pointer>();

			Map<String, Long> _busMap = new HashMap<String, Long>();// 业务分布

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

				Long v2 = _busMap.get(_per.getBusiness_type());
				if (v2 == null) {
					v2 = 0l;
				}
				v2 += score;
				_busMap.put(_per.getBusiness_type(), v2);

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
					_m.put("totalScore", totalScore);
					_m.put("percent", entry.getValue() * 100 / totalScore);
					_dp2.add(_m);
				}
			}

			// _busMap
			List<Map<String, Object>> _busL = new ArrayList<Map<String, Object>>();
			if (totalScore > 0) {
				for (Map.Entry<String, Long> entry : _busMap.entrySet()) {
					Map<String, Object> _m = new HashMap<String, Object>();
					_m.put("tag", entry.getKey());
					_m.put("score", entry.getValue());
					_m.put("totalScore", totalScore);
					_m.put("percent", entry.getValue() * 100 / totalScore);
					_busL.add(_m);
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
			tp.setMerg_time(_time);
			tp.setBussRank(JSON.toJSONString(_busL));

			// 根据比例，重新计算分数
			filterDayScore(_dp2, _pers);

			CountPointersInfo(tp);

			toppts.add(tp);
		}
		analyse(toppts);
		sortTopPointers(toppts);

		// tp.setPointers(sortAndSubLength(_pers));
		return subResult(toppts);
	}

	/**
	 * 转换
	 * 
	 * @param pointers
	 * @param totalCount
	 * @param _time
	 * @return
	 * @throws Exception
	 */
	public static TopPointer analyTopPointer(List<Pointer> pointers, long totalCount, long _time) throws Exception {
		Map<String, Integer> _cm = new HashMap<String, Integer>();
		Map<String, Long> _dsr = new TreeMap<String, Long>();// 每天分数分布

		Map<String, Long> _busMap = new HashMap<String, Long>();// 业务分布

		// 分组
		if (pointers.isEmpty()) {
			return null;
		}
		long totalScore = 0;
		for (Pointer _per : pointers) {
			long score = _per.calcScore(WORTH);
			totalScore += score;
			_per.setScore(score);

			Integer v = _cm.get(_per.getTag());
			if (v == null) {
				v = 1;
			} else {
				v++;
			}
			_cm.put(_per.getTag(), v);

			Long v2 = _busMap.get(_per.getBusiness_type());
			if (v2 == null) {
				v2 = 0l;
			}
			v2 += score;
			_busMap.put(_per.getBusiness_type(), v2);

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
			_m.put("percent", entry.getValue() * 100 / pointers.size());
			_dp.add(_m);
		}
		List<Map<String, Object>> _dp2 = new ArrayList<Map<String, Object>>();
		if (totalScore > 0) {
			for (Map.Entry<String, Long> entry : _dsr.entrySet()) {
				Map<String, Object> _m = new HashMap<String, Object>();
				_m.put("time", entry.getKey());
				_m.put("score", entry.getValue());
				_m.put("totalScore", totalScore);
				_m.put("percent", entry.getValue() * 100 / totalScore);
				_dp2.add(_m);
			}
		}

		// _busMap
		List<Map<String, Object>> _busL = new ArrayList<Map<String, Object>>();
		if (totalScore > 0) {
			for (Map.Entry<String, Long> entry : _busMap.entrySet()) {
				Map<String, Object> _m = new HashMap<String, Object>();
				_m.put("tag", entry.getKey());
				_m.put("score", entry.getValue());
				_m.put("totalScore", totalScore);
				_m.put("percent", entry.getValue() * 100 / totalScore);
				_busL.add(_m);
			}
		}

		// logout.info("聚合点 分数：" + totalScore + " ,点数:" + _pers.size());
		TopPointer tp = new TopPointer();
		tp.setPointers(pointers);
		tp.setScore(totalScore);
		tp.setPcount(pointers.size());
		tp.setTotalCount(totalCount);// 设置总点数
		tp.setDistributionPoint(JSON.toJSONString(_dp));
		tp.setDayScoreRank(JSON.toJSONString(_dp2));
		tp.setDbc_time(0l);
		tp.setMerg_time(_time);
		tp.setBussRank(JSON.toJSONString(_busL));

		// 根据比例，重新计算分数
		filterDayScore(_dp2, pointers);

		CountPointersInfo(tp);
		return tp;
	}

	/**
	 * 按小区 统计集合信息
	 * 
	 * @param toppts
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	public static void CountPointersInfo(TopPointer tp) throws Exception {
		String imsi = null;
		String imei = null;
		Map<String, Map<String, Integer>> appM = new HashMap<String, Map<String, Integer>>();
		Map<String, Map<String, Integer>> hostM = new HashMap<String, Map<String, Integer>>();
		Map<String, Map<String, Integer>> urlM = new HashMap<String, Map<String, Integer>>();
		Map<String, Map<String, Long>> cm = new HashMap<String, Map<String, Long>>();

		// 全局 AppTop计算
		Map<String, Map<String, Object>> totalApp = new HashMap<String, Map<String, Object>>();
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
					if (StringUtil.isNotNullAndEmpty(pt.getDown_bytes())) {
						v = Long.valueOf(pt.getDown_bytes());
					} else {
						v = 0l;
					}
					_mapValue(cm, k, v, pt.getCi());
				}

				String appid = pt.getAppid();
				String host = pt.getHost();
				String url = pt.getUrl();

				Map<String, Object> _tapps = totalApp.get(appid);
				if (_tapps == null) {
					_tapps = new HashMap<String, Object>();
					_tapps.put("tag", appid);
					_tapps.put("value", pt.timeRange() / 1000);
				} else {
					Object o = _tapps.get("value");
					if (o == null) {
						o = 0;
					}
					_tapps.put("value", pt.timeRange() / 1000 + Long.valueOf(o.toString()));
				}
				totalApp.put(appid, _tapps);

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
		// 截取长度
		int sz = ciCountInfos.size() > CI_COUNTINFO_SIZE ? CI_COUNTINFO_SIZE : ciCountInfos.size();
		ciCountInfos = ciCountInfos.subList(0, sz);

		List<Map<String, Object>> __totalAppTop = new ArrayList<Map<String, Object>>(totalApp.values());
		__totalAppTop = sortMapAndSubLength(__totalAppTop);
		// 生成机型号
		String phone_model = phone_model(imei);
		tp.setImsi(imsi);
		tp.setImei(imei);
		tp.setPhone_model(phone_model);
		tp.setCiCountInfos(ciCountInfos);
		tp.setAppTop(__totalAppTop);

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
			// IMSI 必须超过 截取长度
			if (imei.length() < ls) {
				continue;
			}
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

			String[] continueArrays = new String[] { "lat", "lng", "_lat", "_lng" };
			Map<String, Object> maps = BeanUtil.ConvertBeanToMap(tp);
			for (Map.Entry<String, Object> entry : maps.entrySet()) {
				String k = entry.getKey().toLowerCase();
				Object v = entry.getValue();
				boolean ifc = false;
				for (String ca : continueArrays) {
					if (k.equals(ca)) {
						ifc = true;
						break;
					}
				}
				if (!ifc && v != null) {
					// 获取字段标准的 名
					k = getMethodName(k, TopPointer.class);
					if (k == null)
						continue;
					if (v instanceof Long || v instanceof String || v instanceof Integer) {
						cds.add(new ColumnData("info", k, String.valueOf(v)));
					} else { // List
						cds.add(new ColumnData("info", k, JSON.toJSONString(v)));
					}
				}
			}

			puts.add(HBaseUtil.createPut(sbk.toString() + (i + 1), cds));
		}

		return puts;

	}

	public static <T> String getMethodName(String field, Class<T> c) {
		Method method = ReflectUtil.getFieldGetterMethod(field, c);
		if (method != null) {
			String mn = method.getName();
			String _field = mn.substring(3);
			// 默认
			if (_field.length() <= 1) {
				return _field.toLowerCase();
			}
			char _c = _field.charAt(1);
			boolean isl = Character.isLowerCase(_c);
			String nmn = _field.substring(0, 2);
			nmn = isl ? nmn.toLowerCase() : nmn.toUpperCase();
			return nmn + _field.substring(2);
		}
		return null;
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
				// 不保留具体 数据
				// _tr.add(ptr.getStime() + "-" + ptr.getEtime());
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
					Long _i1 = Long.valueOf(_v1.toString());
					Long _i2 = Long.valueOf(_v2.toString());
					return Long.compare(_i2, _i1);// 倒叙
				}
				return 0;
			}

		});
		return list.subList(0, list.size() >= 5 ? 5 : list.size());
	}

	/**
	 * 根据 网格聚类计算结果
	 * 
	 * @param _type
	 * @param pts
	 * @param totalCount
	 * @param _time
	 * @return
	 * @throws Exception
	 */
	public static List<TopPointer> analyseByGridCluster(int _type, DoubleSortKey key, List<Pointer> pts,
			long totalCount, long _time) throws Exception {
		GridClusterService gridClusterService = GridClusterService.getInstince();
		GroupParams groupParams = GroupParams.createGroupParams(_map);

		// 第一次 聚合 1km
		for (Pointer p : pts) {
			long score = p.calcScore(WORTH);
			p.setScore(score);
			gridClusterService.markIndex(p, groupParams.getFirst_step(), groupParams.getFirst_radius());
		}
		List<GridGroup> groups = gridClusterService.makeGroup(groupParams.getFirst_score_threshold(),
				groupParams.getFirst_max_neighbor_distince());
		// 截取长度
		int l = groups.size() > KEEP_COUNT ? KEEP_COUNT : groups.size();
		logout.info(key.getFirstKey().toString() + " 第一次聚合： 个数:" + groups.size() + ",保留:" + l);
		logout.info(" 半径:" + groupParams.getFirst_radius() + " , 步长:" + groupParams.getFirst_step() + " 门限:"
				+ groupParams.getFirst_score_threshold());
		List<TopPointer> tps = new ArrayList<TopPointer>();
		int i = 0;
		for (GridGroup gg : groups.subList(0, l)) {
			i++;
			logout.info(key.getFirstKey().toString() + " 第一次聚合，分组:" + i);
			logout.info(key.getFirstKey().toString() + "\n" + gridClusterService.print(gg).toString());

			// 第二次聚合
			gridClusterService.clear();
			for (Pointer _p : gg.allPointers()) {
				gridClusterService.markIndex(_p, groupParams.getSecond_step(), groupParams.getSecond_radius());
			}
			List<GridGroup> _gs = gridClusterService.makeGroup(groupParams.getSecond_score_threshold(),
					groupParams.getSecond_max_neighbor_distince());
			logout.info(key.getFirstKey().toString() + " 第二次聚合： 个数:" + _gs.size());
			logout.info(" 半径:" + groupParams.getSecond_radius() + " , 步长:" + groupParams.getSecond_step() + " 门限:"
					+ groupParams.getSecond_score_threshold());
			if (_gs.size() > 0) {
				GridGroup _mg = _gs.get(0);
				StringBuffer print = gridClusterService.print(_mg);
				logout.info(key.getFirstKey().toString() + " 第二次聚合，取第一个:");
				logout.info(print.toString());
				TopPointer tp = analyTopPointer(_mg.allPointers(), totalCount, _time);
				if (tp != null) {
					if (_type == UserlifeService.TYPE_HOME) {
						tp.setType("home");
					} else if (_type == UserlifeService.TYPE_WORK) {
						tp.setType("work");
					} else {
						tp.setType("fun");
					}
					tp.setX(String.valueOf(_mg.getX()));
					tp.setY(String.valueOf(_mg.getY()));
					tp.setNeighbors(_mg.getNeighborsMartx());
					tp.setNeighborTxt(print.toString());
					tps.add(tp);
				}
			}
		}

		analyse(tps);
		sortTopPointers(tps);
		return subResult(tps);
	}
}
