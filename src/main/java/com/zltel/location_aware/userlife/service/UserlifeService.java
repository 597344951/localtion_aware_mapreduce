package com.zltel.location_aware.userlife.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zltel.common.utils.hbase.HBaseUtil;
import com.zltel.common.utils.hbase.HBaseUtil.ColumnData;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.common.utils.time.DateTimeUtil;
import com.zltel.dbscan.bean.Point;
import com.zltel.dbscan.service.Dbscan;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.bean.TopPointer;

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
	private static final int MERGETIME = 60 * 1000;
	/** 统计时间 临界值 **/
	public static int TIMERANGE = 10 * 1000;
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

	public static void sortPointers(List<Pointer> pointers) {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		Collections.sort(pointers, getComparator());
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
	 * 是否 相同
	 * 
	 * @param p1
	 * @param p2
	 * @return
	 */
	public static boolean isSame(Pointer p1, Pointer p2) {
		if (p1.getCi().equals(p2.getCi()) && p1.getSource().equals(p2.getSource())) {
			if (Math.abs(p2._stime().getTime() - p1._etime().getTime()) <= MERGETIME) {
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
	 */
	public static List<TopPointer> analyseHome(List<Pointer> pointers) {
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
		return preanalyse(ps);
	}

	/**
	 * 计算工作地点
	 * 
	 * @param pointers
	 */
	public static List<TopPointer> analyseWork(List<Pointer> pointers) {
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
		return preanalyse(ps);
	}

	/**
	 * 计算 娱乐地点
	 * 
	 * @param pointers
	 * @return
	 */
	public static List<TopPointer> analyseFun(List<Pointer> pointers) {
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
		return preanalyse(ps);
	}

	private static List<TopPointer> preanalyse(List<Pointer> ps) {
		List<TopPointer> toppts = new ArrayList<TopPointer>();
		List<List<Point>> points = Dbscan.getInstince().getResult(ps);
		logout.debug(" DBSCAN 聚合点结果:" + points.size() + " \n" + JSON.toJSONString(points));

		for (int gi = 0; gi < points.size(); gi++) {
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

				// 设定 新的需要保留的数据
				/*
				 * Pointer __p = new Pointer(); __p.setScore(score);
				 * __p.setLat(_per.getLat()); __p.setLng(_per.getLng());
				 * __p.setCi(_per.getCi()); __p.setTime(_per.timeRange());
				 */
				_per.setScore(score);
				_pers.add(_per);
			}
			// logout.info("聚合点 分数：" + totalScore + " ,点数:" + _pers.size());
			TopPointer tp = new TopPointer();
			tp.setPointers(_pers);
			tp.setScore(totalScore);
			toppts.add(tp);
		}
		analyse(toppts);
		sortTopPointers(toppts);

		// tp.setPointers(sortAndSubLength(_pers));
		return subResult(toppts);
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
			List<Pointer> ps = tp.getPointers();
			for (Pointer p : ps) {
				Pointer _p = ciMap.get(p.getCi());
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
			List<Pointer> _pts = tp.getPointers();
			if (_pts != null && _pts.size() > 0) {
				cds.add(new ColumnData("info", "points", JSON.toJSONString(tp.getPointers())));
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
			}
		}
		List<Pointer> list = new ArrayList<Pointer>(map.values());
		int ls = list.size();
		sortPointersByScore(list);
		return list.subList(0, ls > MAX_POINTER_COUNT ? MAX_POINTER_COUNT : ls);
	}

	public static void main(String[] args) {
		String s = JSON.toJSONString(null);
		System.out.println(s);
	}
}
