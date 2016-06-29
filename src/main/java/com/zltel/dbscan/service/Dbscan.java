package com.zltel.dbscan.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.dbscan.bean.Point;
import com.zltel.dbscan.utils.Utility;
import com.zltel.location_aware.userlife.bean.Pointer;

public class Dbscan {
	private static final Map<String, String> _map = ConfigUtil.resolveConfigProFile("userlife.properties");

	private List<Point> pointsList = new ArrayList<Point>();// 存储所有点的集合

	private List<List<Point>> resultList = new ArrayList<List<Point>>();// 存储DBSCAN算法返回的结果集
	/** 聚合半径 大小 **/
	private int e = 500;// e半径
	/** 集合最少包含点数 **/
	private int minp = 5;// 密度阈值

	public static final Dbscan getInstince() {
		String radius = ConfigUtil.getConfigValue(_map, "dbscan.radius", "500");
		String minpt = ConfigUtil.getConfigValue(_map, "dbscan.minpoint", "10");

		Dbscan dbs = new Dbscan();
		dbs.e = Integer.valueOf(radius);
		dbs.minp = Integer.valueOf(minpt);

		return dbs;
	}

	/**
	 * 
	 * 提取文本中的的所有点并存储在pointsList中
	 * 
	 * @throws IOException
	 * 
	 */

	public void display() {

		int index = 1;

		for (Iterator<List<Point>> it = resultList.iterator(); it.hasNext();) {
			List<Point> lst = it.next();
			if (lst.isEmpty()) {
				continue;
			}
			System.out.println("-----第" + index + "个聚类-----");

			for (Iterator<Point> it1 = lst.iterator(); it1.hasNext();) {

				Point p = it1.next();

				System.out.println(p.print());

			}

			index++;

		}

	}

	// 找出所有可以直达的聚类

	private void applyDbscan() {

		// pointsList = Utility.getPointsList();

		for (Iterator<Point> it = pointsList.iterator(); it.hasNext();) {
			Point p = it.next();
			if (!p.isClassed()) {

				List<Point> tmpLst = new ArrayList<Point>();

				if ((tmpLst = Utility.isKeyPoint(pointsList, p, e, minp)) != null) {
					// 为所有聚类完毕的点做标示
					Utility.setListClassed(tmpLst);
					resultList.add(tmpLst);

				}

			}

		}

	}

	// 对所有可以直达的聚类进行合并，即找出间接可达的点并进行合并

	public List<List<Point>> getResult(List<Pointer> pointers) {
		pointsList = new ArrayList<Point>();
		for (Pointer p : pointers) {
			pointsList.add(new Point(p));
		}

		applyDbscan();// 找到所有直达的聚类
		int length = resultList.size();
		for (int i = 0; i < length; ++i) {
			for (int j = i + 1; j < length; ++j) {
				if (Utility.mergeList(resultList.get(i), resultList.get(j))) {
					resultList.get(j).clear();
				}
			}
		}
		return resultList;
	}

	/**
	 * 
	 * 程序主函数
	 * 
	 * @param args
	 * 
	 */
	public static void main(String[] args) {
		Dbscan db = Dbscan.getInstince();

	}

}