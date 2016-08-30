package com.zltel.location_aware.userlife.utils;

import com.zltel.location_aware.userlife.bean.Pointer;

/**
 * 地理位置
 * 
 * @author Wangch
 *
 */
public class LocalDistince {
	public static int lng_start = 118;
	public static int lng_end = 121;
	public static int lat_start = 29;
	public static int lat_end = 31;
	/** 单位距离100m 的权重值 **/
	public static int weight_100 = 1000;
	/** 单位距离1000m 的权重值 **/
	public static int weight_1000 = 100;

	public static double Distince(Pointer p1, Pointer p2) {

		return Distance(p1.get_lng(), p1.get_lat(), p2.get_lng(), p2.get_lat());
	}

	/**
	 * 计算地球上任意两点(经纬度)距离
	 * 
	 * @param long1
	 *            第一点经度
	 * @param lat1
	 *            第一点纬度
	 * @param long2
	 *            第二点经度
	 * @param lat2
	 *            第二点纬度
	 * @return 返回距离 单位：米
	 */
	public static double Distance(double long1, double lat1, double long2, double lat2) {
		double a, b, R;
		R = 6378137; // 地球半径
		lat1 = lat1 * Math.PI / 180.0;
		lat2 = lat2 * Math.PI / 180.0;
		a = lat1 - lat2;
		b = (long1 - long2) * Math.PI / 180.0;
		double d;
		double sa2, sb2;
		sa2 = Math.sin(a / 2.0);
		sb2 = Math.sin(b / 2.0);
		d = 2 * R * Math.asin(Math.sqrt(sa2 * sa2 + Math.cos(lat1) * Math.cos(lat2) * sb2 * sb2));
		return d;
	}

	public static int getIndex(double d, int step, int weight) {
		int s = 0;
		if (d > 100) {
			// lon
			s = lng_start;
		} else {
			// lat
			s = lat_start;
		}
		s = 0;
		int r = (int) (d * weight - s * weight);
		int r1 = r / step;
		int r2 = r % step;
		return r1 + (r2 > 0 ? 1 : 0);
	}

	/** 0.001 单位100， **/
	public static void main(String[] args) {
		double distince = Distance(120.150, 30.240, 120.155, 30.245);
		System.out.println(distince);

		System.out.println(getIndex(lng_end, 10, 1000));
		System.out.println(getIndex(lng_end, 1, 100));

	}
}
