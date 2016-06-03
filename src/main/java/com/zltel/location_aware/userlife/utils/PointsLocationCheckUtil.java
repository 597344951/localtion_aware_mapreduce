package com.zltel.location_aware.userlife.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.common.utils.sql.BaseJdbcUtil;
import com.zltel.location_aware.userlife.bean.Pointer;

/**
 * 查询 轨迹点的经纬度 工具类
 * 
 * @author Wangch
 *
 */
public class PointsLocationCheckUtil {
	private static Logger logout = LoggerFactory.getLogger(PointsLocationCheckUtil.class);
	static Map<String, String> _map = ConfigUtil.resolveConfigProFile("jdbc.properties");

	public static Map<String, Map<String, Object>> _cacheMap = null;

	public static List<Pointer> check(List<Pointer> pointers) throws ClassNotFoundException, SQLException {
		if (_cacheMap == null) {
			init();
		}
		List<Pointer> ps = new ArrayList<Pointer>();
		for (Pointer pointer : pointers) {
			Pointer p = check(pointer);
			if (p != null)
				ps.add(p);
		}
		return ps;
	}

	public static Pointer check(Pointer pointer) throws ClassNotFoundException, SQLException {
		if (_cacheMap == null) {
			init();
		}
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

	public static void init() throws ClassNotFoundException, SQLException {
		String driveName = ConfigUtil.getConfigValue(_map, "jdbc.mysql.driver", null);
		String url = ConfigUtil.getConfigValue(_map, "jdbc.mysql.url", null);
		String un = ConfigUtil.getConfigValue(_map, "jdbc.mysql.username", null);
		String pwd = ConfigUtil.getConfigValue(_map, "jdbc.mysql.password", null);

		String ip = ConfigUtil.getConfigValue(_map, "jdbc.mysql.ip", null);
		String port = ConfigUtil.getConfigValue(_map, "jdbc.mysql.port", null);
		String ins = ConfigUtil.getConfigValue(_map, "jdbc.mysql.instance", null);

		String nurl = url.replace("${jdbc.mysql.ip}", ip).replace("${jdbc.mysql.port}", port)
				.replace("${jdbc.mysql.instance}", ins);
		Connection con = null;
		try {
			con = BaseJdbcUtil.getLocConnection(driveName, nurl, un, pwd);
			String sql = "select * from gis g";

			List<Map<String, Object>> _cache = BaseJdbcUtil.executeQueryToMap(con, sql);
			logout.info("查询 gis 数据条数: " + _cache.size());
			_cacheMap = new HashMap<String, Map<String, Object>>();
			for (Map<String, Object> m : _cache) {
				Object _ci = m.get("CI");
				if (_ci != null) {
					_cacheMap.put(String.valueOf(_ci).trim(), m);
				}
			}
		} finally {
			BaseJdbcUtil.close(con);
		}
	}
}
