package com.zltel.location_aware.ci_threshold.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;

import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.common.utils.date.Date;
import com.zltel.common.utils.sql.BaseJdbcUtil;
import com.zltel.location_aware.ci_threshold.bean.ThresholdResult;
import com.zltel.location_aware.ci_threshold.main.CiThresholdMainToMySql;

public class CiThresholdService {
	static Map<String, String> _map = ConfigUtil.resolveConfigProFile(CiThresholdService.class, "jdbc.properties");

	/**
	 * 创建 inser SQL语句
	 * 
	 * @param tr
	 * @return
	 */
	public static StringBuffer createInsertSql(ThresholdResult tr) {
		StringBuffer sql = new StringBuffer();
		sql.append("INSERT INTO ").append(CiThresholdMainToMySql.table_name).append(" ");
		StringBuffer sql_fields = new StringBuffer();
		StringBuffer sql_values = new StringBuffer();
		if (tr.twoG_bytes > 0) {
			sql_fields.append("twoG_bytes,");
			sql_values.append(handleValue(tr.twoG_bytes) + ",");
		}
		if (tr.twoG_count > 0) {
			sql_fields.append("twoG_people,");
			sql_values.append(handleValue(tr.twoG_count) + ",");
		}

		if (tr.threeG_bytes > 0) {
			sql_fields.append("threeG_bytes,");
			sql_values.append(handleValue(tr.threeG_bytes) + ",");
		}
		if (tr.threeG_count > 0) {
			sql_fields.append("threeG_people,");
			sql_values.append(handleValue(tr.threeG_count) + ",");
		}

		if (tr.fourG_bytes > 0) {
			sql_fields.append("fourG_bytes,");
			sql_values.append(handleValue(tr.fourG_bytes) + ",");
		}
		if (tr.fourG_count > 0) {
			sql_fields.append("fourG_people,");
			sql_values.append(handleValue(tr.fourG_count) + ",");
		}

		if (sql_fields.length() == 0 || sql_values.length() == 0) {
			return null;
		}
		sql_fields.append("ci,");
		sql_values.append(handleValue(tr.ci) + ",");
		sql_fields.append("savetime,");
		sql_values.append(handleValue(new Date().getTime()) + ",");

		sql_fields.deleteCharAt(sql_fields.length() - 1);
		sql_values.deleteCharAt(sql_values.length() - 1);
		sql.append("(").append(sql_fields).append(")");
		sql.append(" values ");
		sql.append("(").append(sql_values).append(") ");
		return sql;
	}

	/**
	 * 创建 delete SQL语句
	 * 
	 * @param tr
	 * @return
	 */
	public static StringBuffer createDeleteSql(ThresholdResult tr) {
		StringBuffer sql = new StringBuffer();
		sql.append("DELETE FROM ").append(CiThresholdMainToMySql.table_name).append(" ");
		;
		sql.append("WHERE ci=" + handleValue(tr.ci));
		return sql;
	}

	public static StringBuffer createDeleteSql(long saveTime) {
		StringBuffer sql = new StringBuffer();
		sql.append("DELETE FROM ").append(CiThresholdMainToMySql.table_name).append(" ");
		sql.append("WHERE saveTime<" + saveTime);
		return sql;
	}

	private static String handleValue(Object twoG_bytes) {
		return "'" + twoG_bytes + "'";
	}

	public static void initMySql(Configuration conf) {
		String driveName = ConfigUtil.getConfigValue(_map, "jdbc.mysql.driver", null);
		String url = ConfigUtil.getConfigValue(_map, "jdbc.mysql.url", null);
		String un = ConfigUtil.getConfigValue(_map, "jdbc.mysql.username", null);
		String pwd = ConfigUtil.getConfigValue(_map, "jdbc.mysql.password", null);

		String ip = ConfigUtil.getConfigValue(_map, "jdbc.mysql.ip", null);
		String port = ConfigUtil.getConfigValue(_map, "jdbc.mysql.port", null);
		String ins = ConfigUtil.getConfigValue(_map, "jdbc.mysql.instance", null);

		String nurl = url.replace("${jdbc.mysql.ip}", ip).replace("${jdbc.mysql.port}", port)
				.replace("${jdbc.mysql.instance}", ins);
		DBConfiguration.configureDB(conf, driveName, nurl, un, pwd);
	}

	/**
	 * 获取 MySql连接
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static Connection getMySqlConnection() throws ClassNotFoundException, SQLException {
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
		con = BaseJdbcUtil.getLocConnection(driveName, nurl, un, pwd);
		return con;
	}

	public static String saveToDataBase(StringBuffer sb_d, StringBuffer sb_i) {
		String msg = "";
		Connection con = null;
		Statement statement = null;
		try {
			con = getMySqlConnection();
			con.setAutoCommit(false);
			statement = con.createStatement();
			statement.addBatch(sb_d.toString());
			statement.addBatch(sb_i.toString());
			statement.executeBatch();
			con.commit();
			con.setAutoCommit(true);
		} catch (Exception e) {
			msg = e.getMessage();
			try {
				con.rollback();
			} catch (SQLException e1) {
				msg += "," + e.getMessage();
			}
		} finally {
			BaseJdbcUtil.close(statement);
			BaseJdbcUtil.close(con);
		}
		return msg;
	}

	public static void deleteOldData(long saveTime) throws ClassNotFoundException, SQLException {
		Connection con = null;
		Statement statement = null;
		try {
			con = getMySqlConnection();
			con.setAutoCommit(false);
			statement = con.createStatement();
			statement.addBatch(createDeleteSql(saveTime).toString());
			statement.executeBatch();
			con.commit();
			con.setAutoCommit(true);
		} finally {
			BaseJdbcUtil.close(statement);
			BaseJdbcUtil.close(con);
		}
	}

}
