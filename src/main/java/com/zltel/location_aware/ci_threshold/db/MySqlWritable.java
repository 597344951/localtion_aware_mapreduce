package com.zltel.location_aware.ci_threshold.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import com.zltel.location_aware.ci_threshold.bean.ThresholdResult;

/**
 * 定义数据库 的相应 读写操作
 * 
 * @author Wangch
 *
 */
public class MySqlWritable implements DBWritable {
	public ThresholdResult thresholdResult;

	public MySqlWritable() {
		this.thresholdResult = new ThresholdResult();
	}

	public MySqlWritable(ThresholdResult tr) {
		this.thresholdResult = tr;
	}

	/**
	 * 往数据库写数据 操作方法，没有可以不用实现
	 */
	public void write(PreparedStatement statement) throws SQLException {
		ThresholdResult tr = thresholdResult;
		statement.setString(1, tr.ci);
		statement.setLong(2, tr.twoG_bytes);
		statement.setLong(3, tr.threeG_bytes);
		statement.setLong(4, tr.fourG_bytes);

		statement.setLong(5, tr.twoG_count);
		statement.setLong(6, tr.threeG_count);
		statement.setLong(7, tr.fourG_count);
		statement.setLong(8, tr.saveTime);
	}

	/**
	 * 从数据库读取数据 操作方法，没有可以不用 实现
	 */
	public void readFields(ResultSet resultSet) throws SQLException {
		thresholdResult.ci = resultSet.getString(1);
		thresholdResult.twoG_bytes = resultSet.getLong(2);
		thresholdResult.threeG_bytes = resultSet.getLong(3);
		thresholdResult.fourG_bytes = resultSet.getLong(4);
		thresholdResult.twoG_count = resultSet.getLong(5);
		thresholdResult.threeG_count = resultSet.getLong(6);
		thresholdResult.fourG_count = resultSet.getLong(7);
		thresholdResult.saveTime = resultSet.getLong(8);
	}

	public ThresholdResult getValue() {
		return this.thresholdResult;
	}

}