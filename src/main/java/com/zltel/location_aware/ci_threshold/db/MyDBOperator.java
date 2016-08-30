package com.zltel.location_aware.ci_threshold.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.zltel.common.mapred.db.dbwriter.DBWriterSupport;
import com.zltel.location_aware.ci_threshold.bean.ThresholdResult;

public class MyDBOperator extends DBWriterSupport<ThresholdResult> {
	public ThresholdResult tr = null;

	public MyDBOperator() {
		tr = new ThresholdResult();
	}

	public MyDBOperator(ThresholdResult tr) {
		this.tr = tr;
	}

	@Override
	public void write(PreparedStatement statement, ThresholdResult v) throws SQLException {
		statement.setString(1, tr.ci);
		statement.setLong(2, tr.twoG_bytes);
		statement.setLong(3, tr.threeG_bytes);
		statement.setLong(4, tr.fourG_bytes);

		statement.setLong(5, tr.twoG_count);
		statement.setLong(6, tr.threeG_count);
		statement.setLong(7, tr.fourG_count);
		statement.setLong(8, tr.saveTime);
	}

	@Override
	public void read(ResultSet resultSet) throws SQLException {
		tr.ci = resultSet.getString(1);
		tr.twoG_bytes = resultSet.getLong(2);
		tr.threeG_bytes = resultSet.getLong(3);
		tr.fourG_bytes = resultSet.getLong(4);
		tr.twoG_count = resultSet.getLong(5);
		tr.threeG_count = resultSet.getLong(6);
		tr.fourG_count = resultSet.getLong(7);
		tr.saveTime = resultSet.getLong(8);
	}

	@Override
	public ThresholdResult getValue() {

		return tr;
	}

}
