package com.zltel.location_aware.ci_threshold.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zltel.location_aware.ci_threshold.bean.CiCount;
import com.zltel.location_aware.ci_threshold.bean.ThresholdResult;
import com.zltel.location_aware.ci_threshold.db.MySqlWritable;
import com.zltel.location_aware.ci_threshold.main.CiThresholdMainToMySql;

public class CiThresholdReduceToMySql extends Reducer<Text, CiCount, MySqlWritable, MySqlWritable> {
	/** 运行模式 **/
	private String outModel = null;
	private long saveTime = 0l;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<CiCount> iterators,
			Reducer<Text, CiCount, MySqlWritable, MySqlWritable>.Context context)
			throws IOException, InterruptedException {
		String cellId = key.toString();
		Map<Integer, Long> dataCount = new HashMap<Integer, Long>();
		Map<Integer, Long> userCount = new HashMap<Integer, Long>();
		long total = 0;
		for (CiCount ciCount : iterators) {
			int nt = ciCount.netType;
			if (CiCount.NET_TYPE_2 <= nt && nt <= CiCount.NET_TYPE_4) {
				Long dv = dataCount.get(nt);
				Long uv = userCount.get(nt);
				dv = dv == null ? 0 : dv;
				uv = uv == null ? 0 : uv;

				dv += (ciCount.down_bytes + ciCount.up_bytes);
				uv += ciCount.userCount;
				dataCount.put(nt, dv);
				userCount.put(nt, uv);
				total++;
			}
		}
		if (total == 0) {
			return;
		}
		ThresholdResult tr = new ThresholdResult();
		tr.ci = cellId;
		tr.saveTime = saveTime;
		for (int nt = CiCount.NET_TYPE_2; nt <= CiCount.NET_TYPE_4; nt++) {
			Long dv = dataCount.get(nt);
			Long uv = userCount.get(nt);
			Long _dv = 0l;
			Long _uv = 0l;
			if (dv != null) {
				_dv = (long) ((dv / total) * CiThresholdMainToMySql.ThresholdPercent);
			}
			if (uv != null) {
				_uv = (long) ((uv / total) * CiThresholdMainToMySql.ThresholdPercent);
			}
			if (nt == CiCount.NET_TYPE_2) {
				tr.twoG_bytes = _dv;
				tr.twoG_count = _uv;
			} else if (nt == CiCount.NET_TYPE_3) {
				tr.threeG_bytes = _dv;
				tr.threeG_count = _uv;
			} else if (nt == CiCount.NET_TYPE_4) {
				tr.fourG_bytes = _dv;
				tr.fourG_count = _uv;
			}
		}
		//
		context.write(new MySqlWritable(tr), null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.
	 * Reducer.Context)
	 */
	@Override
	protected void setup(Reducer<Text, CiCount, MySqlWritable, MySqlWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		outModel = conf.get("outModel");
		saveTime = conf.getLong("saveTime", 0l);
		String test = conf.get("test");

	}

}
