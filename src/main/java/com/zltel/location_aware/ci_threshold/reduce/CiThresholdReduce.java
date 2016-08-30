package com.zltel.location_aware.ci_threshold.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.ci_threshold.bean.CiCount;
import com.zltel.location_aware.ci_threshold.bean.ThresholdResult;
import com.zltel.location_aware.ci_threshold.service.CiThresholdService;

public class CiThresholdReduce extends Reducer<Text, CiCount, Text, Text> {
	/** 运行模式 **/
	private String outModel = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<CiCount> iterators, Reducer<Text, CiCount, Text, Text>.Context context)
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
		for (int nt = CiCount.NET_TYPE_2; nt <= CiCount.NET_TYPE_4; nt++) {
			Long dv = dataCount.get(nt);
			Long uv = userCount.get(nt);
			Long _dv = 0l;
			Long _uv = 0l;
			if (dv != null) {
				_dv = dv / total;
			}
			if (uv != null) {
				_uv = uv / total;
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
		saveToDataBase(tr, context);
	}

	/**
	 * 保存入数据库
	 * 
	 * @param tr
	 * @param context
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void saveToDataBase(ThresholdResult tr, Reducer<Text, CiCount, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// context.write(key, new Text(JSON.toJSONString(tr)));
		StringBuffer sb_i = CiThresholdService.createInsertSql(tr);
		StringBuffer sb_d = CiThresholdService.createDeleteSql(tr);
		if (null == sb_i || null == sb_d) {
			return;
		}

		if ("file".equals(outModel)) {
			// 输出SQL到文件
			context.write(null, new Text(sb_i.toString()));
		} else {
			// 输出到 database
			String msg = CiThresholdService.saveToDataBase(sb_d, sb_i);
			if (StringUtil.isNotNullAndEmpty(msg)) {
				context.write(new Text(tr.ci), new Text(msg.toString()));
			}
		}
	}

	private void saveToDataBase(ThresholdResult tr, StringBuffer sql) {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.
	 * Reducer.Context)
	 */
	@Override
	protected void setup(Reducer<Text, CiCount, Text, Text>.Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		outModel = conf.get("outModel");
	}

}
