package com.zltel.location_aware.ci_threshold.map;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.ci_threshold.bean.CiCount;

public class CiThresholdMap extends TableMapper<Text, CiCount> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(ImmutableBytesWritable key, Result values,
			Mapper<ImmutableBytesWritable, Result, Text, CiCount>.Context context)
			throws IOException, InterruptedException {
		String ci = null;
		String nettype = null;
		String down_load = null;
		String up_load = null;
		String userCount = null;
		for (Cell rowKV : values.rawCells()) {
			String f = new String(CellUtil.cloneFamily(rowKV));// family
			String q = new String(CellUtil.cloneQualifier(rowKV));// qualifer
			String v = new String(CellUtil.cloneValue(rowKV)); // 值
			String rowkey = new String(CellUtil.cloneRow(rowKV));
			String[] ss = rowkey.split("_");
			if (ss.length >= 4 && StringUtil.isNullOrEmpty(nettype, ci)) {
				String t = ss[0]; // 时间：00000000
				String dt = ss[1];// 日期 ： 20160102
				nettype = ss[2];// 网络类型
				ci = ss[3];// CI
			}
			if ("d".equals(q)) {
				down_load = v;
			} else if ("u".equals(q)) {
				up_load = v;
			} else if ("um".equals(q)) {
				userCount = v;
			}
		}
		if (StringUtil.isNotNullAndEmpty(nettype, ci)) {
			if (StringUtil.isNum(nettype)) {
				int nt = Integer.valueOf(nettype);
				if (CiCount.NET_TYPE_2 <= nt && nt <= CiCount.NET_TYPE_4) {
					CiCount ciCount = new CiCount();
					ciCount.ci = ci;
					ciCount.netType = nt;
					if (StringUtil.isNotNullAndEmpty(down_load) && StringUtil.isNum(down_load)) {
						ciCount.down_bytes = Long.valueOf(down_load);
					}
					if (StringUtil.isNotNullAndEmpty(up_load) && StringUtil.isNum(up_load)) {
						ciCount.up_bytes = Long.valueOf(up_load);
					}
					if (StringUtil.isNotNullAndEmpty(userCount) && StringUtil.isNum(userCount)) {
						ciCount.userCount = Long.valueOf(userCount);
					}
					context.write(new Text(ci), ciCount);
				}
			}
		}
	}
}
