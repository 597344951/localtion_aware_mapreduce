package com.zltel.location_aware.ci_threshold.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.location_aware.ci_threshold.bean.ThresholdResult;
import com.zltel.location_aware.ci_threshold.db.MyDBOperator;

/**
 * 从 MySql读取数据 map
 * 
 * @author Wangch
 *
 */
public class ReadFromMySqlMap extends Mapper<LongWritable, MyDBOperator, LongWritable, Text> {
	private static Logger logout = LoggerFactory.getLogger(ReadFromMySqlMap.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, MyDBOperator value,
			Mapper<LongWritable, MyDBOperator, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		ThresholdResult tr = value.getValue();

		logout.info(tr.ci);
	}

}
