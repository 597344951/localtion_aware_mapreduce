package com.zltel.location_aware.userlife.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用户 标签 测试数据 生成
 * 
 * @author Wangch
 *
 */
public class UserLifeBinJiangTestDataReduce extends Reducer<Text, Text, Text, Text> {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangTestDataReduce.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.
	 * Reducer.Context)
	 */
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> iterator, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		logout.info("start reduce----------------------------------");
		for (Text value : iterator) {
			context.write(null, value);
		}
	}

}
