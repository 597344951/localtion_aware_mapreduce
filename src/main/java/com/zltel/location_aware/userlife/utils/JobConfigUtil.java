package com.zltel.location_aware.userlife.utils;

import org.apache.hadoop.conf.Configuration;

public class JobConfigUtil {
	/** Map失败百分比 **/
	private static final int MAP_FAIL_PERCENT = 100;
	/** Reduce失败百分比 **/
	private static final int REDUCE_FAIL_PERCENT = 100;
	/** 任务最大超时时间 **/
	private static final Long TASK_TIMEOUT = 15 * 60 * 1000l;

	// private static final int MEM_SIZE = 2048;

	/**
	 * 设置任务 配置基础属性
	 * 
	 * @param conf
	 */
	public static final void initComConf(Configuration conf) {
		conf.set("mapreduce.map.failures.maxpercent", String.valueOf(MAP_FAIL_PERCENT));
		conf.set("mapreduce.reduce.failures.maxpercent", String.valueOf(REDUCE_FAIL_PERCENT));
		conf.set("mapreduce.task.timeout", String.valueOf(TASK_TIMEOUT));

		// 设置压缩
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

		// 设置reduce 堆大小
		// conf.set("mapreduce.admin.reduce.child.java.opts", "-Xmx" + MEM_SIZE
		// + "m");
		// conf.set("mapred.reduce.child.java.opts", "-Xmx" + MEM_SIZE + "m");

	}
}
