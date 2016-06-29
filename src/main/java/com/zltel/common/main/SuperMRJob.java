package com.zltel.common.main;

import java.util.Map;

import org.apache.hadoop.mapreduce.Job;

import com.zltel.common.utils.conf.ConfigUtil;

/**
 * 基础 M/R Job定义父类，提供从配置文件加载基础参数的功能
 * 
 * @author Wangch
 *
 */
public class SuperMRJob {
	/**
	 * 基础任务 配置文件
	 */
	private static final Map<String, String> _confmap = ConfigUtil.resolveConfigProFile("job.properties");

	/**
	 * 设置 Job Reduce数量
	 * 
	 * @param job
	 */
	public static void setReduceCount(Job job) {
		String rc = ConfigUtil.getConfigValue(_confmap, "reduce_count", "30");
		job.setNumReduceTasks(Integer.valueOf(rc));
	}

	/**
	 * 加载 Conf配置
	 * 
	 * @param job
	 */
	public static void initConf(Job job) {

	}

}
