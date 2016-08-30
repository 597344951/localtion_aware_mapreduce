package com.zltel.common.main;

import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.utils.conf.ConfigUtil;

/**
 * 基础 M/R Job定义父类，提供从配置文件加载基础参数的功能
 * 
 * @author Wangch
 *
 */
public class SuperMRJob {
	private static Logger logout = LoggerFactory.getLogger(SuperMRJob.class);
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
		setReduceCount(job, 30);
	}

	/**
	 * 设置Job Reduce 数量
	 * 
	 * @param job
	 * @param default_count
	 *            如果没有使用配置文件，则使用 reduce个数
	 */
	public static void setReduceCount(Job job, int default_count) {
		if (default_count < 0) {
			default_count = 30;
		}

		String rc = ConfigUtil.getConfigValue(_confmap, "reduce_count", String.valueOf(default_count));
		logout.info("设置 Reduce Count:" + rc);
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
