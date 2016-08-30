package com.zltel.location_aware.ci_threshold.main;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.mapred.db.main.DBReadMapRed;
import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.location_aware.ci_threshold.db.MyDBOperator;
import com.zltel.location_aware.ci_threshold.map.ReadFromMySqlMap;

/**
 * 从MySql读取数据
 * 
 * @author Wangch
 *
 */
public class ReadFromMySql extends DBReadMapRed {
	/** 是否DEBUG 模式运行，正式运行时 改为false **/
	public static boolean RUN_DEBUG = true;

	private static Logger logout = LoggerFactory.getLogger(ReadFromMySql.class);

	private static final String table_name = "Default_Threshold";
	private static final String[] table_fields = { "ci", "twoG_bytes", "threeG_bytes", "fourG_bytes", "twoG_people",
			"threeG_people", "fourG_people", "saveTime" };

	public static final String VERSION = "1.0  2016.8.25";

	static Map<String, String> _map = ConfigUtil.resolveConfigProFile("jdbc.properties");

	/**
	 * DEBUG模式运行 返回配置
	 * 
	 * @return 本地集群设置后的信息
	 */
	public static Configuration initConf() {
		Configuration conf = new Configuration();
		return conf;
	}

	/**
	 * 加载 数据库访问配置信息（数据库驱动门，连接url，用户名，密码等）
	 * 
	 * @param conf
	 */
	public static void initMySql(Configuration conf) {
		initDBConf(conf, _map);
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		long saveTime = System.currentTimeMillis();
		long _time = System.currentTimeMillis();
		Configuration conf = RUN_DEBUG ? initConf() : new Configuration();
		// map reduce 错误 数占总数的 百分比（0-100）
		conf.set("mapred.max.map.failures.percent", "10");
		conf.set("mapred.max.reduce.failures.percent", "10");
		conf.set("test", "test");
		conf.setLong("saveTime", saveTime);
		initMySql(conf);

		try {
			Job job = Job.getInstance(conf, "从数据库读取数据");
			job.setJarByClass(ReadFromMySql.class);
			// 此句 在发布的时候删除
			job.setMapperClass(ReadFromMySqlMap.class);
			setDBIn(job, MyDBOperator.class, table_name, table_fields);
			job.setNumReduceTasks(0);
			FileOutputFormat.setOutputPath(job, new Path("out"));
			job.waitForCompletion(true);
		} catch (Exception e) {
			logout.info("执行出错:" + e.getMessage(), e);
		}
		_time = System.currentTimeMillis() - _time;
		logout.info("耗时：" + (_time / 1000 / 1) + " s");
	}

}
