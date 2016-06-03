package com.zltel.location_aware.userlife.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.location_aware.userlife.map.UserLifeBinJiangImportMap;

public class UserLifeBinJiangImportMain {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangImportMain.class);
	public static String writeTable = "userlife";

	public static Configuration initConf() {
		Configuration conf = new Configuration();
		conf.set("mapreduce.map.failures.maxpercent", String.valueOf("100"));
		conf.set("mapreduce.reduce.failures.maxpercent", String.valueOf("100"));
		return conf;
	}

	/**
	 * 
	 * @param args
	 *            1. 导入路径 2. 导入表 3. 是否保存 点(0不保存，其它保存)
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		logout.info("---------------- UserLife Tags Mark  ----------------");
		logout.info("  version: BingJiang Hadoop Import Data ver");
		logout.info("     time: 2016.5.19");
		logout.info("---------------- UserLife Tags Mark  ----------------");

		Configuration _conf = initConf();
		if (args.length <= 0) {
			throw new RuntimeException("需要输入导入文件路径");
		}
		if (args.length >= 2) {
			logout.info(" 设置导入数据表:" + args[1]);
			writeTable = args[1];
		}
		if (args.length >= 3) {
			logout.info(" 保存 其它点:" + args[2]);
			_conf.set("savePoints", args[2]);
		}
		_conf.set(TableOutputFormat.OUTPUT_TABLE, writeTable);
		// JobConfigUtil.initComConf(_conf);
		Job job = Job.getInstance(_conf, "userlife tags import");
		try {

			String Path = args[0];
			logout.info("addPath: " + Path);
			FileInputFormat.addInputPath(job, new Path(Path));

			job.setJarByClass(UserLifeBinJiangImportMain.class);
			// 设置Map/Reduce 处理类
			job.setMapperClass(UserLifeBinJiangImportMap.class);
			// 设置Map/Reduce 中间结果指
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(Put.class);
			job.setOutputFormatClass(TableOutputFormat.class);

			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
			logout.info("UserLife 导入数据完成 ");
		} finally {

		}
	}
}
