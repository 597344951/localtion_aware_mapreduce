package com.zltel.dataexport.main;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.common.utils.hbase.HBaseConfigUtil;
import com.zltel.common.utils.hbase.HBaseUtil;
import com.zltel.dataexport.map.DataExportMap;
import com.zltel.dataexport.reduce.DataExportReduce;
import com.zltel.location_aware.userlife.main.UserLifeMain;

public class DataExportMain {
	private static Logger log = LoggerFactory.getLogger(DataExportMain.class);

	private static String hdfs = "hdfs://nn1:9000";
	private static String hbase = "nn1:60000";
	private static String zookeeper = "view,nn1,dn1";

	public static Configuration initConf() {
		Map<String, String> _map = ConfigUtil.resolveConfigProFile("hadoop.properties");
		hdfs = ConfigUtil.getConfigValue(_map, "fs.defaultFS", hdfs);
		hbase = ConfigUtil.getConfigValue(_map, "hbase.master", hbase);
		zookeeper = ConfigUtil.getConfigValue(_map, "hbase.zookeeper.quorum", zookeeper);

		Configuration conf = HBaseConfigUtil.createConfig(hdfs, hbase, zookeeper);
		return conf;
	}

	/**
	 * 
	 * @param args
	 *            1: 表名 2: startRow 3: endRow 4: 输出路径
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		log.info("=========================");
		log.info("=     --数据导出程序--");
		log.info("=       2016.5.3");
		log.info("=========================");

		Configuration conf = initConf();
		conf.set("mapred.max.map.failures.percent", "1");
		conf.set("mapred.max.reduce.failures.percent", "1");
		Job job = Job.getInstance(conf, "exportdata");
		job.setJarByClass(UserLifeMain.class);
		// 此句 在发布的时候删除

		String table = null;
		String startRow = null;
		String endRow = null;
		String output = null;
		if (args.length < 4) {
			log.error("需要4个参数: \n1: 表名 \n2: startRow \n3: endRow \n4: 输出路径");
			return;
		}
		table = args[0];
		startRow = args[1];
		endRow = args[2];
		output = args[3];

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path pout = new Path(output);
		if (fs.exists(pout)) {
			fs.delete(pout, true);
			System.out.println("存在此路径, 已经删除......");
		}
		TableMapReduceUtil.addDependencyJars(job);
		Scan scan = HBaseUtil.createScan(startRow, endRow);
		// 加载多张表的数据
		TableMapReduceUtil.initTableMapperJob(table, scan, DataExportMap.class, Text.class, Text.class, job);
		TableMapReduceUtil.addDependencyJars(job);
		job.setReducerClass(DataExportReduce.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
