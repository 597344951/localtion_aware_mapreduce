package com.zltel.location_aware.ci_threshold.main;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.common.utils.hbase.HBaseUtil;
import com.zltel.location_aware.ci_threshold.bean.CiCount;
import com.zltel.location_aware.ci_threshold.map.CiThresholdMap;
import com.zltel.location_aware.ci_threshold.reduce.CiThresholdReduce;

public class CiThresholdMain {
	/** 是否DEBUG 模式运行 **/
	public static boolean RUN_DEBUG = true;

	private static Logger logout = LoggerFactory.getLogger(CiThresholdMain.class);
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	private static final String OUT_MODEL_FILE = "file";
	private static final String OUT_MODEL_DATABASE = "database";

	private static final String TABLE_START_STR = "hf_hot5";

	// --------------------
	private static final String hdfs = "hdfs://dn1:9000";
	private static final String hbase = "dn1:60000";
	private static final String zookeeper = "dn1";
	// --------------------

	/**
	 * DEBUG模式运行 返回配置
	 * 
	 * @return 本地集群设置后的信息
	 */
	public static Configuration initConf() {
		Map<String, String> _map = ConfigUtil.resolveConfigProFile("hadoop.properties");
		String _hdfs = ConfigUtil.getConfigValue(_map, "fs.defaultFS", hdfs);
		String _hbase = ConfigUtil.getConfigValue(_map, "hbase.master", hbase);
		String _zookeeper = ConfigUtil.getConfigValue(_map, "hbase.zookeeper.quorum", zookeeper);

		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", zookeeper);
		// conf.set("fs.defaultFS", hdfs);
		conf.set("hbase.master", hbase);
		return conf;
	}

	/**
	 * <ol>
	 * <li>输出模式(file,database)</li>
	 * <li>输出路径</li>
	 * </ol>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		long _time = System.currentTimeMillis();
		Configuration conf = RUN_DEBUG ? initConf() : new Configuration();
		// map reduce 错误 数占总数的 百分比（0-100）
		conf.set("mapred.max.map.failures.percent", "10");
		conf.set("mapred.max.reduce.failures.percent", "10");
		try {
			String outModel = null;
			String outPath = null;

			if (args.length < 1) {
				throw new RuntimeException("需要传入输出模式(file:database)");
			} else {
				outModel = OUT_MODEL_FILE.equals(args[0]) ? OUT_MODEL_FILE : OUT_MODEL_DATABASE;
				conf.set("outModel", outModel);
			}
			if (args.length < 2) {
				throw new RuntimeException("需要一个文件保存路径");
			} else {
				outPath = args[1];
			}

			FileSystem fs = FileSystem.get(conf);
			Path pout = new Path(outPath);
			if (fs.exists(pout)) {
				fs.delete(pout, true);
				logout.info(outPath + "  this path exists, deleting......");
			}

			Job job = Job.getInstance(conf, "小区门限值计算");
			job.setJarByClass(CiThresholdMain.class);

			// 此句 在发布的时候删除
			TableMapReduceUtil.addDependencyJars(job);
			List<Scan> scans = createScans(job.getConfiguration());
			// 加载多张表的数据
			TableMapReduceUtil.initTableMapperJob(scans, CiThresholdMap.class, Text.class, CiCount.class, job);
			job.setReducerClass(CiThresholdReduce.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			job.setNumReduceTasks(1);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			logout.info("执行出错:" + e.getMessage(), e);
		}
		_time = System.currentTimeMillis() - _time;
		logout.info("耗时：" + (_time / 1000 / 60) + " min");
	}

	private static List<Scan> createScans(Configuration conf) throws IOException {
		List<Scan> scans = new ArrayList<Scan>();
		Scan scan = null;
		Connection con = null;
		Admin admin = null;
		try {
			con = HBaseUtil.getConnection(conf);
			admin = con.getAdmin();
			logout.info("扫描目标表");
			TableName[] tableNames = admin.listTableNames();
			for (TableName tn : tableNames) {
				String _tn = tn.getNameAsString();
				if (_tn.startsWith(TABLE_START_STR)) {
					scan = new Scan();
					scan.setCaching(500);
					scan.setCacheBlocks(false);
					scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tn.getName());
					if (RUN_DEBUG) {
						scan.setStartRow("050".getBytes());
						scan.setStopRow("090".getBytes());
					}
					scans.add(scan);
					logout.info("\t加入表:" + tn);
				}
			}

		} finally {
			HBaseUtil.closeAdmin(admin);
			HBaseUtil.close(con);
		}
		return scans;
	}
}
