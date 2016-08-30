package com.zltel.location_aware.ci_threshold.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.main.SuperMRJob;
import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.common.utils.hbase.HBaseUtil;
import com.zltel.location_aware.ci_threshold.bean.CiCount;
import com.zltel.location_aware.ci_threshold.map.CiThresholdMap;
import com.zltel.location_aware.ci_threshold.reduce.CiThresholdReduceToMySql;
import com.zltel.location_aware.ci_threshold.service.CiThresholdService;

public class CiThresholdMainToMySql extends SuperMRJob {
	/** 是否DEBUG 模式运行，正式运行时 改为false **/
	public static boolean RUN_DEBUG = true;

	private static Logger logout = LoggerFactory.getLogger(CiThresholdMainToMySql.class);

	private static final String TABLE_START_STR = "hf_hot5";
	/** 作为门限值 时的上浮 幅度 **/
	public static final double ThresholdPercent = 1.2;

	// --------------------
	private static final String hbase = "dn1:60000";
	private static final String zookeeper = "dn1";
	// --------------------

	public static final String table_name = "threshold_default";
	public static final String[] table_fields = { "ci", "twoG_bytes", "threeG_bytes", "fourG_bytes", "twoG_people",
			"threeG_people", "fourG_people", "saveTime" };

	public static final String VERSION = "1.0  2016.8.25";

	/**
	 * DEBUG模式运行 返回配置
	 * 
	 * @return 本地集群设置后的信息
	 */
	public static Configuration initConf() {
		Map<String, String> _map2 = ConfigUtil.resolveConfigProFile(CiThresholdMainToMySql.class, "hadoop.properties");
		// String _hdfs = ConfigUtil.getConfigValue(_map2, "fs.defaultFS",
		// hdfs);
		String _hbase = ConfigUtil.getConfigValue(_map2, "hbase.master", hbase);
		String _zookeeper = ConfigUtil.getConfigValue(_map2, "hbase.zookeeper.quorum", zookeeper);

		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", _zookeeper);
		// conf.set("fs.defaultFS", hdfs);
		conf.set("hbase.master", _hbase);
		return conf;
	}

	/**
	 * 加载 数据库访问配置信息（数据库驱动门，连接url，用户名，密码等）
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void initMySql(Configuration conf) throws Exception {
		Map<String, String> map = ConfigUtil.resolveConfigProFile(CiThresholdMainToMySql.class, "jdbc.properties");
		logout.info("读取jdbc.properties文件：\n" + map);

		String driveName = ConfigUtil.getConfigValue(map, "jdbc.mysql.driver", null);
		String url = ConfigUtil.getConfigValue(map, "jdbc.mysql.url", null);
		String un = ConfigUtil.getConfigValue(map, "jdbc.mysql.username", "root");
		String pwd = ConfigUtil.getConfigValue(map, "jdbc.mysql.password", "123456");

		String ip = ConfigUtil.getConfigValue(map, "jdbc.mysql.ip", null);
		String port = ConfigUtil.getConfigValue(map, "jdbc.mysql.port", "3307");
		String ins = ConfigUtil.getConfigValue(map, "jdbc.mysql.instance", null);// hadoop_date?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull
		StringBuffer sb = new StringBuffer();
		sb.append("加载数据库参数").append("\n");
		sb.append("\t").append("ip:").append(ip).append("\n");
		sb.append("\t").append("port:").append(port).append("\n");
		sb.append("\t").append("instince:").append(ins).append("\n");
		sb.append("\t").append("driveName:").append(driveName).append("\n");
		sb.append("\t").append("url:").append(url).append("\n");
		sb.append("\t").append("username:").append(un).append("\n");
		sb.append("\t").append("password:").append(pwd).append("\n");

		logout.info("\n" + sb.toString());
		String nurl = url.replace("${jdbc.mysql.ip}", ip).replace("${jdbc.mysql.port}", port)
				.replace("${jdbc.mysql.instance}", ins);
		sb.append("\t").append("conUrl:").append(nurl).append("\n");
		// logout.info("\n" + sb.toString());

		logout.info("\t connection url:" + nurl);
		DBConfiguration.configureDB(conf, driveName, nurl, un, pwd);
	}

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		printInfo();
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
			Job job = Job.getInstance(conf, "小区门限值计算");
			job.setJarByClass(CiThresholdMainToMySql.class);

			// 此句 在发布的时候删除
			TableMapReduceUtil.addDependencyJars(job);
			List<Scan> scans = createScans(job.getConfiguration());
			// 加载多张表的数据
			TableMapReduceUtil.initTableMapperJob(scans, CiThresholdMap.class, Text.class, CiCount.class, job);
			job.setReducerClass(CiThresholdReduceToMySql.class);
			job.setOutputValueClass(Text.class);

			// 设置 数据库输出的表名 和 字段列
			DBOutputFormat.setOutput(job, table_name, table_fields);
			//
			if (RUN_DEBUG) {
				job.setNumReduceTasks(10);
			} else {
				setReduceCount(job, 20);
			}

			job.waitForCompletion(true);
			logout.info("删除比：" + saveTime + " 早的数据");
			CiThresholdService.deleteOldData(saveTime);
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

	private static void printInfo() {
		StringBuffer sb = new StringBuffer();
		sb.append("程序名：计算小区历史门限值程序").append("\n");
		sb.append("版本号：" + VERSION).append("\n");
		sb.append("程序描述：通过扫描Hbase中以" + TABLE_START_STR
				+ "开头的表，计算每一个小区对应的平均2/3/4G数据流量和平均2/3/4G使用人数统计.在此基础上，上浮20%作为告警门限值。此程序每天运行一次。").append("\n");

		logout.info("\n" + sb.toString());
	}
}
