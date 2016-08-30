package com.zltel.location_aware.userlife.main;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.utils.hbase.HBaseUtil;
import com.zltel.location_aware.userlife.map.UserLifeBinJiangImportMap;

public class UserLifeBinJiangImportMain {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangImportMain.class);
	public static String writeTable = "userlife";
	/**是否运行于调试模式**/
	public static boolean RUN_DEBUG = false;

	public static Configuration initConf() {
		Configuration conf = new Configuration();
		conf.set("mapreduce.map.failures.maxpercent", String.valueOf("100"));
		conf.set("mapreduce.reduce.failures.maxpercent", String.valueOf("100"));
		return conf;
	}

	/**
	 * 
	 * @param args
	 *            1. 导入路径 2. 导入表 3. 是否保存 点(0不保存，其它保存)4.Reduce数量 5.
	 *            保存月份数，默认保存上一月份
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		logout.info("---------------- UserLife Tags Mark  ----------------");
		logout.info("  version: BingJiang Hadoop Import Data ver");
		logout.info("     time: 2016.5.19");
		logout.info("---------------- UserLife Tags Mark  ----------------");

		int mapsize = -1;
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
		if (args.length >= 4) {
			logout.info(" 设置 Map运行数目:" + args[3]);
			mapsize = Integer.valueOf(args[3]);
		}
		if (args.length >= 5) {
			logout.info(" 输入 导入的月份数: " + args[4]);
			_conf.set("SAVE_MONTH", String.valueOf(Integer.valueOf(args[4])));
		}
		if (!RUN_DEBUG)
			checkTable(writeTable, _conf);

		_conf.set(TableOutputFormat.OUTPUT_TABLE, writeTable);
		String Path = args[0];
		if (mapsize > 0) {
			FileSystem hdfs = FileSystem.get(URI.create(Path), _conf);
			FileStatus[] fs = hdfs.listStatus(new Path(args[0]));
			Path[] listPath = FileUtil.stat2Paths(fs);
			logout.info("限制资源数导入,总输入个数:" + listPath.length);
			int rc = listPath.length / mapsize + (listPath.length % mapsize > 0 ? 1 : 0);
			logout.info("限制执行Map数:" + mapsize + " ,执行次数:" + rc);
			String idx = "";
			for (int _i = 0; _i < rc; _i++) {
				idx = (_i + 1) + "/" + rc;
				logout.info(" 执行 第 " + idx + "次 导入");
				long time = System.currentTimeMillis();
				Job job = Job.getInstance(_conf, "userlife tags import " + idx);
				for (int j = _i * mapsize; j < _i * mapsize + mapsize && j < listPath.length; j++) {
					logout.info(" add File Path: " + listPath[j]);
					FileInputFormat.addInputPath(job, listPath[j]);
				}
				job.setJarByClass(UserLifeBinJiangImportMain.class);
				// 设置Map/Reduce 处理类
				job.setMapperClass(UserLifeBinJiangImportMap.class);
				// 设置Map/Reduce 中间结果指
				job.setOutputKeyClass(ImmutableBytesWritable.class);
				job.setOutputValueClass(Put.class);
				job.setOutputFormatClass(TableOutputFormat.class);

				job.setNumReduceTasks(0);
				job.waitForCompletion(true);
				time = System.currentTimeMillis() - time;
				logout.info(" 第 " + idx + "次 导入  完成 , 耗时: " + (time / 1000 / 60) + "分");
			}

		} else {

			// JobConfigUtil.initComConf(_conf);
			Job job = Job.getInstance(_conf, "userlife tags import");
			long time = System.currentTimeMillis();
			try {
				logout.info("addPath: " + Path);
				//
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
				time = System.currentTimeMillis() - time;
				logout.info("UserLife 导入数据完成   耗时: " + (time / 1000 / 60) + "分");
			} finally {

			}
		}
	}

	private static void checkTable(String writeTable2, Configuration _conf) throws IOException {
		if (HBaseUtil.isTableExists(_conf, writeTable2)) {
			logout.info(writeTable2 + " table 存在");
		} else {
			logout.info("创建新表: " + writeTable2);
			createTable(writeTable2, _conf);
		}

	}

	public static void createTable(String tableNames, Configuration conf)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		TableName userTable = TableName.valueOf(tableNames);
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		HTableDescriptor tableDescr = new HTableDescriptor(userTable);
		HColumnDescriptor family = new HColumnDescriptor("info".getBytes());
		family.setBloomFilterType(BloomType.ROW);
		family.setDataBlockEncoding(DataBlockEncoding.PREFIX);
		family.setInMemory(true);
		family.setCompressionType(Algorithm.SNAPPY);
		family.setCompactionCompressionType(Algorithm.SNAPPY);
		tableDescr.addFamily(family);
		if (admin.tableExists(userTable)) {
			admin.close();
		} else {
			byte[][] by1 = null;
			by1 = new byte[100][];
			for (int i = 0; i < 10; i++) {
				for (int j = 0; j < 10; j++) {
					String n = i + "" + j;
					by1[Integer.parseInt(n)] = n.getBytes();
				}
			}
			admin.createTable(tableDescr, by1);
			admin.close();
		}
	}

}
