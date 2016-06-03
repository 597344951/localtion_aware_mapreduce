/**
 * Project Name:BigCount
 * File Name:GnTask.java
 * Package Name:xx.local.mr.upfile
 * Date:2016年3月30日下午7:26:51
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.localUp;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * ClassName:GnTask <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年3月30日 下午7:26:51 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class GnTask implements Runnable {
	String dated = "";
	int h = 0;
	String isHdTask = "";

	public String getDated() {
		return dated;
	}

	public void setDated(String dated) {
		this.dated = dated;
	}

	public GnTask() {
	}

	public GnTask(String dated, int h, String isHdTask) {

		this.dated = dated;
		this.h = h;
		this.isHdTask = isHdTask;
	}

	public int getH() {
		return h;
	}

	public void setH(int h) {
		this.h = h;
	}

	public String getIsHdTask() {
		return isHdTask;
	}

	public void setIsHdTask(String isHdTask) {
		this.isHdTask = isHdTask;
	}

	public void run() {
		try {
			String gnmsg = "/home/zltel/msg/" + dated.replaceAll("-", "") + "_"
					+ h + "_gn.txt";
			File filemsg = new File(gnmsg);
			boolean isCon = false;
			for (int i = 0; i < 600; i++) {
				if (filemsg.exists()) {
					isCon = true;
					break;
				} else {
					Thread.sleep(60 * 1000);
				}
				if (i % 60 == 0) {
					System.out.println("gn_" + dated.replaceAll("-", "") + "_"
							+ h + "一个小时");
				}
			}
			if (isCon) {
				if ("1".equals(isHdTask) || "3".equals(isHdTask)) {
					Configuration conf = new Configuration();
					String locatpath = "/home/zltel/data/gn/"
							+ dated.replaceAll("-", "") + "/"
							+ String.format("%02d", h);
					String hfPath = "/hfdata/gn_zs/"
							+ dated.replaceAll("-", "");
					String hPath = hfPath + "/" + String.format("%02d", h)
							+ "/";
					File file = new File(locatpath);
					if (!file.exists()) {
						System.err.println("no exists:" + locatpath);
					} else {
						FileSystem fs = FileSystem.get(conf);
						if (!fs.exists(new Path(hfPath))) {
							fs.mkdirs(new Path(hfPath));
						}
						// if (fs.exists(new Path(hPath))) {
						// fs.delete(new Path(hPath), true);
						// fs.mkdirs(new Path(hPath));
						// }
						if (!fs.exists(new Path(hPath))) {
							fs.mkdirs(new Path(hPath));
						}
						File[] files = file.listFiles();
						if (files != null && files.length > 0) {
							for (int i = 0; i < files.length; i++) {
								File f = files[i];
								if (f.isFile()) {
									fs.copyFromLocalFile(
											new Path(f.toString()), new Path(
													hPath));
									System.out.println("copy from: "
											+ f.toString() + " to " + hfPath
											+ "/" + String.format("%02d", h));
									f.delete();
								}

							}
						}
						file.delete();
						fs.close();
					}
				}
				if ("2".equals(isHdTask) || "3".equals(isHdTask)) {
					Configuration conf = HBaseConfiguration.create();
					conf.set("hbase.zookeeper.quorum", "nn1,view,dn1");
					conf.set("hbase.zookeeper.property.clientPort", "2181");
					conf.set("hbase.master", "nn1:60000");
					conf.set("hbase.zookeeper.dns.nameserver", "nn1");
					String nd = dated.replaceAll("-", "");
					String tabName = "hf_gnql_" + nd;
					String mytable = "1," + tabName;
					int redNum = 24;
					createTable(tabName, conf, h);
					conf.set("mytable", mytable);
					Job job = Job.getInstance(conf,
							"gnqlImp" + dated.replaceAll("-", "") + "/"
									+ String.format("%02d", h));
					job.getConfiguration().setStrings(
							"mapreduce.reduce.shuffle.memory.limit.percent",
							"0.2");
					job.setJarByClass(GnTask.class);
					job.setMapperClass(DataMap.class);
					job.setReducerClass(DataReduce.class);
					job.setOutputFormatClass(MultiTableOutputFormat.class);
					job.setMapOutputKeyClass(Text.class);
					job.setMapOutputValueClass(Text.class);
					job.setNumReduceTasks(redNum);
					String hfPath = "/hfdata/gn_zs/"
							+ dated.replaceAll("-", "") + "/"
							+ String.format("%02d", h) + "/";
					System.out.println("hfPath--" + hfPath);
					FileInputFormat.addInputPath(job, new Path(hfPath));
					job.waitForCompletion(true);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			MainJob.threadLeftOp("-");
		}
	}

	public static void createTable(String tableNames, Configuration conf, int h)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
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
			// if (h == 0) {
			// byte[][] by1 = null;
			// by1 = new byte[100][];
			// for (int i = 0; i < 10; i++) {
			// for (int j = 0; j < 10; j++) {
			// String n = i + "" + j;
			// by1[Integer.parseInt(n)] = n.getBytes();
			// }
			// }
			// admin.createTable(tableDescr, by1);
			// }
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
