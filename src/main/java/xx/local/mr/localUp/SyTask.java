/**
 * Project Name:BigCount
 * File Name:SyTask.java
 * Package Name:xx.local.mr.localUp
 * Date:2016年4月7日下午7:11:54
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
 * ClassName:SyTask <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年4月7日 下午7:11:54 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class SyTask implements Runnable {

	String date = "";
	int h = 0;
	String isHdTask = "";

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public SyTask(String date, int h, String isHdTask) {
		this.date = date;
		this.h = h;
		this.isHdTask = isHdTask;
	}

	public SyTask() {
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

	public static void createTable(String tableName, Configuration conf)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		TableName userTable = TableName.valueOf(tableName);
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		HTableDescriptor tableDescr = new HTableDescriptor(userTable);
		tableDescr.addFamily(new HColumnDescriptor("info".getBytes()));
		HColumnDescriptor family = new HColumnDescriptor("info".getBytes());
		family.setBloomFilterType(BloomType.ROW);
		family.setDataBlockEncoding(DataBlockEncoding.PREFIX);
		family.setInMemory(true);
		family.setCompressionType(Algorithm.SNAPPY);
		family.setCompactionCompressionType(Algorithm.SNAPPY);
		if (admin.tableExists(userTable)) {
			admin.close();
		} else {
			admin.createTable(tableDescr);
			admin.close();
		}

	}

	public void run() {
		try {
			String symsg = "/home/zltel/msg/" + date.replaceAll("-", "") + "_"
					+ h + "_sy.txt";
			File filemsg = new File(symsg);
			boolean isCon = false;
			for (int i = 0; i < 600; i++) {
				if (filemsg.exists()) {
					isCon = true;
					break;
				} else {
					Thread.sleep(60 * 1000);
				}
				if (i % 60 == 0) {
					System.out.println("sy_" + date.replaceAll("-", "") + "_"
							+ h + "一个小时");
				}
			}
			if (isCon) {
				if ("1".equals(isHdTask) || "3".equals(isHdTask)) {
					Configuration conf = new Configuration();
					String locatpath = "/home/zltel/data/sy/"
							+ date.replaceAll("-", "") + "/"
							+ String.format("%02d", h);
					String hfPath = "/hfdata/sy_zs/" + date.replaceAll("-", "");
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
						for (int i = 0; i < files.length; i++) {
							File f = files[i];
							if (f.isFile()) {
								fs.copyFromLocalFile(new Path(f.toString()),
										new Path(hPath));
								System.out.println("copy from: " + f.toString()
										+ " to " + hfPath + "/"
										+ String.format("%02d", h));

								f.delete();
							}

						}

						file.delete();
						// fs.close();

					}
				}
				if ("2".equals(isHdTask) || "3".equals(isHdTask)) {
					// String nd = date.replaceAll("-", "");
					String tabName = "hf_sy";
					Configuration conf = HBaseConfiguration.create();
					conf.set("hbase.zookeeper.quorum", "nn1,view,dn1");
					conf.set("hbase.zookeeper.property.clientPort", "2181");
					conf.set("hbase.master", "nn1:60000");
					conf.set("hbase.zookeeper.dns.nameserver", "nn1");
					conf.set("mytable", tabName);
					Job job = Job.getInstance(
							conf,
							"syImp" + date.replaceAll("-", "") + "/"
									+ String.format("%02d", h));
					job.setJarByClass(SyTask.class);
					job.setMapperClass(SyMap.class);
					job.setReducerClass(SyReduce.class);
					job.setOutputFormatClass(MultiTableOutputFormat.class);
					job.setMapOutputKeyClass(Text.class);
					job.setMapOutputValueClass(Text.class);
					createTable(tabName, conf);
					FileInputFormat.addInputPath(
							job,
							new Path("/hfdata/sy_zs/"
									+ date.replaceAll("-", "") + "/"
									+ String.format("%02d", h)));
					job.waitForCompletion(true);

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			MainJob.threadLeftOp("-");
			System.out.println("SY OVER");
		}
	}
	// }
}
