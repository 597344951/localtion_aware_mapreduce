/**
 * Project Name:BigCount
 * File Name:RlTasek.java
 * Package Name:com.zltel.data.rl
 * Date:2016年3月21日下午4:31:48
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
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * ClassName:RlTasek <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年3月21日 下午4:31:48 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class RlTasek implements Runnable {

	String date = "";
	int h = 0;
	String isHdTask = "";

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public RlTasek(String date, int h, String isHdTask) {
		this.date = date;
		this.h = h;
		this.isHdTask = isHdTask;
	}

	public RlTasek() {
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
		if (admin.tableExists(userTable)) {

			admin.close();
		} else {
			admin.createTable(tableDescr);
			admin.close();
		}

	}

	public void run() {
		try {
			String rlmsg = "/home/zltel/msg/" + date.replaceAll("-", "") + "_"
					+ h + "_rl.txt";
			File filemsg = new File(rlmsg);
			boolean isCon = false;
			for (int i = 0; i < 30; i++) {
				if (filemsg.exists()) {
					isCon = true;
					break;
				} else {
					Thread.sleep(60 * 1000);
				}
			}
			if (isCon) {
				if ("1".equals(isHdTask) || "3".equals(isHdTask)) {
					Configuration conf = new Configuration();
					String locatpath = "/home/zltel/data/rl/"
							+ date.replaceAll("-", "") + "/"
							+ String.format("%02d", h);
					String hfPath = "/hfdata/rl_zs/" + date.replaceAll("-", "");
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
							}
							f.delete();
						}

//						fs.close();
						file.delete();
					}
				}
				if ("2".equals(isHdTask) || "3".equals(isHdTask)) {
					String nd = date.replaceAll("-", "");
					String tabName = "hf_hot5" + nd;
					Configuration conf = HBaseConfiguration.create();
					conf.set("hbase.zookeeper.quorum", "dn4,dn5,dn6");
					conf.set("hbase.zookeeper.property.clientPort", "2181");
					conf.set("hbase.master", "nn1:60000");
					conf.set("hbase.zookeeper.dns.nameserver", "nn1");
					conf.set("mytable", tabName);
					Job job = Job.getInstance(conf, "rlImp");
					job.setJarByClass(RlTasek.class);
					job.setMapperClass(RlMap.class);
					job.setReducerClass(RlReduce.class);
					job.setOutputFormatClass(MultiTableOutputFormat.class);
					job.setMapOutputKeyClass(Text.class);
					job.setMapOutputValueClass(Text.class);
					createTable(tabName, conf);
					FileInputFormat.addInputPath(job, new Path("/hfdata/rl_zs/"
							+ date.replaceAll("-", "") + "/" + h));
					job.waitForCompletion(true);

				}
			}

		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	// }
}
