/**
 * Project Name:BigCount
 * File Name:GnTask.java
 * Package Name:xx.local.mr.splitH
 * Date:2016年4月6日下午2:25:34
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.splitH;

import java.io.ByteArrayInputStream;
import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ClassName:GnTask <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年4月6日 下午2:25:34 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class GnTask implements Runnable {
	private static Log log = LogFactory.getLog(GnTask.class);
	String dated;
	int h;
	String isFtpWk;
	String msgPath;

	public String getMsgPath() {
		return msgPath;
	}

	public void setMsgPath(String msgPath) {
		this.msgPath = msgPath;
	}

	public GnTask() {
	}

	public String getDated() {
		return dated;
	}

	public void setDated(String dated) {
		this.dated = dated;
	}

	public GnTask(String dated, int h, String isFtpWk, String msgPath) {

		this.dated = dated;
		this.h = h;
		this.isFtpWk = isFtpWk;
		this.msgPath = msgPath;
	}

	public int getH() {
		return h;
	}

	public void setH(int h) {
		this.h = h;
	}

	public String getIsFtpWk() {
		return isFtpWk;
	}

	public void setIsFtpWk(String isFtpWk) {
		this.isFtpWk = isFtpWk;
	}

	public void run() {
		try {
			String gnPath = "/data/gn-roll/*/" + dated + "/"
					+ String.format("%02d", h) + "/*.gz";
			String hfOutGNPath = "/user/zltel/hfOutGN/"
					+ dated.replaceAll("-", "") + "/"
					+ String.format("%02d", h);
			if ("1".equals(isFtpWk) || "3".equals(isFtpWk)) {
				log.info("gn job start");

				Configuration conf = new Configuration();
				// conf.setJobName("hfSyData");
				conf.set("mapred.max.map.failures.percent", "100");
				conf.set("mapred.max.reduce.failures.percent", "100");
				Job job = Job.getInstance(
						conf,
						"hfGnData" + dated.replaceAll("-", "") + "/"
								+ String.format("%02d", h));

				job.setMapperClass(HfqsMap.class);
				job.setReducerClass(HfqsReduce.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setJarByClass(GnTask.class);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setNumReduceTasks(12);
				FileOutputFormat.setCompressOutput(job, true);
				FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
				FileInputFormat.addInputPath(job, new Path(gnPath));
				log.info("inputPath:" + gnPath);
				// Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				if (fs.exists(new Path(hfOutGNPath))) {
					if (fs.delete(new Path(hfOutGNPath), true)) {
						log.info("delete  old path  successful  ："
								+ hfOutGNPath);
					} else {
						log.info("delete  old path  err ：" + hfOutGNPath);
					}
				}
				FileOutputFormat.setOutputPath(job, new Path(hfOutGNPath));
				log.info("OutputPath:" + hfOutGNPath);
				job.waitForCompletion(true);
			}
			if ("2".equals(isFtpWk) || "3".equals(isFtpWk)) {
				log.info("gn ftp job start");
				Path path = new Path(hfOutGNPath);
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				if (fs.exists(path)) {
					FileStatus[] stats = fs.listStatus(path);
					for (int i = 0; i < stats.length; ++i) {
						if (stats[i].isFile()) {
							String localPath = stats[i].getPath().toString();
							String[] localNames = localPath.split("/");
							String rem = localNames[localNames.length - 1];
							String remt = rem + ".tmp";
							String rePath = "data/gn/"
									+ dated.replaceAll("-", "");
							String rp = rePath + "/" + String.format("%02d", h);
							FSDataInputStream fin = fs
									.open(new Path(localPath));
							FtpUtil ftp = new FtpUtil();
							log.info("local path:" + remt);
							log.info("remot Path" + rp);
							ftp.uploadFile(remt, fin, rePath, rp);
							ftp.rename(rem, remt, rp);
							fin.close();
						}
					}
				} else {
					throw new Exception("the file is not found .");
				}
				FtpUtil ftp = new FtpUtil();
				String msg = dated.replaceAll("-", "") + "_" + h + "_"
						+ "gn.txt";
				ftp.uploadFile(msg, new ByteArrayInputStream("1".getBytes()),
						null, msgPath + "/");
				log.info("sed msg：" + msg);
//				fs.close();
			}
			if ("4".equals(isFtpWk)) {
				FtpUtil ftp = new FtpUtil();
				String msg = dated.replaceAll("-", "") + "_" + h + "_"
						+ "gn.txt";
				ftp.uploadFile(msg, new ByteArrayInputStream("1".getBytes()),
						null, msgPath + "/");
				log.info("sed msg：" + msg);
			}

		} catch (Exception e) {
			log.error("err: GN qs ");
			String errmsg = "/home/zltel/hfqs/err/" + dated.replaceAll("-", "")
					+ "_" + h + "_" + "gn.txt";
			File errfile = new File(errmsg);
			errfile.mkdir();
			log.error(e.getMessage());
			e.printStackTrace();
		} finally {
			MainJob.threadLeftOp("-");
			System.out.println("gn over");
		}
	}
}
