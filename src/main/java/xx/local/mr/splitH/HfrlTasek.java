/**
 * Project Name:BigCount
 * File Name:RlTasek.java
 * Package Name:com.zltel.data.rl
 * Date:2016年3月21日下午4:31:48
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.splitH;

import java.io.ByteArrayInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

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
public class HfrlTasek implements Runnable {
  private static Log log = LogFactory.getLog(HfrlTasek.class);
  String gnPath;
  String csPath;
  String dated;
  int h;
  String isFtpWk;

  public HfrlTasek() {
  }

  public HfrlTasek(String gnPath, String csPath, String dated, int h,
      String isFtpWk) {

    this.gnPath = gnPath;
    this.csPath = csPath;
    this.dated = dated;
    this.h = h;
    this.isFtpWk = isFtpWk;
  }

  public String getDated() {
    return dated;
  }

  public void setDated(String dated) {
    this.dated = dated;
  }

  public int getH() {
    return h;
  }

  public void setH(int h) {
    this.h = h;
  }

  public String getGnPath() {
    return gnPath;
  }

  public void setGnPath(String gnPath) {
    this.gnPath = gnPath;
  }

  public String getCsPath() {
    return csPath;
  }

  public void setCsPath(String csPath) {
    this.csPath = csPath;
  }

  public String getIsFtpWk() {
    return isFtpWk;
  }

  public void setIsFtpWk(String isFtpWk) {
    this.isFtpWk = isFtpWk;
  }

  public void run() {
    try {
      String hfrlPath = "/user/zltel/hfOutRL/" + dated.replaceAll("-", "")
          + "/" + String.format("%02d", h);
      if ("1".equals(isFtpWk) || "3".equals(isFtpWk)) {
        log.info("rl job start");
        JobConf jobConf = new JobConf(HfqsTask.class);
        jobConf.setJobName("hfRlData");
        jobConf.set("mapred.max.map.failures.percent", "100");
        jobConf.set("mapred.max.reduce.failures.percent", "100");
        jobConf.setMapperClass(HfrlMap.class);
        jobConf.setReducerClass(HfrlReduce.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);
        // jobConf.setInputFormat(SequenceFileInputFormat.class);
        // jobConf.setNumReduceTasks(6);
        FileOutputFormat.setCompressOutput(jobConf, true);
        FileOutputFormat.setOutputCompressorClass(jobConf, GzipCodec.class);
        if (gnPath != null && gnPath.length() > 0) {
          log.info("inputPath:" + gnPath);
          FileInputFormat.addInputPath(jobConf, new Path(gnPath));
        }
        if (csPath != null && csPath.length() > 0) {
          log.info("inputPath:" + csPath);
          FileInputFormat.addInputPath(jobConf, new Path(csPath));
        }
        if (gnPath == null && csPath == null) {
          return;
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(hfrlPath))) {
          if (fs.delete(new Path(hfrlPath), true)) {
            log.info("delete  old path  successful  ：" + hfrlPath);
          } else {
            log.info("delete  old path  err ：" + hfrlPath);
          }
        }
        FileOutputFormat.setOutputPath(jobConf, new Path(hfrlPath));
        JobClient.runJob(jobConf);
        log.info("rl job end");
      }
      if ("2".equals(isFtpWk) || "3".equals(isFtpWk)) {
        log.info("rl ftp job start");
        Path path = new Path(hfrlPath);
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
              String rePath = "data/rl/" + dated.replaceAll("-", "");
              String rp = rePath + "/" + String.format("%02d", h);
              FSDataInputStream fin = fs.open(new Path(localPath));
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
        String msg = dated.replaceAll("-", "") + "_" + h + "_" + "rl.txt";
        ftp.uploadFile(msg, new ByteArrayInputStream("1".getBytes()), null,
            "msg/");
        log.info("sed msg：" + msg);
        fs.close();
      }
    } catch (Exception e) {
      log.error(e.getMessage());
      e.printStackTrace();
    }
  }

  // public static void main(String[] args) throws Exception {
  // String startDate = null;
  // String endDate = null;
  // if (args.length > 1) {
  // startDate = args[0];
  // endDate = args[1];
  // } else {
  // return;
  // }
  // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
  // SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
  // long nlt = sdfh.parse(sdfh.format(new Date())).getTime();
  // long olt = sdfh.parse(startDate).getTime();
  // long elt = sdfh.parse(endDate).getTime();
  // while (nlt > olt && olt <= elt) {
  // String dated = sdf.format(new Date(olt));
  // Calendar c = Calendar.getInstance();
  // c.setTime(new Date(olt));
  // int h = c.get(Calendar.HOUR_OF_DAY);
  // olt += 60 * 60 * 1000;
  //
  // job.setNumReduceTasks(3);
  // createTable("hf_sy", conf);
  // String hfPath = "/hfdata/gn_zs/" + dated.replaceAll("-", "") + "/"
  // + String.format("%02d", h) + "/";
  // System.out.println("hfPath--" + hfPath);
  // FileInputFormat.addInputPath(job, new Path(hfPath));
  // job.waitForCompletion(true);
  // }
  //
  // }

	// public static void createTable(String tableName, Configuration conf)
	// throws MasterNotRunningException, ZooKeeperConnectionException,
	// IOException {
	//
	// TableName userTable = TableName.valueOf(tableName);
	// HBaseAdmin admin = new HBaseAdmin(conf);
	// HTableDescriptor tableDescr = new HTableDescriptor(userTable);
	// tableDescr.addFamily(new HColumnDescriptor("info".getBytes()));
	// if (admin.tableExists(userTable)) {
	// admin.close();
	// } else {
	// admin.createTable(tableDescr);
	// admin.close();
	// }
	//
	// }
  /*
   * public static void main(String[] args) throws IOException {
   * System.setProperty("hadoop.home.dir", "D:\\Workspaces\\hadoop"); JobConf
   * jobConf = new JobConf(PnumTask.class); jobConf.setJobName("Pnum");
   * jobConf.set("mapred.max.map.failures.percent", "100");
   * jobConf.set("mapred.max.reduce.failures.percent", "100");
   * 
   * Path path = new Path("E:/2015-120-25/sq/01");
   * jobConf.setMapperClass(PnumMap.class);
   * jobConf.setReducerClass(PnumReduce.class);
   * jobConf.setOutputKeyClass(Text.class);
   * jobConf.setOutputValueClass(Text.class);
   * jobConf.setInputFormat(SequenceFileInputFormat.class);
   * FileInputFormat.addInputPath(jobConf, path);
   * FileOutputFormat.setOutputPath(jobConf, new Path("E:/2015-120-25/sq/out"));
   * 
   * try{ JobClient.runJob(jobConf); }catch(Exception e){ e.printStackTrace(); }
   * }
   */

}
