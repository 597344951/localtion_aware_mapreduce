package com.zltel.testwork.cachefile.main;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.testwork.cachefile.map.DistributedCacheMap;
import com.zltel.testwork.cachefile.reduce.DistributedCacheReduce;

/**
 * 测试 cache file 特性
 * 
 * @author Wangch
 *
 */
public class DistributedCacheMain {
	private static Logger logout = LoggerFactory.getLogger(DistributedCacheMain.class);

	public static final String BASE_PATH = "/user/zltel/";
	public static final String GISPATH = BASE_PATH + "userlife/res/gis.txt";

	public static Configuration initConf() {
		Configuration conf = new Configuration();
		return conf;
	}

	public static void main(String[] args) throws Exception {
		String path = GISPATH;
		String outpath = BASE_PATH + "testcache/";

		Configuration _conf = initConf();

		Job job = Job.getInstance(_conf, "test Cache");
		FileInputFormat.addInputPath(job, new Path(path));
		job.setJarByClass(DistributedCacheMain.class);
		// 设置Map/Reduce 处理类
		job.setMapperClass(DistributedCacheMap.class);
		job.setReducerClass(DistributedCacheReduce.class);
		// 设置Map/Reduce 中间结果指
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 设置保存结果压缩
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path pout = new Path(outpath);
		if (fs.exists(pout)) {
			fs.delete(pout, true);
			logout.info(outpath + "  this path exists, deleting......");
		}

		// 设置输出结果
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		job.setNumReduceTasks(1);

		// 失败
		// logout.info("增加 本地缓存文件");
		// job.addCacheFile(new URI("/home/zltel/userlife/gis.json"));// 缓存本地文件

		// logout.info("增加 本地缓存文件2");
		// job.addCacheFile(new URI("file:///home/zltel/userlife/gis.json"));//
		// 缓存本地文件

		logout.info("增加标准hdfs缓存文件");
		job.addCacheFile(new URI("hdfs://" + GISPATH));
		logout.info("增加标准hdfs缓存文件");
		job.addCacheFile(new URI("hdfs://nn1:9000" + GISPATH));

		job.waitForCompletion(true);
	}
}
