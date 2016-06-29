package com.zltel.location_aware.userlife.main;

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

import com.zltel.location_aware.userlife.map.UserLifeBinJiangTempleDataHandleMap;
import com.zltel.location_aware.userlife.reduce.UserLifeBinJiangReduce;
import com.zltel.location_aware.userlife.utils.JobConfigUtil;

/**
 * 滨江用户数据 临时标签数据处理
 * 
 * @author Wangch
 *
 */
public class UserLifeBinJiangTempleDataHandleMain {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangTempleDataHandleMain.class);

	public static Configuration initConf() {
		Configuration conf = new Configuration();
		return conf;
	}

	/**
	 * 
	 * @param args
	 *            1. 输入的临时数据路径 2. 输出结果路径
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		logout.info("---------------- UserLife Tags Temp Data Handle  ----------------");
		logout.info("  version: BingJiang Hadoop ver");
		logout.info("     time: 2016.5.19");
		logout.info("---------------- UserLife Tags Temp Data Handle  ----------------");

		Configuration _conf = initConf();
		_conf.set(UserLifeBinJiangMain.STR_STARTREGION, "00");
		_conf.set(UserLifeBinJiangMain.STR_ENDREGION, "99");
		_conf.set("KEEP_POINTS", "1");
		_conf.set("POINTER_CALC_COUNT_LIMIT", String.valueOf(50000));

		JobConfigUtil.initComConf(_conf);
		Job job = Job.getInstance(_conf, "userlife tags ");
		try {
			String inputpath = null;
			String outputpath = null;
			// 时间
			if (args.length < 2) {
				throw new RuntimeException("需要一个临时数据输入路径和输出路径!");
			} else {
				inputpath = args[0];
				outputpath = args[1];
				logout.info("init Param: inputpath=" + args[0] + ",outputpath=" + outputpath);
			}

			FileSystem fs = FileSystem.get(job.getConfiguration());
			Path pout = new Path(outputpath);
			if (fs.exists(pout)) {
				fs.delete(pout, true);
				logout.info(outputpath + "  this path exists, deleting......");
			}

			FileInputFormat.addInputPath(job, new Path(inputpath));

			job.setJarByClass(UserLifeBinJiangTempleMain.class);
			// 设置Map/Reduce 处理类
			job.setMapperClass(UserLifeBinJiangTempleDataHandleMap.class);
			// 设置Map/Reduce 中间结果指
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setReducerClass(UserLifeBinJiangReduce.class);

			// 设置保存结果压缩
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

			// 设置输出结果
			FileOutputFormat.setOutputPath(job, new Path(outputpath));

			job.setNumReduceTasks(70);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			logout.info("UserLife Complete ");
		} finally {

		}
	}
}
