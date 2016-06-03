package com.zltel.location_aware.userlife.main;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

import com.zltel.common.utils.date.DateUtil;
import com.zltel.location_aware.userlife.map.UserLifeBinJiangMap;
import com.zltel.location_aware.userlife.reduce.UserLifeBinJiangTempReduce;
import com.zltel.location_aware.userlife.utils.JobConfigUtil;

/**
 * 用户 标签 实现，滨江移动杭州分公司
 * 
 * @author Wangch
 *
 */
public class UserLifeBinJiangTempleMain {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangTempleMain.class);

	public static final String ULWRITETABLE = "userlife_temp";

	public static final String BASE_PATH = "/user/zltel/";
	public static final String GISPATH = BASE_PATH + "userlife/res/gis.txt";

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	private static final String hdfs = "hdfs://nn1:9000";
	private static final String hbase = "nn1:60000";
	private static final String zookeeper = "view,nn1,dn1";

	public static Configuration initConf() {
		Configuration conf = new Configuration();
		return conf;
	}

	/**
	 * 
	 * @param args
	 *            1. starttime 2. endtime 3. 输出路径 ,4. cs输入路径,5.gn输入路径
	 */
	public static void main(String[] args) throws Exception {
		logout.info("---------------- UserLife Tags Mark  ----------------");
		logout.info("  version: BingJiang Hadoop ver");
		logout.info("     time: 2016.5.19");
		logout.info("---------------- UserLife Tags Mark  ----------------");

		Configuration _conf = initConf();
		JobConfigUtil.initComConf(_conf);
		Job job = Job.getInstance(_conf, "userlife tags ");
		try {
			// 时间
			if (args.length < 3) {
				throw new RuntimeException("need3 params: yyyyMMdd , stime,etime ,output");
			} else {
				logout.info("init Param: start=" + args[0] + " , end=" + args[1]);
			}

			String outPath = args[2];
			Date startDt = sdf.parse(args[0]);
			Date endDt = sdf.parse(args[1]);

			FileSystem fs = FileSystem.get(job.getConfiguration());
			Path pout = new Path(outPath);
			if (fs.exists(pout)) {
				fs.delete(pout, true);
				logout.info(outPath + "  this path exists, deleting......");
			}
			if (fs.exists(new Path(GISPATH))) {
				logout.info("userlife res:" + GISPATH + "  exists");
			} else {
				String m = "userlife res:" + GISPATH + "  does not exists !";
				logout.error(m);
				throw new RuntimeException(m);
			}

			List<Date> dates = DateUtil.dateSplit(startDt, endDt, DateUtil.RANGE_DAY);
			logout.info("时间 间隔：" + dates.size());
			if (args.length >= 4) {
				List<String> inputs = new ArrayList<String>();
				if (args.length >= 4) {
					inputs.add(args[3]);
				}
				if (args.length >= 5) {
					inputs.add(args[4]);
				}
				for (Date _dt : dates) {
					String str = sdf.format(_dt);
					for (String _t : inputs) {
						String _path = _t.concat(str).concat(File.separator);
						if (fs.exists(new Path(_path))) {
							String path = _path.concat("*").concat(File.separator);
							FileInputFormat.addInputPath(job, new Path(path));
							logout.info("addPath: " + path);
						}
					}
				}
			} else {
				String[] types = new String[] { "hfOutGN", "hfdataOutCS" };
				for (Date _dt : dates) {
					String str = sdf.format(_dt);
					for (String _t : types) {
						String _path = BASE_PATH.concat(_t).concat(File.separator).concat(str).concat(File.separator);
						if (fs.exists(new Path(_path))) {
							String path = _path.concat("*").concat(File.separator);
							FileInputFormat.addInputPath(job, new Path(path));
							logout.info("addPath: " + path);
						}
					}
				}
			}

			job.setJarByClass(UserLifeBinJiangTempleMain.class);
			// 设置Map/Reduce 处理类
			job.setMapperClass(UserLifeBinJiangMap.class);
			// 设置Map/Reduce 中间结果指
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setReducerClass(UserLifeBinJiangTempReduce.class);

			// 设置保存结果压缩
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

			// 设置输出结果
			FileOutputFormat.setOutputPath(job, new Path(outPath));

			job.setNumReduceTasks(30);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			logout.info("UserLife Complete ");
		} finally {

		}
	}

}
