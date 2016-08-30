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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.main.SuperMRJob;
import com.zltel.common.utils.date.DateUtil;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.userlife.doublesort.DoubleSortKey;
import com.zltel.location_aware.userlife.doublesort.FirstPartitioner;
import com.zltel.location_aware.userlife.doublesort.GroupingComparator;
import com.zltel.location_aware.userlife.doublesort.MyComparator;
import com.zltel.location_aware.userlife.map.UserLifeBJ_DoubleSort_Map;
import com.zltel.location_aware.userlife.reduce.UserLifeBJ_DoubleSort_Reduce;
import com.zltel.location_aware.userlife.utils.JobConfigUtil;

/**
 * 使用 双重排序 提高计算效率
 * 
 * @author Wangch
 *
 */
public class UserLifeBJ_DoubleSortMain extends SuperMRJob {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBJ_DoubleSortMain.class);

	/**
	 * 任务 分片数
	 */
	public static int splitCount = 20;
	public static int startRegion = 00;
	public static int endRegion = 99;

	private static String KEEP_POINTS = "1";
	/** 运算点最大值 **/
	private static int POINTER_CALC_COUNT_LIMIT = 5 * 10000;

	public static final String STR_STARTREGION = "startregion";
	public static final String STR_ENDREGION = "endregion";

	public static String BASE_PATH = "/user/zltel/";
	public static final String _GISPATH = "userlife/res/gis.txt";
	public static final String _IMEI_PATH = "userlife/res/imei.txt";

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
	 * 
	 *            1. 开始时间 2. 结束时间 3. 输出路径 4. cs输入路径(不设置null) 5. gn输入路径 (不设置null)
	 *            6. 分片数 7. 分片开始 8. 分片结束 9.是否保留数据点(0不保留，1保留) 10.最大点聚合值
	 * 
	 * 
	 */
	public static void main(String[] args) throws Exception {
		logout.info("---------------- UserLife Tags Mark  ----------------");
		logout.info("  version: 使用双重 排序提高计算效率");
		logout.info("     time: 2016.7.6");
		logout.info("---------------- UserLife Tags Mark  ----------------");
		if (args.length >= 6) {
			splitCount = Integer.valueOf(args[5]);
			logout.info("init Param 分片数:" + splitCount);
		}
		if (args.length >= 7) {
			startRegion = Integer.valueOf(args[6]);
			logout.info("init Param 开始分片:" + startRegion);
		}
		if (args.length >= 8) {
			endRegion = Integer.valueOf(args[7]);
			logout.info("init Param 结束分片:" + endRegion);
		}
		if (args.length >= 9) {
			KEEP_POINTS = args[8];
			logout.info("init Param 是否保留数据点:" + KEEP_POINTS);
		}
		if (args.length >= 10) {
			POINTER_CALC_COUNT_LIMIT = Integer.valueOf(args[9]);
			logout.info("init Param 聚合最大输入点限制:" + POINTER_CALC_COUNT_LIMIT);
		}
		List<String[]> split = split();
		logout.info("任务分片数:" + split.size());
		for (int idx = 0, _sz = split.size(); idx < _sz; idx++) {
			String[] ss = split.get(idx);
			logout.info("开始 第" + idx + " 个分片 :" + ss[0] + "-" + ss[1]);
			run(args, ss);
		}

	}

	/**
	 * 根据起始位置 和 分片数切分
	 * 
	 * @return
	 */
	public static final List<String[]> split() {
		List<String[]> ret = new ArrayList<String[]>();
		int step = (int) Math.floor((endRegion - startRegion) / splitCount);
		logout.info("分片单位:" + step);
		if (step == 0) {
			ret.add(new String[] { StringUtil.toFix(startRegion, 2), StringUtil.toFix(endRegion, 2) });
		} else {
			int _cr = startRegion;
			while (_cr < endRegion) {
				int _e = _cr + step;
				int _er = _e < endRegion ? _e : endRegion;
				ret.add(new String[] { StringUtil.toFix(_cr, 2), StringUtil.toFix(_er, 2) });
				_cr = _e;
			}
		}
		return ret;
	}

	public static final void run(String[] args, String[] splits) throws Exception {
		long _time = System.currentTimeMillis();
		Configuration _conf = initConf();
		_conf.set(STR_STARTREGION, splits[0]);
		_conf.set(STR_ENDREGION, splits[1]);
		_conf.set("KEEP_POINTS", KEEP_POINTS);
		_conf.set("POINTER_CALC_COUNT_LIMIT", String.valueOf(POINTER_CALC_COUNT_LIMIT));

		String outPath = args[2] + splits[0] + "-" + splits[1] + "/";
		Date startDt = sdf.parse(args[0]);
		Date endDt = sdf.parse(args[1]);

		_conf.set("result_out_path", outPath);

		JobConfigUtil.initComConf(_conf);
		Job job = Job.getInstance(_conf, "userlife tags ");
		try {
			// 时间
			if (args.length < 3) {
				throw new RuntimeException("need3 params: yyyyMMdd , stime,etime ,output");
			} else {
				logout.info("init Param: start=" + args[0] + " , end=" + args[1]);
			}

			FileSystem fs = FileSystem.get(job.getConfiguration());
			Path pout = new Path(outPath);
			if (fs.exists(pout)) {
				fs.delete(pout, true);
				logout.info(outPath + "  this path exists, deleting......");
			}
			if (fs.exists(new Path(BASE_PATH + _GISPATH)) && fs.exists(new Path(BASE_PATH + _IMEI_PATH))) {
				logout.info("userlife res:<" + BASE_PATH + _GISPATH + "> and <" + BASE_PATH + _IMEI_PATH + ">  exists");
				job.getConfiguration().set("GISPATH", BASE_PATH + _GISPATH);
				job.getConfiguration().set("IMEI_PATH", BASE_PATH + _IMEI_PATH);
			} else {
				String m = "userlife res:" + BASE_PATH + _GISPATH + " or " + BASE_PATH + _IMEI_PATH
						+ " does not exists !";
				logout.error(m);
				throw new RuntimeException(m);
			}

			List<Date> dates = DateUtil.dateSplit(startDt, endDt, DateUtil.RANGE_DAY);
			logout.info("时间 间隔：" + dates.size());
			boolean findPath = false;
			if (args.length >= 4) {
				// 有输入路径
				logout.info("有输入自定义路径");
				List<String> inputs = new ArrayList<String>();
				if (args.length >= 4) {
					inputs.add(args[3]);
					if (!findPath)
						findPath = !"null".equals(args[3]);
					logout.info("\t输入路径：" + args[3]);
				}
				if (args.length >= 5) {
					inputs.add(args[4]);
					if (!findPath)
						findPath = !"null".equals(args[4]);
					logout.info("\t输入路径：" + args[4]);
				}
				if (findPath) {
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
					logout.info(" 没有设定自定义路径");
				}
			}
			// 没找到使用默认 的
			if (!findPath) {
				logout.info("使用默认路径定义");
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

			job.setJarByClass(UserLifeBJ_DoubleSortMain.class);
			// 设置Map/Reduce 处理类
			job.setMapperClass(UserLifeBJ_DoubleSort_Map.class);
			job.setReducerClass(UserLifeBJ_DoubleSort_Reduce.class);
			// 分区函数
			job.setPartitionerClass(FirstPartitioner.class);
			// 分组函数( 根据此 方法确定 相同的 key进行 value分组)
			job.setGroupingComparatorClass(GroupingComparator.class);
			// 设定自定义排序函数
			job.setSortComparatorClass(MyComparator.class);

			// map 输出Key的类型
			job.setMapOutputKeyClass(DoubleSortKey.class);
			// map输出Value的类型
			job.setMapOutputValueClass(Text.class);
			// rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
			job.setOutputKeyClass(Text.class);
			// rduce输出Value的类型
			job.setOutputValueClass(Text.class);
			// 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
			job.setInputFormatClass(TextInputFormat.class);
			// 提供一个RecordWriter的实现，负责数据输出。
			job.setOutputFormatClass(TextOutputFormat.class);

			// 设置保存结果压缩
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

			// 设置输出结果
			FileOutputFormat.setOutputPath(job, new Path(outPath));

			// job.setNumReduceTasks(70);
			setReduceCount(job);

			job.waitForCompletion(true);
			_time = System.currentTimeMillis() - _time;
			logout.info("UserLife Complete ,cost time: " + _time / 60000 + " min");
		} finally {

		}
	}

}
