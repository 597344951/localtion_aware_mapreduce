package com.zltel.location.test.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sort {
	private static Logger logout = LoggerFactory.getLogger(Sort.class);

	private static String jobName = null;

	// 每行记录是一个整数。将Text文本转换为IntWritable类型，作为map的key
	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

		@Override
		protected void setup(Mapper<Object, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		private static IntWritable data = new IntWritable();

		// 实现map函数
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			data.set(line.length());
			context.write(data, new IntWritable(1));
		}
	}

	// reduce之前hadoop框架会进行shuffle和排序，因此直接输出key即可。
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

		// 实现reduce函数
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			for (IntWritable v : values) {
				context.write(key, new Text(""));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		System.err.println("任务开始启动！");
		Configuration conf = new Configuration();

		// 指定JobTracker地址
		conf.set("mapred.job.tracker", " 2. 8. . 9:  ");
		if (args.length != 2) {
			System.err.println("Usage: Data Sort <in> <out>");
			System.exit(2);
		}
		System.out.println(args[0]);
		System.out.println(args[1]);
		jobName = "userlife-" + System.currentTimeMillis();
		conf.set("jobName", jobName);
		Job job = Job.getInstance(conf, "Data Sort");
		job.setJarByClass(Sort.class);
		job.setJar("target/localtion_aware_mapreduce-0.0.1-SNAPSHOT.jar");

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path pout = new Path(args[1]);
		if (fs.exists(pout)) {
			fs.delete(pout, true);
			System.out.println("存在此路径, 已经删除......");
		}

		// Configuration jconf = job.getConfiguration();
		// ((JobConf) job.getConfiguration()).setJar("sort.jar");
		// 设置Map和Reduce处理类
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// 设置输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
