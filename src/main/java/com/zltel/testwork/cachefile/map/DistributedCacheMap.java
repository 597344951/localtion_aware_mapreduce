package com.zltel.testwork.cachefile.map;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedCacheMap extends Mapper<LongWritable, Text, Text, Text> {
	private static Logger logout = LoggerFactory.getLogger(DistributedCacheMap.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		URI[] uris = context.getCacheFiles();
		logout.info("缓存的 缓存文件个数: " + uris.length);
		for (URI uri : uris) {
			logout.info("read: " + uri.getPath());
			// FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统
			FileSystem fs = FileSystem.get(uri, context.getConfiguration());
			FSDataInputStream in = null;
			in = fs.open(new Path(uri));
			// 用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			IOUtils.copyBytes(in, bo, 50, true);
			logout.info("读取 长度:" + bo.size());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
	}

}
