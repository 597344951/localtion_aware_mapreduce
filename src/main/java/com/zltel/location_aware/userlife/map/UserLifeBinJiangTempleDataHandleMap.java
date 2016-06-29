package com.zltel.location_aware.userlife.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserLifeBinJiangTempleDataHandleMap extends Mapper<LongWritable, Text, Text, Text> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] strs = value.toString().trim().split("\t");
		if (strs.length < 2)
			return;
		String imsi = strs[0];
		String text = strs[1];
		context.write(new Text(imsi), new Text(text));
	}

}
