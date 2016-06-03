package com.zltel.dataexport.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataExportReduce extends Reducer<Text, Text, Text, Text> {
	private static Logger logout = LoggerFactory.getLogger(DataExportReduce.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> iterator, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text value : iterator) {
			try {
				context.write(key, value);
			} catch (Exception e) {
				logout.error(e.getMessage(), e);
			}
		}
	}

}
