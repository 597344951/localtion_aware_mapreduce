package com.zltel.location_aware.userlife.doublesort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区函数类。根据first确定Partition。
 */
public class FirstPartitioner extends Partitioner<DoubleSortKey, Text> {

	@Override
	public int getPartition(DoubleSortKey key, Text value, int numPartitions) {
		return Math.abs(key.getFirstKey().hashCode() * 257) % numPartitions;
	}
}
