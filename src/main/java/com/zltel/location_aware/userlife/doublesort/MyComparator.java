package com.zltel.location_aware.userlife.doublesort;

import org.apache.hadoop.io.WritableComparator;

public class MyComparator extends WritableComparator {

	public MyComparator() {
		super(DoubleSortKey.class, true);
	}
}
