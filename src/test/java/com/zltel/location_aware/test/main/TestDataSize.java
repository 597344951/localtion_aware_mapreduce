package com.zltel.location_aware.test.main;

import java.util.HashMap;
import java.util.Map;

import com.zltel.common.utils.string.StringUtil;

public class TestDataSize {

	public static void main(String[] args) throws InterruptedException {
		Map<String, String> map = new HashMap<String, String>();

		long max = 10000000l;
		long time = System.currentTimeMillis();
		for (long i = 0; i < max; i++) {
			map.put(StringUtil.toFix(i, 15), "123456789012345");
			if (System.currentTimeMillis() - time > 10000) {
				int p = (int) (i * 100 / max);
				System.out.println("已完成: " + p + "%");
				time = System.currentTimeMillis();
			}
		}
		System.out.println("加载完成");

		Thread.sleep(10000);
	}

}
