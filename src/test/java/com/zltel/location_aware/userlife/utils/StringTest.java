package com.zltel.location_aware.userlife.utils;

import org.junit.Test;

import com.zltel.common.utils.string.StringUtil;

public class StringTest {

	@Test
	public void testReventStr() {
		String str = "460007163622789";
		System.out.println(StringUtil.reverse(str));
	}
}
