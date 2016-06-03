package com.zltel.location_aware.userlife.service;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.zltel.location_aware.userlife.bean.Pointer;

public class TestList {

	@Test
	public void testList() {
		List<String> list = new ArrayList<String>();
		System.out.println(list);
		System.out.println(list.subList(0, 0));

		Pointer pter = new Pointer();
		System.out.println(JSON.toJSONString(pter));
	}
}
