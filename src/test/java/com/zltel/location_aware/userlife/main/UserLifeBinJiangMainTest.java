package com.zltel.location_aware.userlife.main;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UserLifeBinJiangMainTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testSplit() {
		UserLifeBinJiangMain.splitCount = 50;
		UserLifeBinJiangMain.startRegion = 0;
		UserLifeBinJiangMain.endRegion = 99;

		List<String[]> list = UserLifeBinJiangMain.split();
		for (String[] ss : list) {
			System.out.println(ss[0] + " " + ss[1]);
		}
	}

}
