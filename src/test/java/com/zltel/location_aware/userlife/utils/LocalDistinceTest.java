package com.zltel.location_aware.userlife.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zltel.location_aware.userlife.bean.Pointer;

public class LocalDistinceTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testDistince() {
		Pointer p1 = new Pointer();
		p1.setLat("30.21861");
		p1.setLng("120.17028");
		Pointer p2 = new Pointer();
		p2.setLat("30.26339");
		p2.setLng("120.15375");

		double r = LocalDistince.Distince(p1, p2);
		System.out.println(" 距离： " + r);

	}

}
