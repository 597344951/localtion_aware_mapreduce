package com.zltel.gridcluster.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zltel.gridcluster.bean.Group;

public class GridClusterServiceTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testCheckNeighbor() {
		int x = 1;
		int y = 1;
		Group _ng = new Group();
		_ng.setX(3);
		_ng.setY(3);
		long neighbor_distince = 3;
		boolean r = GridClusterService.checkNeighbor(x, y, _ng, neighbor_distince);
		System.out.println(r);
	}

}
