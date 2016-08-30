package com.zltel.location_aware.ci_threshold.main;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;

public class CiThresholdMainTest {
	List<String> args = new ArrayList<String>();
	String BASE_PATH = "D:\\HDFS_HOME\\user\\zltel\\threshold\\";

	@Before
	public void setUp() throws Exception {
		args.add("database");// database|file
		args.add(BASE_PATH + "out\\");
	}

	@After
	public void tearDown() throws Exception {

	}

	public final void testMain() {
		String[] _args = new String[args.size()];
		args.toArray(_args);
		try {
			CiThresholdMain.main(_args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
