package com.zltel.location_aware.userlife.main;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UserLifeBJ_DoubleSortMainTest {
	List<String> args = new ArrayList<String>();

	@Before
	public void setUp() throws Exception {
		UserLifeBJ_DoubleSortMain.BASE_PATH = "D:\\HDFS_HOME\\user\\zltel\\";
		//
		args.add("20160501");
		args.add("20160531");
		args.add(UserLifeBJ_DoubleSortMain.BASE_PATH + "\\userlife\\out\\");
		args.add(UserLifeBJ_DoubleSortMain.BASE_PATH + "userlife\\testData\\cs\\");
		args.add(UserLifeBJ_DoubleSortMain.BASE_PATH + "userlife\\testData\\gn\\");
		args.add("1");
		args.add("00");
		args.add("99");
		args.add("1");
		args.add("300000");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testMain() {
		String[] _args = new String[args.size()];
		args.toArray(_args);
		try {
			UserLifeBJ_DoubleSortMain.main(_args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
