package com.zltel.location_aware.userlife.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UserLifeBinJiangImportMapTest {

	String context = "214033882160334_2\t[{\"lat\":30.239004,\"lng\":120.43136,\"pcount\":33,\"pointers\":[{\"cell\":\"机场T3航站楼3楼B区SFDM\",\"ci\":\"101210626\",\"etime\":\"20160529102205\",\"imsi\":\"310410873689735\",\"lac\":\"22451\",\"lat\":30.239004,\"lng\":120.431366,\"nettype\":\"4\",\"score\":3472,\"source\":\"gn\",\"stime\":\"20160529100952\",\"tag\":\"MW\",\"tagDescript\":\"上午工作时段\",\"time\":0,\"weekday\":false,\"weight\":0.8}],\"score\":3472},{\"lat\":30.281433,\"lng\":120.10966,\"pcount\":26,\"pointers\":[{\"cell\":\"米兰国际酒店SM_2\",\"ci\":\"101059074\",\"etime\":\"20160528100652\",\"imsi\":\"310410873689735\",\"lac\":\"22709\",\"lat\":30.281431,\"lng\":120.10966,\"nettype\":\"4\",\"score\":1190,\"source\":\"gn\",\"stime\":\"20160528100631\",\"tag\":\"MW\",\"tagDescript\":\"上午工作时段\",\"time\":0,\"weekday\":false,\"weight\":0.8},{\"cell\":\"米兰国际CA辅小区D_1\",\"ci\":\"101554692\",\"etime\":\"20160528093431\",\"imsi\":\"310410873689735\",\"lac\":\"22709\",\"lat\":30.281431,\"lng\":120.10966,\"nettype\":\"4\",\"score\":947,\"source\":\"gn\",\"stime\":\"20160528093419\",\"tag\":\"MW\",\"tagDescript\":\"上午工作时段\",\"time\":0,\"weekday\":false,\"weight\":0.8},{\"cell\":\"米兰国际D_1\",\"ci\":\"101554689\",\"etime\":\"20160528093916\",\"imsi\":\"310410873689735\",\"lac\":\"22709\",\"lat\":30.281431,\"lng\":120.10966,\"nettype\":\"4\",\"score\":384,\"source\":\"gn\",\"stime\":\"20160528093430\",\"tag\":\"MW\",\"tagDescript\":\"上午工作时段\",\"time\":0,\"weekday\":false,\"weight\":0.8},{\"cell\":\"米兰国际酒店SM_1\",\"ci\":\"101059073\",\"etime\":\"20160528095051\",\"imsi\":\"310410873689735\",\"lac\":\"22709\",\"lat\":30.281431,\"lng\":120.10966,\"nettype\":\"4\",\"score\":154,\"source\":\"gn\",\"stime\":\"20160528094751\",\"tag\":\"MW\",\"tagDescript\":\"上午工作时段\",\"time\":0,\"weekday\":false,\"weight\":0.8},{\"cell\":\"米兰国际CA辅小区D_2\",\"ci\":\"101554693\",\"etime\":\"20160528103344\",\"imsi\":\"310410873689735\",\"lac\":\"22709\",\"lat\":30.281431,\"lng\":120.10966,\"nettype\":\"4\",\"score\":26,\"source\":\"gn\",\"stime\":\"20160528103311\",\"tag\":\"MW\",\"tagDescript\":\"上午工作时段\",\"time\":0,\"weekday\":false,\"weight\":0.8}],\"score\":2701},{\"lat\":30.267435,\"lng\":120.16388,\"pcount\":26,\"pointers\":[{\"cell\":\"同方财富大厦CA辅小区D_2\",\"ci\":\"185044741\",\"etime\":\"20160528133038\",\"imsi\":\"310410873689735\",\"lac\":\"22303\",\"lat\":30.26724,\"lng\":120.16382,\"nettype\":\"4\",\"score\":875,\"source\":\"gn\",\"stime\":\"20160528133018\",\"tag\":\"AW\",\"tagDescript\":\"下午工作时段\",\"time\":0,\"weekday\":false,\"weight\":1},{\"cell\":\"土畜产外挂SM_2\",\"ci\":\"193589250\",\"etime\":\"20160528152239\",\"imsi\":\"310410873689735\",\"lac\":\"22303\",\"lat\":30.268091,\"lng\":120.16411,\"nettype\":\"4\",\"score\":400,\"source\":\"gn\",\"stime\":\"20160528151917\",\"tag\":\"AW\",\"tagDescript\":\"下午工作时段\",\"time\":0,\"weekday\":false,\"weight\":1},{\"cell\":\"同方财富大厦D_2\",\"ci\":\"185044738\",\"etime\":\"20160528140634\",\"imsi\":\"310410873689735\",\"lac\":\"22303\",\"lat\":30.26724,\"lng\":120.16382,\"nettype\":\"4\",\"score\":343,\"source\":\"gn\",\"stime\":\"20160528140623\",\"tag\":\"AW\",\"tagDescript\":\"下午工作时段\",\"time\":0,\"weekday\":false,\"weight\":1},{\"cell\":\"同方财富大厦SM_2\",\"ci\":\"101252354\",\"etime\":\"20160528134917\",\"imsi\":\"310410873689735\",\"lac\":\"22303\",\"lat\":30.26724,\"lng\":120.16382,\"nettype\":\"4\",\"score\":121,\"source\":\"gn\",\"stime\":\"20160528134838\",\"tag\":\"AW\",\"tagDescript\":\"下午工作时段\",\"time\":0,\"weekday\":false,\"weight\":1},{\"cell\":\"同方财富大厦D_1\",\"ci\":\"185044737\",\"etime\":\"20160528151926\",\"imsi\":\"310410873689735\",\"lac\":\"22303\",\"lat\":30.26724,\"lng\":120.16382,\"nettype\":\"4\",\"score\":14,\"source\":\"gn\",\"stime\":\"20160528151912\",\"tag\":\"AW\",\"tagDescript\":\"下午工作时段\",\"time\":0,\"weekday\":false,\"weight\":1}],\"score\":1753},{\"lat\":30.241037,\"lng\":120.43118,\"pcount\":8,\"pointers\":[{\"cell\":\"机场T3航站楼3楼C区D区SFDM\",\"ci\":\"101210627\",\"etime\":\"20160529101011\",\"imsi\":\"310410873689735\",\"lac\":\"22451\",\"lat\":30.241037,\"lng\":120.43118,\"nettype\":\"4\",\"score\":586,\"source\":\"gn\",\"stime\":\"20160529100205\",\"tag\":\"MW\",\"tagDescript\":\"上午工作时段\",\"time\":0,\"weekday\":false,\"weight\":0.8}],\"score\":586},{\"lat\":30.276041,\"lng\":120.15451,\"pcount\":8,\"pointers\":[{\"cell\":\"客运码头CA辅小区D_2\",\"ci\":\"101625605\",\"etime\":\"20160529091423\",\"imsi\":\"310410873689735\",\"lac\":\"22303\",\"lat\":30.27604,\"lng\":120.15451,\"nettype\":\"4\",\"score\":212,\"source\":\"gn\",\"stime\":\"20160529091353\",\"tag\":\"MW\",\"tagDescript\":\"上午工作时段\",\"time\":0,\"weekday\":false,\"weight\":0.8}],\"score\":212}]";

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testMapLongWritableTextContext() throws IOException, InterruptedException {
		UserLifeBinJiangImportMap im = new UserLifeBinJiangImportMap();
		Text value = new Text(context);
		LongWritable key = new LongWritable();
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context con = null;
		// im.map(key, value, con);
	}

}
