package com.zltel.location_aware.test.main;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.zltel.location_aware.userlife.bean.TopPointer;

public class TestFastJson {
	public static class TestBean {
		@JSONField(name = "_id")
		private String _id;
		@JSONField(name = "_name")
		private String _name;

		public TestBean(String _id, String _name) {
			super();
			this._id = _id;
			this._name = _name;
		}

		/**
		 * @return the _id
		 */
		public final String get_id() {
			return _id;
		}

		/**
		 * @return the _name
		 */
		public final String get_name() {
			return _name;
		}

		/**
		 * @param _id
		 *            the _id to set
		 */
		public final void set_id(String _id) {
			this._id = _id;
		}

		/**
		 * @param _name
		 *            the _name to set
		 */
		public final void set_name(String _name) {
			this._name = _name;
		}

	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void test() {
		TestBean tb = new TestBean("myid", "myname");
		String json = JSON.toJSONString(tb);
		System.out.println(json);

		TopPointer tp = new TopPointer();
		tp.set_lat(1f);
		tp.set_lng(2f);
		json = JSON.toJSONString(tp);
		System.out.println(json);
	}

}
