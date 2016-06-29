package com.zltel.location_aware.userlife.bean;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zltel.common.utils.reflect.ReflectUtil;

/**
 * 所属小区统计信息
 * 
 * @author Wangch
 *
 */
public class CiCountInfo {
	public static transient Map<String, String> numMap = new HashMap<String, String>();
	static {
		numMap.put("2", "two");
		numMap.put("3", "three");
		numMap.put("4", "four");
	}

	/** 小区信息 **/
	private String ci;
	private Map<String, Object> ciinfo;
	/** 出现次数 **/
	private long count;
	/** 分数 **/
	private long score;

	/** 流量 使用情况 **/
	private long twoG_up_bytes;
	private long twoG_down_bytes;
	private long threeG_up_bytes;
	private long threeG_down_bytes;
	private long fourG_up_bytes;
	private long fourG_down_bytes;

	/** 业务使用情况 **/
	private long sms_count;
	private long voice_count;
	private long location_count;

	/** 使用率 top **/
	private List<Map<String, Object>> appTop;
	private List<Map<String, Object>> urlTop;
	private List<Map<String, Object>> hostCount;

	/**
	 * @return the ci
	 */
	public final String getCi() {
		return ci;
	}

	/**
	 * @return the ciinfo
	 */
	public final Map<String, Object> getCiinfo() {
		return ciinfo;
	}

	/**
	 * @return the twoG_up_bytes
	 */
	public final long getTwoG_up_bytes() {
		return twoG_up_bytes;
	}

	/**
	 * @return the twoG_down_bytes
	 */
	public final long getTwoG_down_bytes() {
		return twoG_down_bytes;
	}

	/**
	 * @return the threeG_up_bytes
	 */
	public final long getThreeG_up_bytes() {
		return threeG_up_bytes;
	}

	/**
	 * @return the threeG_down_bytes
	 */
	public final long getThreeG_down_bytes() {
		return threeG_down_bytes;
	}

	/**
	 * @return the fourG_up_bytes
	 */
	public final long getFourG_up_bytes() {
		return fourG_up_bytes;
	}

	/**
	 * @return the fourG_down_bytes
	 */
	public final long getFourG_down_bytes() {
		return fourG_down_bytes;
	}

	/**
	 * @param ci
	 *            the ci to set
	 */
	public final void setCi(String ci) {
		this.ci = ci;
	}

	/**
	 * @param ciinfo
	 *            the ciinfo to set
	 */
	public final void setCiinfo(Map<String, Object> ciinfo) {
		this.ciinfo = ciinfo;
	}

	/**
	 * @param twoG_up_bytes
	 *            the twoG_up_bytes to set
	 */
	public final void setTwoG_up_bytes(long twoG_up_bytes) {
		this.twoG_up_bytes = twoG_up_bytes;
	}

	/**
	 * @param twoG_down_bytes
	 *            the twoG_down_bytes to set
	 */
	public final void setTwoG_down_bytes(long twoG_down_bytes) {
		this.twoG_down_bytes = twoG_down_bytes;
	}

	/**
	 * @return the score
	 */
	public final long getScore() {
		return score;
	}

	/**
	 * @param score
	 *            the score to set
	 */
	public final void setScore(long score) {
		this.score = score;
	}

	/**
	 * @param threeG_up_bytes
	 *            the threeG_up_bytes to set
	 */
	public final void setThreeG_up_bytes(long threeG_up_bytes) {
		this.threeG_up_bytes = threeG_up_bytes;
	}

	/**
	 * @param threeG_down_bytes
	 *            the threeG_down_bytes to set
	 */
	public final void setThreeG_down_bytes(long threeG_down_bytes) {
		this.threeG_down_bytes = threeG_down_bytes;
	}

	/**
	 * @param fourG_up_bytes
	 *            the fourG_up_bytes to set
	 */
	public final void setFourG_up_bytes(long fourG_up_bytes) {
		this.fourG_up_bytes = fourG_up_bytes;
	}

	/**
	 * @param fourG_down_bytes
	 *            the fourG_down_bytes to set
	 */
	public final void setFourG_down_bytes(long fourG_down_bytes) {
		this.fourG_down_bytes = fourG_down_bytes;
	}

	/**
	 * @return the sms_count
	 */
	public final long getSms_count() {
		return sms_count;
	}

	/**
	 * @return the voice_count
	 */
	public final long getVoice_count() {
		return voice_count;
	}

	/**
	 * @return the location_count
	 */
	public final long getLocation_count() {
		return location_count;
	}

	/**
	 * @return the count
	 */
	public final long getCount() {
		return count;
	}

	/**
	 * @param count
	 *            the count to set
	 */
	public final void setCount(long count) {
		this.count = count;
	}

	/**
	 * @param sms_count
	 *            the sms_count to set
	 */
	public final void setSms_count(long sms_count) {
		this.sms_count = sms_count;
	}

	/**
	 * @param voice_count
	 *            the voice_count to set
	 */
	public final void setVoice_count(long voice_count) {
		this.voice_count = voice_count;
	}

	/**
	 * @param location_count
	 *            the location_count to set
	 */
	public final void setLocation_count(long location_count) {
		this.location_count = location_count;
	}

	/**
	 * @return the numMap
	 */
	public static final Map<String, String> getNumMap() {
		return numMap;
	}

	/**
	 * @return the appTop
	 */
	public final List<Map<String, Object>> getAppTop() {
		return appTop;
	}

	/**
	 * @return the urlTop
	 */
	public final List<Map<String, Object>> getUrlTop() {
		return urlTop;
	}

	/**
	 * @return the hostCount
	 */
	public final List<Map<String, Object>> getHostCount() {
		return hostCount;
	}

	/**
	 * @param numMap
	 *            the numMap to set
	 */
	public static final void setNumMap(Map<String, String> numMap) {
		CiCountInfo.numMap = numMap;
	}

	/**
	 * @param appTop
	 *            the appTop to set
	 */
	public final void setAppTop(List<Map<String, Object>> appTop) {
		this.appTop = appTop;
	}

	/**
	 * @param urlTop
	 *            the urlTop to set
	 */
	public final void setUrlTop(List<Map<String, Object>> urlTop) {
		this.urlTop = urlTop;
	}

	/**
	 * @param hostCount
	 *            the hostCount to set
	 */
	public final void setHostCount(List<Map<String, Object>> hostCount) {
		this.hostCount = hostCount;
	}

	public static void main(String[] args)
			throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		String _word = CiCountInfo.numMap.get("3");
		System.out.println(_word);
		_word = CiCountInfo.numMap.get("2");
		System.out.println(_word);
		_word = CiCountInfo.numMap.get("4");
		System.out.println(_word);

		Map<String, Long> _count = new HashMap<String, Long>();
		for (String s : new String[] { "G_up_bytes", "G_down_bytes" }) {
			for (String nt : CiCountInfo.numMap.values()) {
				String k = nt.concat(s);
				_count.put(k, System.currentTimeMillis());
			}
		}
		CiCountInfo ci = new CiCountInfo();
		for (Map.Entry<String, Long> entry : _count.entrySet()) {
			Method method = ReflectUtil.getFieldSetterMethod(entry.getKey(), CiCountInfo.class, Long.class);
			method.invoke(ci, entry.getValue());
		}

		System.out.println("OK " + ci.getTwoG_up_bytes());
	}

}
