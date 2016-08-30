package com.zltel.location_aware.userlife.bean;

import java.util.List;
import java.util.Map;

/**
 * 聚合 点
 * 
 * @author Wangch
 *
 */
public class TopPointer {
	/**
	 * 分数
	 */
	private long score;
	private float _lat;
	private float _lng;

	private String lat;
	private String lng;

	/*** 聚合点个数 */
	private long pcount;
	/** 总统计点数 **/
	private long totalCount;
	/*** 聚合的点 */

	private List<Pointer> pointers;

	/** 点分布范围 [{tag:,count:,percent:}] **/
	private String distributionPoint;
	/** 每天分数分布 **/
	private String dayScoreRank;

	// -----DEBUG INFO-----
	/** DBSCAN 算法耗时 **/
	private long dbc_time;
	/** 合并算法耗时 **/
	private long merg_time;
	// -----DEBUG INFO-----

	// ----------2016.6.28------------
	/** 相关小区聚合信息 **/
	private List<CiCountInfo> ciCountInfos;
	private String imsi;
	private String imei;
	private String phone_model;
	/** [{tag:,score:,percent:}] **/
	private String bussRank;// 业务 分布状况

	//
	/** 使用率 top **/
	private List<Map<String, Object>> appTop;
	private List<Map<String, Object>> urlTop;
	private List<Map<String, Object>> hostCount;

	private String type;

	// ------------------
	private String x;
	private String y;

	private List<Map<String, String>> neighbors;
	private String neighborTxt;

	// -------------------------------------------------------------------------------------

	/**
	 * @return the score
	 */
	public final long getScore() {
		return score;
	}

	/**
	 * @return the _lat
	 */
	public final float get_lat() {
		return _lat;
	}

	/**
	 * @return the _lng
	 */
	public final float get_lng() {
		return _lng;
	}

	/**
	 * @return the lat
	 */
	public final String getLat() {
		return lat;
	}

	/**
	 * @return the lng
	 */
	public final String getLng() {
		return lng;
	}

	/**
	 * @param _lat
	 *            the _lat to set
	 */
	public final void set_lat(float _lat) {
		this._lat = _lat;
		this.lat = String.valueOf(_lat);
	}

	/**
	 * @param _lng
	 *            the _lng to set
	 */
	public final void set_lng(float _lng) {
		this._lng = _lng;
		this.lng = String.valueOf(_lng);
	}

	/**
	 * @param lat
	 *            the lat to set
	 */
	public final void setLat(String lat) {
		this.lat = lat;
		this._lat = Float.valueOf(lat);
	}

	/**
	 * @param lng
	 *            the lng to set
	 */
	public final void setLng(String lng) {
		this.lng = lng;
		this._lng = Float.valueOf(lng);
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

	/**
	 * @return the merg_time
	 */
	public final long getMerg_time() {
		return merg_time;
	}

	/**
	 * @param merg_time
	 *            the merg_time to set
	 */
	public final void setMerg_time(long merg_time) {
		this.merg_time = merg_time;
	}

	/**
	 * @return the pointers
	 */
	public final List<Pointer> getPointers() {
		return pointers;
	}

	/**
	 * @param score
	 *            the score to set
	 */
	public final void setScore(long score) {
		this.score = score;
	}

	/**
	 * @param pointers
	 *            the pointers to set
	 */
	public final void setPointers(List<Pointer> pointers) {
		this.pointers = pointers;
	}

	/**
	 * @return the pcount
	 */
	public final long getPcount() {
		return pcount;
	}

	/**
	 * @param pcount
	 *            the pcount to set
	 */
	public final void setPcount(long pcount) {
		this.pcount = pcount;
	}

	/**
	 * @return the totalCount
	 */
	public final long getTotalCount() {
		return totalCount;
	}

	/**
	 * @param totalCount
	 *            the totalCount to set
	 */
	public final void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	/**
	 * @return the distributionPoint
	 */
	public final String getDistributionPoint() {
		return distributionPoint;
	}

	/**
	 * @param distributionPoint
	 *            the distributionPoint to set
	 */
	public final void setDistributionPoint(String distributionPoint) {
		this.distributionPoint = distributionPoint;
	}

	/**
	 * @return the dbc_time
	 */
	public final long getDbc_time() {
		return dbc_time;
	}

	/**
	 * @param dbc_time
	 *            the dbc_time to set
	 */
	public final void setDbc_time(long dbc_time) {
		this.dbc_time = dbc_time;
	}

	/**
	 * @return the dayScoreRank
	 */
	public final String getDayScoreRank() {
		return dayScoreRank;
	}

	/**
	 * @param dayScoreRank
	 *            the dayScoreRank to set
	 */
	public final void setDayScoreRank(String dayScoreRank) {
		this.dayScoreRank = dayScoreRank;
	}

	/**
	 * @return the imsi
	 */
	public final String getImsi() {
		return imsi;
	}

	/**
	 * @return the imei
	 */
	public final String getImei() {
		return imei;
	}

	/**
	 * @return the phone_model
	 */
	public final String getPhone_model() {
		return phone_model;
	}

	/**
	 * @return the ciCountInfos
	 */
	public final List<CiCountInfo> getCiCountInfos() {
		return ciCountInfos;
	}

	/**
	 * @param ciCountInfos
	 *            the ciCountInfos to set
	 */
	public final void setCiCountInfos(List<CiCountInfo> ciCountInfos) {
		this.ciCountInfos = ciCountInfos;
	}

	/**
	 * @param imsi
	 *            the imsi to set
	 */
	public final void setImsi(String imsi) {
		this.imsi = imsi;
	}

	/**
	 * @param imei
	 *            the imei to set
	 */
	public final void setImei(String imei) {
		this.imei = imei;
	}

	/**
	 * @param phone_model
	 *            the phone_model to set
	 */
	public final void setPhone_model(String phone_model) {
		this.phone_model = phone_model;
	}

	/**
	 * @return the bussRank
	 */
	public final String getBussRank() {
		return bussRank;
	}

	/**
	 * @param bussRank
	 *            the bussRank to set
	 */
	public final void setBussRank(String bussRank) {
		this.bussRank = bussRank;
	}

	/**
	 * @return the type
	 */
	public final String getType() {
		return type;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public final void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the x
	 */
	public final String getX() {
		return x;
	}

	/**
	 * @return the y
	 */
	public final String getY() {
		return y;
	}

	/**
	 * @return the neighbors
	 */
	public final List<Map<String, String>> getNeighbors() {
		return neighbors;
	}

	/**
	 * @param x
	 *            the x to set
	 */
	public final void setX(String x) {
		this.x = x;
	}

	/**
	 * @param y
	 *            the y to set
	 */
	public final void setY(String y) {
		this.y = y;
	}

	/**
	 * @param neighbors
	 *            the neighbors to set
	 */
	public final void setNeighbors(List<Map<String, String>> neighbors) {
		this.neighbors = neighbors;
	}

	/**
	 * @return the neighborTxt
	 */
	public final String getNeighborTxt() {
		return neighborTxt;
	}

	/**
	 * @param neighborTxt
	 *            the neighborTxt to set
	 */
	public final void setNeighborTxt(String neighborTxt) {
		this.neighborTxt = neighborTxt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TopPointer [score=");
		builder.append(score);
		builder.append(", _lat=");
		builder.append(_lat);
		builder.append(", _lng=");
		builder.append(_lng);
		builder.append(", pointers=");
		if (pointers != null) {
			builder.append(pointers.size());
		}
		builder.append("]");
		return builder.toString();
	}

}
