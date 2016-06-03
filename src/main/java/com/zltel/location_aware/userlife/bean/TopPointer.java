package com.zltel.location_aware.userlife.bean;

import java.util.List;

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
	private String lat;
	private String lng;

	private float _lat;
	private float _lng;
	/**
	 * 聚合的点
	 */
	private List<Pointer> pointers;

	/**
	 * @return the score
	 */
	public final long getScore() {
		return score;
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
	 * @param lat
	 *            the lat to set
	 */
	public final void setLat(String lat) {
		this.lat = lat;
	}

	/**
	 * @param lng
	 *            the lng to set
	 */
	public final void setLng(String lng) {
		this.lng = lng;
	}

	/**
	 * @param _lat
	 *            the _lat to set
	 */
	public final void set_lat(float _lat) {
		this._lat = _lat;
	}

	/**
	 * @param _lng
	 *            the _lng to set
	 */
	public final void set_lng(float _lng) {
		this._lng = _lng;
	}

	/**
	 * @param pointers
	 *            the pointers to set
	 */
	public final void setPointers(List<Pointer> pointers) {
		this.pointers = pointers;
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
