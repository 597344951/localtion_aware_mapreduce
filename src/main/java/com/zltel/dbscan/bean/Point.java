package com.zltel.dbscan.bean;

import com.zltel.location_aware.userlife.bean.Pointer;

public class Point {

	private float lat;
	private float lng;

	private boolean isKey;
	private boolean isClassed;

	private Pointer pointer;

	public Point(Pointer p) {
		this.lat = p.get_lat();
		this.lng = p.get_lng();
		this.pointer = p;
	}

	public boolean isKey() {
		return isKey;
	}

	public void setKey(boolean isKey) {
		this.isKey = isKey;
		this.isClassed = true;
	}

	public boolean isClassed() {
		return isClassed;
	}

	public void setClassed(boolean isClassed) {
		this.isClassed = isClassed;
	}

	public String print() {
		return "<" + this.lat + "," + this.lng + ">";
	}

	/**
	 * @return the pointer
	 */
	public final Pointer getPointer() {
		return pointer;
	}

	/**
	 * @param pointer
	 *            the pointer to set
	 */
	public final void setPointer(Pointer pointer) {
		this.pointer = pointer;
	}

}