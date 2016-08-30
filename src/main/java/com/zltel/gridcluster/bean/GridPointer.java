package com.zltel.gridcluster.bean;

import com.zltel.location_aware.userlife.bean.Pointer;

public class GridPointer {
	private double lng;
	private double lat;

	private int x;
	private int y;

	private Pointer pointer;

	public GridPointer() {

	}

	public GridPointer(Pointer pointer) {
		this.pointer = pointer;
		this.lng = pointer.get_lng();
		this.lat = pointer.get_lat();
	}

	/**
	 * @return the lng
	 */
	public final double getLng() {
		return lng;
	}

	/**
	 * @return the lat
	 */
	public final double getLat() {
		return lat;
	}

	/**
	 * @return the pointer
	 */
	public final Pointer getPointer() {
		return pointer;
	}

	/**
	 * @param lng
	 *            the lng to set
	 */
	public final void setLng(double lng) {
		this.lng = lng;
	}

	/**
	 * @param lat
	 *            the lat to set
	 */
	public final void setLat(double lat) {
		this.lat = lat;
	}

	/**
	 * @param pointer
	 *            the pointer to set
	 */
	public final void setPointer(Pointer pointer) {
		this.pointer = pointer;
	}

	/**
	 * @return the x
	 */
	public final int getX() {
		return x;
	}

	/**
	 * @return the y
	 */
	public final int getY() {
		return y;
	}

	/**
	 * @param x
	 *            the x to set
	 */
	public final void setX(int x) {
		this.x = x;
	}

	/**
	 * @param y
	 *            the y to set
	 */
	public final void setY(int y) {
		this.y = y;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(lat);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(lng);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((pointer == null) ? 0 : pointer.hashCode());
		result = prime * result + x;
		result = prime * result + y;
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GridPointer other = (GridPointer) obj;
		if (Double.doubleToLongBits(lat) != Double.doubleToLongBits(other.lat))
			return false;
		if (Double.doubleToLongBits(lng) != Double.doubleToLongBits(other.lng))
			return false;
		if (pointer == null) {
			if (other.pointer != null)
				return false;
		} else if (!pointer.equals(other.pointer))
			return false;
		if (x != other.x)
			return false;
		if (y != other.y)
			return false;
		return true;
	}

}
