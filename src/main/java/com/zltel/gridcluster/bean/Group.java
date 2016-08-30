package com.zltel.gridcluster.bean;

import java.util.ArrayList;
import java.util.List;

import com.zltel.location_aware.userlife.bean.Pointer;

public class Group {
	/** 分组id **/
	private String id;
	/** x,y **/
	private int x;
	private int y;

	private long score;

	private List<GridPointer> gridPointers = new ArrayList<GridPointer>();
	private List<Pointer> pointers = new ArrayList<Pointer>();

	/**
	 * 计算 索引用的 key
	 * 
	 * @param x
	 * @param y
	 * @return
	 */
	public static String markKey(int x, int y) {
		StringBuffer ret = new StringBuffer();
		ret.append(x).append("_").append(y);
		return ret.toString();
	}

	public static int[] getKeyXY(String markKey) {
		String[] ss = markKey.split("_");
		int[] ret = new int[2];
		ret[0] = Integer.valueOf(ss[0]);
		ret[1] = Integer.valueOf(ss[1]);
		return ret;
	}

	/**
	 * 计算 分数
	 */
	public long calcScore() {
		long score = 0;
		if (pointers != null) {
			for (Pointer p : pointers) {
				score += p.getScore();
			}
		}
		this.score = score;
		return score;
	}

	public void addGridPointer(GridPointer gp) {
		this.gridPointers.add(gp);
		this.pointers.add(gp.getPointer());
	}

	/**
	 * @return the id
	 */
	public final String getId() {
		return id;
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
	 * @param id
	 *            the id to set
	 */
	public final void setId(String id) {
		this.id = id;
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

	/**
	 * @return the gridPointers
	 */
	public final List<GridPointer> getGridPointers() {
		return gridPointers;
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
	 * @return the pointers
	 */
	public final List<Pointer> getPointers() {
		return pointers;
	}

	/**
	 * @param gridPointers
	 *            the gridPointers to set
	 */
	public final void setGridPointers(List<GridPointer> gridPointers) {
		this.gridPointers = gridPointers;
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
		builder.append("Group [score:");
		builder.append(score);
		builder.append(",x:").append(x);
		builder.append(",y:").append(y);
		builder.append("]");
		return builder.toString();
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
		result = prime * result + ((gridPointers == null) ? 0 : gridPointers.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((pointers == null) ? 0 : pointers.hashCode());
		result = prime * result + (int) (score ^ (score >>> 32));
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
		Group other = (Group) obj;
		if (gridPointers == null) {
			if (other.gridPointers != null)
				return false;
		} else if (!gridPointers.equals(other.gridPointers))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (pointers == null) {
			if (other.pointers != null)
				return false;
		} else if (!pointers.equals(other.pointers))
			return false;
		if (score != other.score)
			return false;
		if (x != other.x)
			return false;
		if (y != other.y)
			return false;
		return true;
	}

}
