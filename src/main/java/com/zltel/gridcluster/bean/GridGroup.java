package com.zltel.gridcluster.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.zltel.gridcluster.service.GridClusterService;
import com.zltel.location_aware.userlife.bean.Pointer;

public class GridGroup {

	private long score;
	private Set<Group> neighbors = new HashSet<Group>();
	private int x;
	private int y;

	/**
	 * 获取 对象矩阵
	 * 
	 * @return
	 */
	public List<Map<String, String>> getNeighborsMartx() {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		for (Group g : neighbors) {
			Map<String, String> m = new HashMap<String, String>();
			m.put("x", g.getX() + "");
			m.put("y", g.getY() + "");
			m.put("score", g.getScore() + "");
			list.add(m);
		}
		return list;
	}

	public void addNeighbor(Group group) {
		neighbors.add(group);
	}

	/**
	 * 获取 所有 pointer
	 * 
	 * @return
	 */
	public List<Pointer> allPointers() {
		List<Pointer> ret = new ArrayList<Pointer>();
		for (Group g : neighbors) {
			ret.addAll(g.getPointers());
		}
		return ret;
	}

	/**
	 * 判断 是否 是邻居<br>
	 * 1. 点必须没有在已有 邻居点钟 <br>
	 * 2. 距离中心点距离不能超过 最大距离
	 * 
	 * @param _ng
	 * @return
	 */
	public boolean checkNeighbor(Group _ng, long neighbor_distince) {
		if (this.getNeighbors().contains(_ng)) {
			return false;
		}
		if (GridClusterService.checkNeighbor(x, y, _ng, neighbor_distince)) {
			return true;
		}
		return false;
	}

	public long calcScore() {
		long score = 0;
		for (Group g : neighbors) {
			score += g.calcScore();
		}
		this.score = score;
		return score;
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
	 * @return the neighbors
	 */
	public final Set<Group> getNeighbors() {
		return neighbors;
	}

	/**
	 * @param neighbors
	 *            the neighbors to set
	 */
	public final void setNeighbors(Set<Group> neighbors) {
		this.neighbors = neighbors;
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
		result = prime * result + ((neighbors == null) ? 0 : neighbors.hashCode());
		result = prime * result + (int) (score ^ (score >>> 32));
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
		GridGroup other = (GridGroup) obj;
		if (neighbors == null) {
			if (other.neighbors != null)
				return false;
		} else if (!neighbors.equals(other.neighbors))
			return false;
		if (score != other.score)
			return false;
		return true;
	}

}
