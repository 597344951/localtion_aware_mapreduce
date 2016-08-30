package com.zltel.gridcluster.bean;

import java.util.Map;

import com.zltel.common.utils.conf.ConfigUtil;

public class GroupParams {
	/****/
	private int first_step;
	private int first_radius;
	private long first_max_neighbor_distince;
	private long first_score_threshold;

	private int second_step;
	private int second_radius;
	private long second_max_neighbor_distince;
	private long second_score_threshold;

	public static GroupParams createGroupParams(Map<String, String> _params) {
		String key = null;
		String v = null;

		GroupParams gp = new GroupParams();

		key = "userlife.grid.first.grid_radius";
		v = ConfigUtil.getConfigValue(_params, key, "100");
		gp.setFirst_radius(Integer.valueOf(v));
		key = "userlife.grid.first.grid_step";
		v = ConfigUtil.getConfigValue(_params, key, "2");
		gp.setFirst_step(Integer.valueOf(v));
		key = "userlife.grid.first.max_neighbor_distince";
		v = ConfigUtil.getConfigValue(_params, key, "2");
		gp.setFirst_max_neighbor_distince(Integer.valueOf(v));
		key = "userlife.grid.first.score_threshold";
		v = ConfigUtil.getConfigValue(_params, key, "3000");
		gp.setFirst_score_threshold(Long.valueOf(v));

		key = "userlife.grid.second.grid_radius";
		v = ConfigUtil.getConfigValue(_params, key, "1000");
		gp.setSecond_radius(Integer.valueOf(v));
		key = "userlife.grid.second.grid_step";
		v = ConfigUtil.getConfigValue(_params, key, "2");
		gp.setSecond_step(Integer.valueOf(v));
		key = "userlife.grid.second.max_neighbor_distince";
		v = ConfigUtil.getConfigValue(_params, key, "2");
		gp.setSecond_max_neighbor_distince(Integer.valueOf(v));
		key = "userlife.grid.second.score_threshold";
		v = ConfigUtil.getConfigValue(_params, key, "500");
		gp.setSecond_score_threshold(Long.valueOf(v));

		return gp;
	}

	/**
	 * @return the first_step
	 */
	public final int getFirst_step() {
		return first_step;
	}

	/**
	 * @return the first_radius
	 */
	public final int getFirst_radius() {
		return first_radius;
	}

	/**
	 * @return the first_max_neighbor_distince
	 */
	public final long getFirst_max_neighbor_distince() {
		return first_max_neighbor_distince;
	}

	/**
	 * @return the first_score_threshold
	 */
	public final long getFirst_score_threshold() {
		return first_score_threshold;
	}

	/**
	 * @return the second_step
	 */
	public final int getSecond_step() {
		return second_step;
	}

	/**
	 * @return the second_radius
	 */
	public final int getSecond_radius() {
		return second_radius;
	}

	/**
	 * @return the second_max_neighbor_distince
	 */
	public final long getSecond_max_neighbor_distince() {
		return second_max_neighbor_distince;
	}

	/**
	 * @return the second_score_threshold
	 */
	public final long getSecond_score_threshold() {
		return second_score_threshold;
	}

	/**
	 * @param first_step
	 *            the first_step to set
	 */
	public final void setFirst_step(int first_step) {
		this.first_step = first_step;
	}

	/**
	 * @param first_radius
	 *            the first_radius to set
	 */
	public final void setFirst_radius(int first_radius) {
		this.first_radius = first_radius;
	}

	/**
	 * @param first_max_neighbor_distince
	 *            the first_max_neighbor_distince to set
	 */
	public final void setFirst_max_neighbor_distince(long first_max_neighbor_distince) {
		this.first_max_neighbor_distince = first_max_neighbor_distince;
	}

	/**
	 * @param first_score_threshold
	 *            the first_score_threshold to set
	 */
	public final void setFirst_score_threshold(long first_score_threshold) {
		this.first_score_threshold = first_score_threshold;
	}

	/**
	 * @param second_step
	 *            the second_step to set
	 */
	public final void setSecond_step(int second_step) {
		this.second_step = second_step;
	}

	/**
	 * @param second_radius
	 *            the second_radius to set
	 */
	public final void setSecond_radius(int second_radius) {
		this.second_radius = second_radius;
	}

	/**
	 * @param second_max_neighbor_distince
	 *            the second_max_neighbor_distince to set
	 */
	public final void setSecond_max_neighbor_distince(long second_max_neighbor_distince) {
		this.second_max_neighbor_distince = second_max_neighbor_distince;
	}

	/**
	 * @param second_score_threshold
	 *            the second_score_threshold to set
	 */
	public final void setSecond_score_threshold(long second_score_threshold) {
		this.second_score_threshold = second_score_threshold;
	}

}
