package com.zltel.location_aware.userlife.bean;

import java.text.ParseException;
import java.util.Date;

import com.zltel.common.utils.string.StringUtil;
import com.zltel.common.utils.time.DateTimeUtil;
import com.zltel.location_aware.userlife.service.UserlifeService;

public class Pointer {
	private static final long MAX_TIMERANGE = 1000 * 60 * 30;
	private static Date _baseDT = null;
	public static final String SOURCE_GN = "gn";
	public static final String SOURCE_CS = "cs";

	private String stime;
	private String etime;
	private String lac;
	private String ci;
	/** 来源 **/
	private String source;
	private String nettype;

	private String cell;
	private String lat;
	private String lng;

	private Date _stime;
	private Date _etime;
	private float _lat;
	private float _lng;
	/** 使用时长 **/
	private long time;

	/** 标签 **/
	private String tag;
	/** 标签 描述 **/
	private String tagDescript;
	/** 权重 **/
	private float weight;
	/** 是否是工作日 **/
	private Boolean weekday;

	private long score;

	private String imsi;

	static {
		try {
			_baseDT = UserlifeService.sdf.parse("20150101000000");
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	// ----------------------------------------
	public boolean avaliable() {
		return StringUtil.isNotNullAndEmpty(stime, etime, lac, ci, source) && biggerThanTime(_stime)
				&& biggerThanTime(_etime);
	}

	/**
	 * 必须比基准时间要大
	 * 
	 * @param dt
	 * @return
	 */
	public boolean biggerThanTime(Date dt) {
		if (dt == null || dt.getTime() < _baseDT.getTime()) {
			return false;
		}
		return true;
	}

	/**
	 * 时间范围
	 * 
	 * @return 结束时间-开始时间 的毫秒数
	 * @warn 如果时间差值 大于一定值则 返回最大值
	 */
	public long timeRange() {
		long r = this._etime.getTime() - this._stime.getTime();
		if (r > MAX_TIMERANGE) {
			r = MAX_TIMERANGE;
		}
		return r == 0 ? 1000 : r;
	}

	/**
	 * 计算 分数 (时常 * 价值 * 占比 )
	 * 
	 * @return
	 */
	public long calcScore(int worth) {
		if (0 == score) {
			score = (long) (this.timeRange() * worth / 1000 * this.weight);
		}
		return score;
	}

	/**
	 * 是否是 工作日
	 * 
	 * @return
	 */
	public boolean isWeekday() {
		if (weekday == null) {
			DateTimeUtil dtu = DateTimeUtil.getInstince();
			dtu.setTime(_stime());
			weekday = dtu.isWeekday();
		}
		return weekday;
	}

	// ----------------------------------------

	/**
	 * @return the stime
	 */
	public final String getStime() {
		return stime;
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
	 * @return the etime
	 */
	public final String getEtime() {
		return etime;
	}

	/**
	 * @return the lac
	 */
	public final String getLac() {
		return lac;
	}

	/**
	 * @return the ci
	 */
	public final String getCi() {
		return ci;
	}

	/**
	 * @return the cell
	 */
	public final String getCell() {
		return cell;
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
	 * @param cell
	 *            the cell to set
	 */
	public final void setCell(String cell) {
		this.cell = cell;
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
	 * @param stime
	 *            the stime to set
	 */
	public final void setStime(String stime) {
		this.stime = stime;
		if (StringUtil.isNotNullAndEmpty(stime)) {
			try {
				this._stime = UserlifeService.sdf.parse(this.stime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param etime
	 *            the etime to set
	 */
	public final void setEtime(String etime) {
		this.etime = etime;
		if (StringUtil.isNotNullAndEmpty(etime)) {
			try {
				this._etime = UserlifeService.sdf.parse(this.etime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param lac
	 *            the lac to set
	 */
	public final void setLac(String lac) {
		this.lac = lac;
	}

	/**
	 * @return the source
	 */
	public final String getSource() {
		return source;
	}

	/**
	 * @param source
	 *            the source to set
	 */
	public final void setSource(String source) {
		this.source = source;
	}

	/**
	 * @param ci
	 *            the ci to set
	 */
	public final void setCi(String ci) {
		this.ci = ci;
	}

	/**
	 * @return the nettype
	 */
	public final String getNettype() {
		return nettype;
	}

	/**
	 * @param nettype
	 *            the nettype to set
	 */
	public final void setNettype(String nettype) {
		this.nettype = nettype;
	}

	/**
	 * @return the _stime
	 */
	public final Date _stime() {
		return _stime;
	}

	/**
	 * @return the _etime
	 */
	public final Date _etime() {
		return _etime;
	}

	/**
	 * @param _stime
	 *            the _stime to set
	 */
	public final void _stime(Date _stime) {
		this._stime = _stime;
	}

	/**
	 * @param _etime
	 *            the _etime to set
	 */
	public final void _etime(Date _etime) {
		this._etime = _etime;
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
	 * @return the tag
	 */
	public final String getTag() {
		return tag;
	}

	/**
	 * @param tag
	 *            the tag to set
	 */
	public final void setTag(String tag) {
		this.tag = tag;
	}

	/**
	 * @return the weight
	 */
	public final float getWeight() {
		return weight;
	}

	/**
	 * @param weight
	 *            the weight to set
	 */
	public final void setWeight(float weight) {
		this.weight = weight;
	}

	/**
	 * @param weekday
	 *            the weekday to set
	 */
	public final void setWeekday(boolean weekday) {
		this.weekday = weekday;
	}

	/**
	 * @return the time
	 */
	public final long getTime() {
		return time;
	}

	/**
	 * @param time
	 *            the time to set
	 */
	public final void setTime(long time) {
		this.time = time;
	}

	/**
	 * @return the tagDescript
	 */
	public final String getTagDescript() {
		return tagDescript;
	}

	/**
	 * @return the weekday
	 */
	public final Boolean getWeekday() {
		return weekday;
	}

	/**
	 * @param tagDescript
	 *            the tagDescript to set
	 */
	public final void setTagDescript(String tagDescript) {
		this.tagDescript = tagDescript;
	}

	/**
	 * @param weekday
	 *            the weekday to set
	 */
	public final void setWeekday(Boolean weekday) {
		this.weekday = weekday;
	}

	/**
	 * @return the imsi
	 */
	public final String getImsi() {
		return imsi;
	}

	/**
	 * @param imsi
	 *            the imsi to set
	 */
	public final void setImsi(String imsi) {
		this.imsi = imsi;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Pointer [stime=");
		builder.append(stime);
		builder.append(", etime=");
		builder.append(etime);
		builder.append(", lac=");
		builder.append(lac);
		builder.append(", ci=");
		builder.append(ci);
		builder.append(", source=");
		builder.append(source);
		builder.append(", nettype=");
		builder.append(nettype);
		builder.append(", tag=");
		builder.append(this.tag);
		builder.append("]");
		return builder.toString();
	}

	public static void main(String[] args) throws Exception {
		String dts = "1463196948000";
		Pointer pt = new Pointer();
		pt.setStime(dts);
		pt.setEtime(dts);
		pt.setLac(dts);
		pt.setCi(dts);
		pt.setSource(dts);
		System.out.println(pt);
		System.out.println(pt.avaliable());
	}
}
