package com.zltel.location_aware.userlife.bean;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.common.utils.time.DateTimeUtil;
import com.zltel.location_aware.userlife.service.UserlifeService;

public class Pointer {
	private static final long MAX_TIMERANGE = 1000 * 60 * 60 * 2;
	private static Date _baseDT = null;
	public static final String SOURCE_GN = "gn";
	public static final String SOURCE_CS = "cs";

	public static double GN_PERCENT = 1.2;
	public static double CS_PERCENT = 1;
	/** 短信业务 **/
	public static final String BUSSINESS_SMS = "sms";
	/** 语音业务 **/
	public static final String BUSSINESS_VOICE = "voice";
	/** 定位业务 **/
	public static final String BUSSINESS_LOCATION = "location";
	/** 数据业务 **/
	public static final String BUSSINESS_DATA = "data";

	private static final Map<String, String> _map = ConfigUtil.resolveConfigProFile("userlife.properties");
	static {
		String _gnp = ConfigUtil.getConfigValue(_map, "GN.PERCENT", "1.2");
		String _csp = ConfigUtil.getConfigValue(_map, "CS.PERCENT", "1");

		GN_PERCENT = Double.valueOf(_gnp);
		CS_PERCENT = Double.valueOf(_csp);
	}
	/** 业务类型 **/
	private String business_type;
	private String stime;
	private String etime;
	private String lac;
	private String ci;
	/** 来源 **/
	private String source;
	/** 网络类型： 2，3，4G **/
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
	private String imei;
	/** 上传 字节数 **/
	private String up_bytes;
	/** 下载 字节数 **/
	private String down_bytes;
	/*** 使用APPID */
	private String appid;
	/** 使用url **/
	private String url;
	private String host;

	// --------------------------------
	/** 合并点的范围 **/
	private List<String> timeRanges;
	/** 聚合次数 **/
	private int groupCount;

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
			if (SOURCE_GN.equals(this.source)) {
				this.score = (long) (this.score * GN_PERCENT);
			} else if (SOURCE_CS.equals(this.source)) {
				this.score = (long) (this.score * CS_PERCENT);
			}
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
		if (null != _stime) {
			this.stime = UserlifeService.sdf.format(_stime);
		}
	}

	/**
	 * @param _etime
	 *            the _etime to set
	 */
	public final void _etime(Date _etime) {
		this._etime = _etime;
		if (null != _etime) {
			this.etime = UserlifeService.sdf.format(_etime);
		}
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

	/**
	 * @return the timeRanges
	 */
	public final List<String> getTimeRanges() {
		return timeRanges;
	}

	/**
	 * @param timeRanges
	 *            the timeRanges to set
	 */
	public final void setTimeRanges(List<String> timeRanges) {
		this.timeRanges = timeRanges;
	}

	/**
	 * @return the groupCount
	 */
	public final int getGroupCount() {
		return groupCount;
	}

	/**
	 * @param groupCount
	 *            the groupCount to set
	 */
	public final void setGroupCount(int groupCount) {
		this.groupCount = groupCount;
	}

	/**
	 * @return the business_type
	 */
	public final String getBusiness_type() {
		return business_type;
	}

	/**
	 * @param business_type
	 *            the business_type to set
	 */
	public final void setBusiness_type(String business_type) {
		this.business_type = business_type;
	}

	/**
	 * @return the imei
	 */
	public final String getImei() {
		return imei;
	}

	/**
	 * @param imei
	 *            the imei to set
	 */
	public final void setImei(String imei) {
		this.imei = imei;
	}

	/**
	 * @return the up_bytes
	 */
	public final String getUp_bytes() {
		return up_bytes;
	}

	/**
	 * @return the down_bytes
	 */
	public final String getDown_bytes() {
		return down_bytes;
	}

	/**
	 * @param up_bytes
	 *            the up_bytes to set
	 */
	public final void setUp_bytes(String up_bytes) {
		this.up_bytes = up_bytes;
	}

	/**
	 * @param down_bytes
	 *            the down_bytes to set
	 */
	public final void setDown_bytes(String down_bytes) {
		this.down_bytes = down_bytes;
	}

	/**
	 * @return the appid
	 */
	public final String getAppid() {
		return appid;
	}

	/**
	 * @return the url
	 */
	public final String getUrl() {
		return url;
	}

	/**
	 * @param appid
	 *            the appid to set
	 */
	public final void setAppid(String appid) {
		this.appid = appid;
	}

	/**
	 * @param url
	 *            the url to set
	 */
	public final void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @return the host
	 */
	public final String getHost() {
		return host;
	}

	/**
	 * @param host
	 *            the host to set
	 */
	public final void setHost(String host) {
		this.host = host;
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
