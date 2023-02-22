package com.taos.example.highvolume;

import java.math.BigDecimal;
import java.util.Date;

/**
 * 设备定位
 * @author mumud
 */
public class DeviceLocation {
    private Long _ts;
    /**
     * 设备id
     */
    private String deviceId;
    /**
     * 定位时间
     */
    private Date locTime;
    /**
     * 经度
     */
    private BigDecimal lng;

    /**
     * 纬度
     */
    private BigDecimal lat;


    public Long get_ts() {
        return _ts;
    }

    public void set_ts(Long _ts) {
        this._ts = _ts;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Date getLocTime() {
        return locTime;
    }

    public void setLocTime(Date locTime) {
        this.locTime = locTime;
    }

    public BigDecimal getLng() {
        return lng;
    }

    public void setLng(BigDecimal lng) {
        this.lng = lng;
    }

    public BigDecimal getLat() {
        return lat;
    }

    public void setLat(BigDecimal lat) {
        this.lat = lat;
    }
}
