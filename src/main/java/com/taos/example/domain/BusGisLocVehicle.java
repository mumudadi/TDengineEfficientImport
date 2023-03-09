package com.taos.example.domain;


import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

/**
 * 车辆定位对象 bus_gis_loc_vehicle
 *
 * @author shangma
 * @date 2022-02-15
 */

public class BusGisLocVehicle implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 设备id
     */
    private String deviceId;
    /**
     * 车牌号
     */
    private String vehicleNumber;

    /**
     * 经度
     */
    private BigDecimal lng;

    /**
     * 纬度
     */
    private BigDecimal lat;

    /**
     * 地址
     */
    private String address;

    /**
     * 定位时间
     */
    private Date locTime;
    /**
     * 定位时间戳(纳秒)
     */
    private Long _ts;
    /**
     * 千米/小时
     */
    private BigDecimal spd;

    /**
     * 正北，大于 0 且小于 90：东北，等于 90：正东，大于 90 且小于180：东南，等于 180：正南，大于 180 且小于 270：西南， * 等于 270：正西，大于270 且小于等于 359：西北，其他：未知
     */
    private String drc;

    /**
     * 省
     */
    private String province;

    /**
     * 市
     */
    private String city;

    /**
     * 县
     */
    private String country;

    /**
     * 上报里程（KM）
     */
    private BigDecimal mil;

    /**
     * 1离线 0在线
     */
    private String offline;

    /**
     * 离线时长（分钟）
     */
    private Integer offlineTime;

    /**
     * 距离出发点里程（KM）
     */
    private BigDecimal runDistance;

    /**
     * 剩余运距（KM）
     */
    private BigDecimal remainDistance;

    /**
     * 预计到达时间
     */
    private Date estimateTime;

    /**
     * 参考运单ID
     */
    private Integer refWaybillId;

    /**
     * 参考运单编号
     */
    private String refWaybillNo;

    /**
     * 采集时间
     */
    private Date createTime;

    /**
     * 车牌颜色
     */
    private String color;
}
