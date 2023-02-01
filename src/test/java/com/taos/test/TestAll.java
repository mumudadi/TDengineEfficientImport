package com.taos.test;

import cn.hutool.core.collection.LineIter;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.lang.Console;
import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.simple.SimpleDataSource;
import cn.hutool.db.handler.EntityListHandler;
import cn.hutool.db.sql.SqlExecutor;
import com.taos.example.*;
import org.junit.FixMethodOrder;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Date;
import java.util.List;

@FixMethodOrder
public class TestAll {
    private String[] args = new String[]{};

    public void dropDB(String dbName) throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop database if exists " + dbName);
            }
        }
    }

    public void insertData() throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-03 14:38:05.000',10.30000,219,0.31000)\n" +
                        "                        power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-03 15:38:15.000',12.60000,218,0.33000)\n" +
                        "                        power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-03 15:38:16.800',12.30000,221,0.31000)\n" +
                        "                        power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES('2018-10-03 15:38:16.650',10.30000,218,0.25000)\n" +
                        "                        power.d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES('2018-10-03 15:38:05.500',11.80000,221,0.28000)\n" +
                        "                        power.d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES('2018-10-03 15:38:16.600',13.40000,223,0.29000)\n" +
                        "                        power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 15:38:05.000',10.80000,223,0.29000)\n" +
                        "                        power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 15:38:06.000',10.80000,223,0.29000)\n" +
                        "                        power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 15:38:07.000',10.80000,223,0.29000)\n" +
                        "                        power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 15:38:08.500',11.50000,221,0.35000)";

                stmt.execute(sql);
            }
        }
    }

    @Test
    public void testJNIConnect() throws SQLException {
        JNIConnectExample.main(args);
    }

    @Test
    public void testRestConnect() throws SQLException {
        RESTConnectExample.main(args);
    }

    @Test
    public void testRestInsert() throws SQLException {
        dropDB("power");
        RestInsertExample.main(args);
        RestQueryExample.main(args);
    }

    @Test
    public void testStmtInsert() throws SQLException {
        dropDB("power");
        StmtInsertExample.main(args);
    }

    @Test
    public void testSubscribe() {

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(1000);
                insertData();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        thread.start();
        SubscribeDemo.main(args);
    }

    @Test
    public void testSchemaless() throws SQLException {
        LineProtocolExample.main(args);
        TelnetLineProtocolExample.main(args);
        // for json protocol, tags may be double type. but for telnet protocol tag must be nchar type.
        // To avoid type mismatch, we delete database test.
        dropDB("test");
        JSONProtocolExample.main(args);
    }

    public static void main(String[] args) throws SQLException{
       /* final LineIter lineIter = new LineIter(ResourceUtil.getUtf8Reader("C:\\D\\DevelopProject\\SqlBackUp\\bus_gis_loc_vehicle.sql"));
        for (String line : lineIter) {
            Console.log(line);
        }*/
        /*String jdbcUrl = "jdbc:TAOS://192.168.0.66:6030?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try (Statement stmt = conn.createStatement()) {
                *//*String sql = "INSERT INTO demo.`bus_gis_loc_vehicle` (`locvehicle_id`, `vehicle_number`, `lng`, `lat`, `address`, `loc_time`, `spd`, `drc`, `province`, `city`, `country`, `mil`, `offline`, `offline_time`, `run_distance`, `remain_distance`, `estimate_time`, `ref_waybill_id`, `ref_waybill_no`, `create_time`) VALUES\n" +
                        "\t(6, '鲁NY2667', 116.778720706128, 38.385518212107, '河北省沧州市沧县朔黄铁路桥,东北方向,665.7米', '2022-07-03 15:23:53', 77.00, '21', '河北省', '沧州市', '沧县', 300505.90, '0', NULL, 198.90, 100.20, '2022-07-06 16:46:30', 1357, 'YD2207061500040', '2022-07-06 15:24:10')" +
                        "(8, '鲁NY2667', 116.824919102912, 38.521017036966, '河北省沧州市青县白庄子村,西方向,279.0米', '2022-07-06 15:36:14', 77.00, '357', '河北省', '沧州市', '青县', 300522.60, '0', NULL, 214.90, 84.20, '2022-07-06 16:48:05', 1357, 'YD2207061500040', '2022-07-06 15:36:32')\n" +
                        "\t(9, '鲁NY2667', 116.807292224038, 38.584221395533, '河北省沧州市青县西环北路附近城里住宅小区,西方向,125.6米', '2022-07-06 15:41:33', 81.00, '4', '河北省', '沧州市', '青县', 300530.10, '0', NULL, 222.20, 77.00, '2022-07-06 16:47:57', 1357, 'YD2207061500040', '2022-07-06 15:41:56');";*//*
                String sql = "INSERT INTO demo.`bus_gis_loc_vehicle` (`locvehicle_id`, `vehicle_number`, `lng`, `lat`, `address`, `loc_time`, `spd`, `drc`, `province`, `city`, `country`, `mil`, `offline`, `offline_time`, `run_distance`, `remain_distance`, `estimate_time`, `ref_waybill_id`, `ref_waybill_no`, `create_time`) VALUES\n" +
                        "\t(6, '鲁NY2667', 116.778720706128, 38.385518212107, '河北省沧州市沧县朔黄铁路桥,东北方向,665.7米', '2022-07-03 15:23:53', 77.00, '21', '河北省', '沧州市', '沧县', 300505.90, '0', NULL, 198.90, 100.20, '2022-07-06 16:46:30', 1357, 'YD2207061500040', '2022-07-06 15:24:10');" +
                        "INSERT INTO demo.`bus_gis_loc_vehicle` (`locvehicle_id`, `vehicle_number`, `lng`, `lat`, `address`, `loc_time`, `spd`, `drc`, `province`, `city`, `country`, `mil`, `offline`, `offline_time`, `run_distance`, `remain_distance`, `estimate_time`, `ref_waybill_id`, `ref_waybill_no`, `create_time`) VALUES\n" +
                "\t(66, '鲁NY2667', 116.778720706128, 38.385518212107, '河北省沧州市沧县朔黄铁路桥,东北方向,665.7米', '2022-07-03 15:23:53', 77.00, '21', '河北省', '沧州市', '沧县', 300505.90, '0', NULL, 198.90, 100.20, '2022-07-06 16:46:30', 1357, 'YD2207061500040', '2022-07-06 15:24:10');";
                stmt.execute(sql);
            }
        }*/
        /*DataSource ds = new SimpleDataSource("jdbc:mysql://192.168.0.66:3306/shangma_sys_test", "root", "289862d2-d782-4020-96b1-605d2837cfab");
        Connection conn = null;
        try {
            conn = ds.getConnection();
            List<Entity> entityList = SqlExecutor.query(conn, "select * from bus_gis_loc_vehicle limit 1", new EntityListHandler());
            Console.log("{}", entityList);
        } catch (SQLException e) {
            Console.log(e, "SQL error!");
        } finally {
            DbUtil.close(conn);
        }*/
    }
}
