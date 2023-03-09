package com.taos.example.highvolume;

import cn.hutool.core.convert.Convert;

import java.sql.*;

/**
 * Prepare target database.
 * Count total records in database periodically so that we can estimate the writing speed.
 */
public class DataBaseMonitor {
    private Connection conn;
    private Statement stmt;

    public DataBaseMonitor init() throws SQLException {
        if (conn == null) {
            String jdbcURL = "jdbc:TAOS-RS://192.168.0.66:6041/shangma_sys?user=root&password=taosdata";
            conn = DriverManager.getConnection(jdbcURL);
            stmt = conn.createStatement();
        }
        return this;
    }

    public void close() {
        try {
            stmt.close();
        } catch (SQLException e) {
        }
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }

    public void prepareDatabase() throws SQLException {
        stmt.execute("DROP DATABASE IF EXISTS test");
        stmt.execute("CREATE DATABASE test");
        stmt.execute("CREATE STABLE test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
    }

    public Long count() throws SQLException {
        if (!stmt.isClosed()) {
            ResultSet result = stmt.executeQuery("SELECT count(*) from demo.bus_gis_loc_vehicle");
            result.next();
            return result.getLong(1);
        }
        return null;
    }

    /**
     * show test.stables;
     *
     *               name              |      created_time       | columns |  tags  |   tables    |
     * ============================================================================================
     *  meters                         | 2022-07-20 08:39:30.902 |       4 |      2 |      620000 |
     */
    public  Long getTableCount() throws SQLException {
        if (!stmt.isClosed()) {
            ResultSet result = stmt.executeQuery("show test.tables");
            result.next();
            return Convert.toLong(result.getString(1).substring(4,5));
        }
        return null;
    }
}