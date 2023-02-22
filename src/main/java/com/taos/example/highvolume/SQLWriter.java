package com.taos.example.highvolume;

import cn.hutool.core.lang.Console;
import cn.hutool.core.util.StrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * A helper class encapsulate the logic of writing using SQL.
 * <p>
 * The main interfaces are two methods:
 * <ol>
 *     <li>{@link SQLWriter#processLine}, which receive raw lines from WriteTask and group them by table names.</li>
 *     <li>{@link  SQLWriter#flush}, which assemble INSERT statement and execute it.</li>
 * </ol>
 * <p>
 * There is a technical skill worth mentioning: we create table as needed when "table does not exist" error occur instead of creating table automatically using syntax "INSET INTO tb USING stb".
 * This ensure that checking table existence is a one-time-only operation.
 * </p>
 *
 * </p>
 */
public class SQLWriter {
    final static Logger logger = LoggerFactory.getLogger(SQLWriter.class);

    private Connection conn;
    private Statement stmt;

    /**
     * current number of buffered records
     */
    private int bufferedCount = 0;
    /**
     * Maximum number of buffered records.
     * Flush action will be triggered if bufferedCount reached this value,
     */
    private int maxBatchSize;


    /**
     * Maximum SQL length.
     */
    private int maxSQLLength;

    /**
     * Map from table name to column values. For example:
     * "tb001" -> "(1648432611249,2.1,114,0.09) (1648432611250,2.2,135,0.2)"
     */
    private Map<String, String> tbValues = new HashMap<>();

    /**
     * Map from table name to tag values in the same order as creating stable.
     * Used for creating table.
     */
    private Map<String, String> tbTags = new HashMap<>();

    public SQLWriter(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }


    /**
     * Get Database Connection
     *
     * @return Connection
     * @throws SQLException
     */
    private static Connection getConnection() throws SQLException {
        String jdbcURL = "jdbc:TAOS://192.168.0.66:6030?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcURL);
    }

    /**
     * Create Connection and Statement
     *
     * @throws SQLException
     */
    public void init() throws SQLException {
        conn = getConnection();
        stmt = conn.createStatement();
        stmt.execute("use shangma_sys");
        ResultSet rs = stmt.executeQuery("show variables");
        while (rs.next()) {
            String configName = rs.getString(1);
            if ("maxSQLLength".equals(configName)) {
                maxSQLLength = Integer.parseInt(rs.getString(2));
                Console.log("maxSQLLength={}", maxSQLLength);
            }
        }
    }

    /**
     * Convert raw data to SQL fragments, group them by table name and cache them in a HashMap.
     * Trigger writing when number of buffered records reached maxBachSize.
     *
     * @param line raw data get from task queue in format: tbName,ts,current,voltage,phase,location,groupId
     */
    public void processLine(String line) throws SQLException {
        /*bufferedCount += 1;
        *//*int firstComma = line.indexOf(',');
        String tbName = line.substring(0, firstComma);
        int lastComma = line.lastIndexOf(',');
        int secondLastComma = line.lastIndexOf(',', lastComma - 1);
        String value = "(" + line.substring(firstComma + 1, secondLastComma) + ") ";*//*
        String tbName = "bus_gis_loc_vehicle";
        String value = line.trim()+" ";
        if (tbValues.containsKey(tbName)) {
            tbValues.put(tbName, tbValues.get(tbName) + value);
        } else {
            tbValues.put(tbName, value);
        }
        *//*if (!tbTags.containsKey(tbName)) {
            String location = line.substring(secondLastComma + 1, lastComma);
            String groupId = line.substring(lastComma + 1);
            String tagValues = "('" + location + "'," + groupId + ')';
            tbTags.put(tbName, tagValues);
        }*//*
        if (bufferedCount == maxBatchSize) {
            flush();
        }*/
        //bufferedCount += 1;
        String tbName = "bus_gis_loc_vehicle";
        if (tbValues.containsKey(tbName)) {
            tbValues.put(tbName, tbValues.get(tbName) + line);
        } else {
            tbValues.put(tbName, line);
        }
        flush();
    }


    /**
     * Assemble INSERT statement using buffered SQL fragments in Map {@link SQLWriter#tbValues} and execute it.
     * In case of "Table does not exit" exception, create all tables in the sql and retry the sql.
     */
    public void flush() throws SQLException {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        for (Map.Entry<String, String> entry : tbValues.entrySet()) {
            String tableName = "shangma_sys.`bus_gis_loc_vehicle`";
            String values = entry.getValue();
            String q = tableName + " values " + values + " ";
            if (sb.length() + q.length() > maxSQLLength) {
                sb.append(q);
                executeSQL(sb.toString());
                //Console.log("increase maxSQLLength or decrease maxBatchSize to gain better performance");
                sb = new StringBuilder("INSERT INTO ");
            }
            sb.append(q);
        }
        executeSQL(sb.toString());
        tbValues.clear();
        bufferedCount = 0;
    }

    public void executeSQL(String sql) throws SQLException {
        try {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            // convert to error code defined in taoserror.h
            /*int errorCode = e.getErrorCode() & 0xffff;
            if (errorCode == 0x362 || errorCode == 0x218 || errorCode==9731) {
                // Table does not exist
                createTables();
                executeSQL(sql);
            } else {
                Console.log("Execute SQL: {}", sql);
                throw e;
            }*/
            Console.log("Execute SQL: {}", sql);
            throw e;
        } catch (Throwable throwable) {
            Console.log("Execute SQL: {}", sql);
            throw throwable;
        }
    }

    /**
     * Create tables in batch using syntax:
     * <p>
     * CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
     * </p>
     */
    private void createTables() throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        for (String tbName : tbValues.keySet()) {
            String tagValues = tbTags.get(tbName);
            sb.append("IF NOT EXISTS ").append(tbName).append(" USING meters TAGS ").append(tagValues).append(" ");
        }
        String sql = sb.toString();
        try {
            stmt.executeUpdate(sql);
        } catch (Throwable throwable) {
            Console.log("Execute SQL: {}", sql);
            throw throwable;
        }
    }

    public boolean hasBufferedValues() {
        return bufferedCount > 0;
    }

    public int getBufferedCount() {
        return bufferedCount;
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
}