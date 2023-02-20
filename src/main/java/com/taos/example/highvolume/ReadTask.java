package com.taos.example.highvolume;

import cn.hutool.core.collection.LineIter;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.lang.Console;
import cn.hutool.core.math.MathUtil;
import cn.hutool.core.text.StrBuilder;
import cn.hutool.core.text.StrFormatter;
import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.PageUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.simple.SimpleDataSource;
import cn.hutool.db.handler.EntityListHandler;
import cn.hutool.db.handler.NumberHandler;
import cn.hutool.db.sql.SqlExecutor;
import cn.hutool.json.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

class ReadTask implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ReadTask.class);
    private final int taskId;
    private final List<BlockingQueue<String>> taskQueues;
    private final int queueCount;
    private final int tableCount;
    private boolean active = true;

    public ReadTask(int readTaskId, List<BlockingQueue<String>> queues, int tableCount) {
        this.taskId = readTaskId;
        this.taskQueues = queues;
        this.queueCount = queues.size();
        this.tableCount = tableCount;
    }

    /**
     * Assign data received to different queues.
     * Here we use the suffix number in table name.
     * You are expected to define your own rule in practice.
     *
     * @param line record received
     * @return which queue to use
     */
    public int getQueueId(String line) {
        String tbName = line.substring(0, line.indexOf(',')); // For example: tb1_101
        String suffixNumber = tbName.split("_")[1];
        return Integer.parseInt(suffixNumber) % this.queueCount;
    }

    @Override
    public void run() {
        Console.log("started");
       // Iterator<String> it = new MockDataSource("tb" + this.taskId, tableCount);
        DataSource ds = new SimpleDataSource("jdbc:mysql://127.0.0.1:3306/shangma_sys", "root", "289862d2-d782-4020-96b1-605d2837cfab");
        Connection conn = null;

        try {
            /*while (it.hasNext() && active) {
                String line = it.next();
                int queueId = getQueueId(line);
                taskQueues.get(queueId).put(line);
            }*/

            conn = ds.getConnection();
            Number count = SqlExecutor.query(conn, "select count(*) from bus_gis_loc_vehicle", new NumberHandler());
            Integer pageSize = 1000;
            int queueId=0;
            Date now = DateUtil.beginOfYear(new Date());
            for(int i=0;i<count.intValue();i+=pageSize) {
                List<Entity> entityList = SqlExecutor.query(conn, StrFormatter.format("select * from bus_gis_loc_vehicle where MOD(locvehicle_id,{})={} limit {},{}",tableCount, taskId,i,pageSize), new EntityListHandler());
                StrBuilder sb = StrBuilder.create();
                if(ObjUtil.isNotEmpty(entityList) && active) {
                    for(Entity entity:entityList) {
                        /*String ts = DateUtil.formatDateTime(DateUtil.offsetSecond(now,entity.getInt("locvehicle_id")));
                        sb.append("(").append(StrFormatter.format("'{}',",ts)).append( entity.values().stream().map(e -> {
                            if(ObjUtil.isNull(e)) {
                                return "NULL";
                            }
                            if(e instanceof String || e instanceof Timestamp) {
                                return "'"+Convert.toStr(e)+"'";
                            }
                            return Convert.toStr(e);
                        }).collect(Collectors.joining(",")))
                        .append(")").append(" ");*/
                        entity.set("_ts",entity.getLong("loc_time")*1000000);
                        entity.remove("locvehicle_id");
                        List<String> lineList = JsonConvertInflux.convert(JSONUtil.toJsonStr(entity), "BUS_GIS_LOC_VEHICLE","ref_waybill_no");
                        //sb.append(lineList.get(0));
                        queueId%=this.queueCount;
                        taskQueues.get(queueId).put(lineList.get(0));
                        queueId++;
                    }
                   /* queueId%=this.queueCount;
                    taskQueues.get(queueId).put(sb.toString());
                    queueId++;*/
                }
            }

        } catch (SQLException e) {
            Console.log(e, "SQL error!");
        }
        catch (Exception e) {
            Console.log("Read Task Error", e);
        }finally {
            DbUtil.close(conn);
        }
    }

    public void stop() {
        Console.log("stop");
        this.active = false;
    }
}