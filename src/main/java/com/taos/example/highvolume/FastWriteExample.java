package com.taos.example.highvolume;

import cn.hutool.core.lang.Console;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class FastWriteExample {
    final static Logger logger = LoggerFactory.getLogger(FastWriteExample.class);

    final static int taskQueueCapacity = 1000000;
    final static List<BlockingQueue<String>> taskQueues = new ArrayList<>();
    final static List<ReadTask> readTasks = new ArrayList<>();
    final static List<WriteTask> writeTasks = new ArrayList<>();
    final static DataBaseMonitor databaseMonitor = new DataBaseMonitor();

    public static void stopAll() {
        Console.log("shutting down");
        readTasks.forEach(task -> task.stop());
        writeTasks.forEach(task -> task.stop());
        databaseMonitor.close();
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        int readTaskCount = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        int writeTaskCount = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        int tableCount = args.length > 2 ? Integer.parseInt(args[2]) : 10;
        int maxBatchSize = args.length > 3 ? Integer.parseInt(args[3]) : 3000;

        Console.log("readTaskCount={}, writeTaskCount={} tableCount={} maxBatchSize={}",
                readTaskCount, writeTaskCount, tableCount, maxBatchSize);

        databaseMonitor.init();

        // Create task queues, whiting tasks and start writing threads.
        for (int i = 0; i < writeTaskCount; ++i) {
            BlockingQueue<String> queue = new ArrayBlockingQueue<>(taskQueueCapacity);
            taskQueues.add(queue);
            WriteTask task = new WriteTask(queue, maxBatchSize);
            Thread t = new Thread(task);
            t.setName("WriteThread-" + i);
            t.start();
        }

        // create reading tasks and start reading threads
        int tableCountPerTask = tableCount / readTaskCount;
        /*for (int i = 0; i < readTaskCount; ++i) {
            ReadTask task = new ReadTask(i, taskQueues, 1);
            Thread t = new Thread(task);
            t.setName("ReadThread-" + i);
            t.start();
        }*/
        ReadTask task = new ReadTask(0, taskQueues, -1500000);
        Thread t = new Thread(task);
        t.setName("ReadThread-" + 0);
        t.start();
        ReadTask task2 = new ReadTask(1, taskQueues, 1500000);
        Thread t2 = new Thread(task2);
        t2.setName("ReadThread-" + 1);
        t2.start();

        Runtime.getRuntime().addShutdownHook(new Thread(FastWriteExample::stopAll));

        long lastCount = 0;
        while (true) {
            Thread.sleep(10000);
            //long numberOfTable = databaseMonitor.getTableCount();
            /*long count = databaseMonitor.count();
            Console.log("numberOfTable={} count={} speed={}", 1, count, (count - lastCount) / 10);
            lastCount = count;*/
            Console.log("ReadTask队列：{}",taskQueues.stream().map(queue -> queue.size()).reduce(0,Integer::sum));
        }
    }
}