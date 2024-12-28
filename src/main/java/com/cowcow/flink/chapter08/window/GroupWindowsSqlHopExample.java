package com.cowcow.flink.chapter08.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * flinksql的窗口分为两类：
 * 1）Group window
 *  1.1：滚动窗口（支持tvfs（表值函数））
 *  1.2：滑动窗口（支持tvfs（表值函数））
 *  1.3：会话窗口（flink1.14才会支持）
 *  1.4：累计窗口（flink1.13刚支持）
 * 2）over window
 *
 * 表值函数：
 *  window_start
 *  window_end
 *  window_time
 */
public class GroupWindowsSqlHopExample {
    public static void main(String[] args) throws Exception {


        //todo 3）构建flink的表的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        String filePath = GroupWindowsSqlHopExample.class.getClassLoader().getResource("bid.csv").getPath();
        tabEnv.executeSql("create table Bid(" +
                "bidtime TIMESTAMP(3)," +
                "price DECIMAL(10, 2), " +
                "item string," +
                "watermark for bidtime as bidtime - interval '1' second) " +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'file:///"+filePath+"',"
                + "'format' = 'csv'"
                + ")");

        Table table = tabEnv.sqlQuery("" +
                "select window_start,window_end,sum(price) as sum_price " +
                " from table(" +
                "  hop(table Bid, DESCRIPTOR(bidtime), interval '5' MINUTES, interval '10' MINUTES))" +
                "  group by window_start,window_end");

        tabEnv.toAppendStream(table, Row.class).print();
        env.execute();

    }
}
