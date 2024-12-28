package com.cowcow.flink.chapter08.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class GroupWindowsSqlCumulateExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String filePath = GroupWindowsSqlCumulateExample.class.getClassLoader().getResource("bid.csv").getPath();

        // 作为事件时间的字段必须是 timestamp 类型, 所以根据 long 类型的 ts 计算出来一个 t
        tEnv.executeSql("create table Bid(" +
                "bidtime TIMESTAMP(3)," +
                "price DECIMAL(10, 2), " +
                "item string," +
                "watermark for bidtime as bidtime - interval '1' second) " +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'file:///"+filePath+"',"
                + "'format' = 'csv'"
                + ")");
//        tEnv
//                .sqlQuery(
//                        "SELECT window_start, window_end, SUM(price) as sum_price\n" +
//                                "  FROM TABLE(\n" +
//                                "    CUMULATE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '2' MINUTES, INTERVAL '10' MINUTES))\n" +
//                                "  GROUP BY window_start, window_end"
//                )
//                .execute()
//                .print();

        tEnv
                .sqlQuery(
                        "SELECT window_start, window_end, SUM(price)\n" +
                                "  FROM TABLE(\n" +
                                "    CUMULATE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '2' MINUTES, INTERVAL '1' DAY))\n" +
                                "  GROUP BY window_start, window_end"
                )
                .execute()
                .print();
    }
}
