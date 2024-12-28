package com.cowcow.flink.chapter08.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSql01_EventTimeTableDDL {
    public static void main(String[] args) throws Exception {
        //todo 1）构建flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        //todo 2）设置并行度
        env.setParallelism(1);

        //todo 4 创建表的sql语句
        String filePath = "./input/order.csv";
        String sqlDDL = "CREATE TABLE InputTable (\n" +
                "  `id` varchar,\n" +
                "  `timestamp` bigint,\n" +
                "  `money` double,\n" +
                "  `category` varchar,\n" +
                "  `rt` AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),\n" +
                "  watermark for rt as rt - interval '1' second\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = '"+filePath+"',\n" +
                "  'format' = 'csv'\n" +
                ")";

        //todo 5）执行创建表的操作
        tabEnv.executeSql(sqlDDL);

        //todo 6）打印表的结构信息
        Table table = tabEnv.sqlQuery("select * from InputTable");
        table.printSchema();

        //todo 7)启动运行
        tabEnv.toAppendStream(table, Row.class).print();

        env.execute();
    }
}
