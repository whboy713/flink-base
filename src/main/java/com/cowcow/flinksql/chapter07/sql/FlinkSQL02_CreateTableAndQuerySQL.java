package com.cowcow.flinksql.chapter07.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL02_CreateTableAndQuerySQL {
    public static void main(String[] args) {

        // TODO 1.创建表环境
        // 1.2 写法二
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.创建表
        tableEnv.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");\n");


        tableEnv.executeSql("CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    sumVC INT \n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");\n");

        // TODO 3.执行查询
        DataStreamSource<String> textStreamDS = env.socketTextStream("localhost", 7777);
        // 把table对象，注册成表名
        tableEnv.createTemporaryView("tmp", textStreamDS);
        Table table1 = tableEnv.sqlQuery("select * from tmp where id > 7");



        // TODO 4.输出表
        // 4.1 sql用法
//        tableEnv.executeSql("insert into sink select * from tmp");
        // 4.2 tableapi用法
        table1.executeInsert("sink");
    }
}
