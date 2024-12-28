package com.cowcow.flink.chapter09.hbase;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLHBaseExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1、创建HBase source table 使用SQL
        String createSourceTableSql = "CREATE TEMPORARY TABLE students_text (" +
                "  id STRING," +
                "  name STRING," +
                "  age INT," +
                "  sex STRING," +
                "  clazz STRING" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'path/to/students_text.csv'," +
                "  'format' = 'csv'," +
                "  'csv.field-delimiter' = ','" +
                ")";
        tEnv.executeSql(createSourceTableSql);

        // 2、创建HBase sink table 使用SQL
        String createSinkTableSql = "CREATE TEMPORARY TABLE students_hbase (" +
                "  id STRING," +
                "  info ROW<name STRING, age INT, sex STRING, clazz STRING>," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'hbase-2.2'," +
                "  'table-name' = 'students_flink'," +
                "  'zookeeper.quorum' = 'hadoop102:2181,hadoop102:2181,hadoop102:2181'" +
                ")";
        tEnv.executeSql(createSinkTableSql);

        // 3、插入数据到HBase
        String insertSql = "INSERT INTO students_hbase " +
                "SELECT " +
                "  id," +
                "  ROW(name, age, sex, clazz) AS info " +
                "FROM " +
                "  students_text " +
                "WHERE " +
                "  clazz IS NOT NULL";
        tEnv.executeSql(insertSql);

        // 4、查询HBase表
        String selectSql = "SELECT id, info.age FROM students_hbase";
        tEnv.executeSql(selectSql).print();
    }
}

