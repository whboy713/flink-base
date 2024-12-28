package com.cowcow.flinksql.chapter07.catalogs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink01_SQLDDLDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.setProperty("HADOOP_USER_NAME", "wolffy");

        String catalogName = "my_hive";
        String databaseName = "test_database1";

        HiveCatalog catalog = new HiveCatalog(
                catalogName,
                "default",
                "src/main/resources"
        );

        //todo 3）注册目录
        System.out.println("===========注册目录==================");
        tableEnv.registerCatalog(catalogName, catalog);

        //todo 4）切换目录
        System.out.println("===========切换目录==================");
        tableEnv.useCatalog(catalogName);

        //todo 5）创建数据库
        System.out.println("===========创建数据库==================");
        String createDBSql = "CREATE DATABASE IF NOT EXISTS "+catalogName+"."+databaseName;
        tableEnv.executeSql(createDBSql);

        //todo 6）切换数据库
        System.out.println("===========切换数据库==================");
        tableEnv.useDatabase(databaseName);

        //todo 7）创建表
        System.out.println("===========创建表==================");
        String createTableSql = "CREATE TABLE IF NOT EXISTS my_table4 (\n" +
                "  name STRING,\n" +
                "  age INT\n" +
                ") WITH (\n" +
                "  'connector' = 'hive',\n" +
                "  'format' = 'orc',\n" + // 或者 'format' = 'parquet'
                "  'hive-conf-dir' = '/opt/module/hive/conf'\n" +
                ")";
        tableEnv.executeSql(createTableSql);

        //todo 8）查询所有的表
        System.out.println("===========创建表==================");
        tableEnv.listTables();

    }
}
