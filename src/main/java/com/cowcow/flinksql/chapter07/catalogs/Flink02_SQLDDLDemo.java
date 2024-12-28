package com.cowcow.flinksql.chapter07.catalogs;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Flink02_SQLDDLDemo {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);
        System.setProperty("HADOOP_USER_NAME", "wolffy");

        // 配置 Hive 目录
        String hiveCatalogName = "my_hive";
        String defaultDatabase = "test_database2";
        String hiveConfDir = "src/main/resources"; // 替换为实际的 Hive 配置目录路径

        tableEnv.executeSql("CREATE CATALOG " + hiveCatalogName + " WITH (\n" +
                "  'type'='hive',\n" +
                "  'hive-conf-dir'='" + hiveConfDir + "'\n" +
                ")");

        // 使用 Hive 目录作为默认目录
        tableEnv.useCatalog(hiveCatalogName);

        // 切换数据库
        System.out.println("===========切换数据库==================");
        tableEnv.useDatabase(defaultDatabase);

        // 创建表，并明确指定连接器类型
        System.out.println("===========创建表==================");
        String createTableSql = "CREATE TABLE IF NOT EXISTS my_table6 (\n" +
                "  name STRING,\n" +
                "  age INT\n" +
                ") WITH (\n" +
                "  'connector' = 'hive',\n" +
                "  'format' = 'orc',\n" + // 或者 'format' = 'parquet'
                "  'hive-conf-dir' = '/opt/module/hive/conf'\n" +
                ")";
        tableEnv.executeSql(createTableSql);

        // 查询所有的表
        System.out.println("===========查询所有表==================");
        System.out.println(Arrays.toString(tableEnv.listTables()));
    }
}

