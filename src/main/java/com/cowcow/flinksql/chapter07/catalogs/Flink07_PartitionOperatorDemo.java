package com.cowcow.flinksql.chapter07.catalogs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Flink07_PartitionOperatorDemo {
    private static final Logger logger = LoggerFactory.getLogger(Flink07_PartitionOperatorDemo.class);

    public static void main(String[] args) throws DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException, DatabaseNotEmptyException, TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, PartitionNotExistException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.setProperty("HADOOP_USER_NAME", "wolffy");

        String catalogName = "my_hive";
        String databaseName = "test_database1";
        String tableName = "test";

        HiveCatalog catalog = new HiveCatalog(
                catalogName,
                "default",
                "src/main/resources"
        );

        //todo 3）注册目录
        System.out.println("===========注册目录==================");
        tableEnv.registerCatalog(catalogName, catalog);

        System.out.println("---------------创建数据库------------------------");
        catalog.createDatabase(databaseName,
                new CatalogDatabaseImpl(new HashMap<>(), "my comment"), true);
        //todo 4）切换数据库
        System.out.println("===========切换数据库==================");
        tableEnv.useDatabase(databaseName);

        // 创建分区
        // 定义分区键和值
        Map<String, String> partitionSpecMap = new HashMap<>();
        partitionSpecMap.put("year", "2023");
        partitionSpecMap.put("month", "10");
        CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(partitionSpecMap);

        // 定义分区的元数据
        Map<String, String> partitionProperties = new HashMap<>();
        partitionProperties.put("key1", "value1");
        partitionProperties.put("key2", "value2");

        CatalogPartitionImpl partition = new CatalogPartitionImpl(partitionProperties, "This is a partition for year 2023 and month 10");

        // 创建分区
        catalog.createPartition(
                new ObjectPath("mydb", "mytable"),
                partitionSpec,
                partition,
                false
        );

        // 删除分区
        catalog.dropPartition(new ObjectPath(
                "mydb", "mytable"),
                partitionSpec,
                false);

        // 修改分区
        catalog.alterPartition(
                new ObjectPath("mydb", "mytable"),
                partitionSpec,
                partition,
                false
        );

        // 获得分区
        catalog.getPartition(
                new ObjectPath("mydb", "mytable"),
                partitionSpec
        );

        // 检查分区是否存在
        catalog.partitionExists(
                new ObjectPath("mydb", "mytable"),
                partitionSpec
        );

        // 返回表所有的分区
        catalog.listPartitions(
                new ObjectPath("mydb", "mytable")
        );

        // 列出给定分区规范下表的分区
        catalog.listPartitions(
                new ObjectPath("mydb", "mytable"),
                partitionSpec
        );

        // 按表达式筛选器列出表的分区
        // 使用 Arrays.asList 将这些对象组合成一个列表
        List<CatalogPartitionSpec> partitionSpecs = Arrays.asList((CatalogPartitionSpec) partitionSpecMap);

        // 列出分区
        catalog.listPartitions(
                new ObjectPath("mydb", "mytable"),
                (CatalogPartitionSpec) partitionSpecs
        );


    }

}

