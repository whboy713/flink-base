package com.cowcow.flinksql.chapter07.catalogs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Flink08_FunctionOperatorDemo {
    private static final Logger logger = LoggerFactory.getLogger(Flink08_FunctionOperatorDemo.class);

    public static void main(String[] args) throws DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException, DatabaseNotEmptyException, TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, PartitionNotExistException, FunctionNotExistException, FunctionAlreadyExistException {
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

        // 创建函数
        // 创建函数
        System.out.println("---------------创建函数------------------------");
        String functionName = "myfunc";
        String className = "com.cowcow.flink.chapter07.catalogs.MyCustomFunction";
        Map<String, String> functionProperties = new HashMap<>();
        functionProperties.put("key1", "value1");
        functionProperties.put("key2", "value2");

        CatalogFunctionImpl function = new CatalogFunctionImpl(className);

        // 创建函数
        catalog.createFunction(
                new ObjectPath(databaseName, functionName),
                function,
                false
        );

        // 删除函数
        catalog.dropFunction(new ObjectPath("mydb", "myfunc"),
                false);

        // 修改函数
        catalog.alterFunction(
                new ObjectPath("mydb", "myfunc"),
                function,
                false);

        // 获得函数
        catalog.getFunction(ObjectPath.fromString("myfunc"));

        // 检查函数是否存在
        catalog.functionExists(ObjectPath.fromString("myfunc"));

        // 列出数据库中的函数
        catalog.listFunctions("mydb");


    }

}

