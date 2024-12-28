package com.cowcow.flinksql.chapter07.catalogs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Flink05_TableOperatorDemo {
    public static void main(String[] args) throws DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException, DatabaseNotEmptyException, TableNotExistException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.setProperty("HADOOP_USER_NAME", "wolffy");

        String catalogName = "myhive";
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
        //todo 4）切换目录
        System.out.println("===========切换目录==================");
        tableEnv.useCatalog(catalogName);

//        System.out.println("---------------创建数据库------------------------");
//        catalog.createDatabase(databaseName,
//                new CatalogDatabaseImpl(new HashMap<>(),
//                        "my comment"),
//                true);


        //todo 4）切换数据库
        System.out.println("===========切换数据库==================");
        tableEnv.useDatabase(databaseName);


        TableSchema schema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();
        Map<String, String> properties = new HashMap<>();
        // 创建表
        catalog.createTable(
                new ObjectPath(databaseName, tableName),
                new CatalogTableImpl(schema,properties, "my comment"),
                true
        );

//        // 删除表
//        catalog.dropTable(
//                new ObjectPath(databaseName, tableName),
//                false);

        // 修改表
//        TableSchema schema2 = TableSchema.builder()
//                .field("username", DataTypes.STRING())
//                .field("age", DataTypes.INT())
//                .build();
//        Map<String, String> properties2 = new HashMap<>();
//        catalog.alterTable(
//                new ObjectPath(databaseName, tableName),
//                new CatalogTableImpl(schema2, properties2, "my comment"),
//                true
//        );

        // 重命名表
//        catalog.renameTable(
//                new ObjectPath(databaseName, databaseName),
//                "my_new_table",true);

        // 获得表
//        catalog.getTable(ObjectPath.fromString("mytable"));

        // 判断表是否存在
//        catalog.tableExists(ObjectPath.fromString("mytable"));

        // 返回数据库所有表的列表
        catalog.listTables(databaseName).forEach(System.out::println);

    }

}

