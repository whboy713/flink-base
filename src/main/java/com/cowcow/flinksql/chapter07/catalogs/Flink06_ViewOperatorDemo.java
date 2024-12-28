package com.cowcow.flinksql.chapter07.catalogs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogViewImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Flink06_ViewOperatorDemo {
    private static final Logger logger = LoggerFactory.getLogger(Flink06_ViewOperatorDemo.class);
    public static void main(String[] args) throws DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException, DatabaseNotEmptyException, TableNotExistException {
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


        // 创建视图
        String viewName = "myview";
        String query = "SELECT * FROM my_table";
        TableSchema schema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();
        String viewComment = "This is a view for demonstration purposes";
        Map<String, String> viewProperties = new HashMap<>();
        catalog.createTable(
                new ObjectPath("mydb", viewName),
                new CatalogViewImpl(viewName,query , schema, viewProperties,viewComment),
                true);
        logger.info("ok");

        // 删除视图
        catalog.dropTable(
                new ObjectPath("mydb", viewName),
                true);

        // 修改视图
        catalog.alterTable(
                new ObjectPath("mydb", "mytable"),
                new CatalogViewImpl(viewName,query , schema, viewProperties,viewComment),
                true);

        // 重命名视图
        catalog.renameTable(
                new ObjectPath("mydb", "myview"),
                "my_new_view",
                true);

        // 获得视图
        catalog.getTable(ObjectPath.fromString("myview"));

        // 检查视图是否存在
        catalog.tableExists(ObjectPath.fromString("mytable"));

        // 获得数据库所有视图
        catalog.listViews("mydb");

    }

}

