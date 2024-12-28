package com.cowcow.flinksql.chapter07.catalogs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.HashMap;
import java.util.Map;

public class Flink04_DatabaseOperatorDemo {
    public static void main(String[] args) throws DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException, DatabaseNotEmptyException {
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

        System.out.println("---------------删除数据库------------------------");
        catalog.dropDatabase(databaseName, true, true);

//        System.out.println("---------------修改数据库------------------------");
//        catalog.alterDatabase(databaseName, new CatalogDatabaseImpl(...), false);

        System.out.println("---------------删除数据库------------------------");
        //CatalogDatabase database = catalog.getDatabase(databaseName);

        System.out.println("---------------验证数据库是否存在------------------------");
        boolean result = catalog.databaseExists(databaseName);
        System.out.println("---------------"+result+"------------------------");

        System.out.println("---------------在目录中列出数据库------------------------");
        catalog.listDatabases();

    }
}
