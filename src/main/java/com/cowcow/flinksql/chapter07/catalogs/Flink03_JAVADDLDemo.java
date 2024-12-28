package com.cowcow.flinksql.chapter07.catalogs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.HashMap;
import java.util.Map;

public class Flink03_JAVADDLDemo {
    public static void main(String[] args) throws DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException {
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

        //todo 4）切换目录
        System.out.println("===========切换目录==================");
        tableEnv.useCatalog(catalogName);

        //todo 5）创建数据库
        System.out.println("===========创建数据库==================");
        catalog.createDatabase(databaseName,
                new CatalogDatabaseImpl(new HashMap<>(), "my comment"),
                true);

        //todo 6）切换数据库
        System.out.println("===========切换数据库==================");
        tableEnv.useDatabase(databaseName);

        //todo 7）创建表
        System.out.println("===========创建表==================");
        // Create a catalog table
        TableSchema schema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();

        Map<String, String> properties = new HashMap<>();
        /**
         * HiveCatalog可用于处理两种表：Hive 兼容表和通用表。就元数据和存储层中的数据而言，
         * Hive 兼容表是那些以 Hive 兼容方式存储的表。因此，可以从 Hive 端查询通过 Flink 创建的 Hive 兼容表。
         * 另一方面，通用表是 Flink 特有的。使用 来创建通用表时HiveCatalog，我们只是使用 HMS 来持久化元数据。
         * 虽然这些表对 Hive 可见，但 Hive 不太可能理解元数据。因此，在 Hive 中使用此类表会导致未定义的行为。
         * Flink 使用属性“ is_generic ”来判断表是 Hive 兼容的还是通用的。使用 来创建表时 HiveCatalog，默认情况下将其视为通用表。
         * 如果您想创建一个与 Hive 兼容的表，请确保is_generic在您的表属性中设置 为 false。
         * 如上所述，不应在 Hive 中使用通用表。在 Hive CLI 中，您可以调用DESCRIBE FORMATTED一个表并通过检查is_generic属性来决定它是否是通用的。
         * 通用表将具有is_generic=true.
         *
         * HiveCatalog 有两个用途：
         * todo 作为原生 Flink 元数据的持久化存储：通用表
         * todo 以及作为读写现有 Hive 元数据的接口：兼容表
         */
        properties.put("is_generic", String.valueOf(true));
        //properties.put("connector", "COLLECTION");
        catalog.createTable(
                new ObjectPath(databaseName, tableName),
                new CatalogTableImpl(
                        schema,
                        properties,
                        "my comment"),
                true
        );
        System.out.println(tableName);

    }
}
