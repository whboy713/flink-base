package com.cowcow.flinksql.chapter01.table;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_CreateTableTableAPI {
    public static void main(String[] args) throws Exception {
        // 1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建表source
        final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("ts", DataTypes.BIGINT())
                        .column("vc", DataTypes.INT())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .option("fields.id.kind", "random")
                .option("fields.id.min", "1")
                .option("fields.id.max", "100")
                .option("fields.ts.kind", "sequence")
                .option("fields.ts.start", "1")
                .option("fields.ts.end", "1000000")
                .option("fields.vc.kind", "random")
                .option("fields.vc.min", "1")
                .option("fields.vc.max", "10000")
                .build();
        tableEnv.createTemporaryTable("source", sourceDescriptor);

        // 创建表对象sourceTable
        Table sourceTable = tableEnv.from("source");

        // 查询
        Table table = sourceTable.
                where($("id").isGreater(5))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sumVC"))
                .select($("id"), $("sumVC"));
        // 执行并打印表中的数据
        table.execute().print();

        env.execute();

    }
}
