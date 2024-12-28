package com.cowcow.flink.chapter09.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * #查看topic列表
 * bin/kafka-topics.sh --bootstrap-server hadoop102:9092 --list
 *
 * #创建输入与输出topic
 * bin/kafka-topics.sh --bootstrap-server hadoop102:9092 --create --topic clicklog_input --replication-factor 3 --partitions 3
 * bin/kafka-topics.sh --bootstrap-server hadoop102:9092 --create --topic clicklog_output --replication-factor 3 --partitions 3
 * #打开Kafka输入Topic生产者
 * bin/kafka-console-producer.sh --bootstrap-server hadoop102:9092 --topic clicklog_input
 * {"user":"Mary","url":"./home","cTime":"2025-02-02 12:00:00"}
 * #Kafka 输出topic打开消费者
 * bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic clicklog_output
 */
public class Flink01_TableAPI_Kafka2Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //2、创建kafka source table
        final Schema schema = Schema.newBuilder()
                .column("user", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                .column("cTime", DataTypes.STRING())
                .build();
        tEnv.createTemporaryTable("sourceTable", TableDescriptor.forConnector("kafka")
                .schema(schema)
                .format("json")
                .option("topic","clicklog_input")
                .option("properties.bootstrap.servers","hadoop102:9092")
                .option("properties.group.id","clicklog")//每次都从最早的offsets开始
                .option("scan.startup.mode","latest-offset")
                .build());

        //3、创建 kafka sink table
        tEnv.createTemporaryTable("sinkTable", TableDescriptor.forConnector("kafka")
                .schema(schema)
                .format("csv")
                .option("topic","clicklog_output")
                .option("properties.bootstrap.servers","hadoop102:9092")
                .build());

        //5、输出(包括执行,不需要单独在调用tEnv.execute("job"))
        tEnv.from("sourceTable")
                .select($("user"), $("url"),$("cTime"))
                .executeInsert("sinkTable");
    }
}
