package com.cowcow.flink.chapter09.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


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
public class Flink02_SQLAPI_Kafka2Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2、创建kafka source table 使用SQL
        String createSourceTableSql = "CREATE TEMPORARY TABLE sourceTable (" +
                "  user STRING," +
                "  url STRING," +
                "  cTime STRING" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'clicklog_input'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "  'properties.group.id' = 'clicklog'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'" +
                ")";
        tEnv.executeSql(createSourceTableSql);

        // 3、创建 kafka sink table 使用SQL
        String createSinkTableSql = "CREATE TEMPORARY TABLE sinkTable (" +
                "  user STRING," +
                "  url STRING," +
                "  cTime STRING" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'clicklog_output'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "  'format' = 'csv'" +
                ")";
        tEnv.executeSql(createSinkTableSql);

        // 4、使用SQL API进行查询并插入到sinkTable
        String sqlQuery = "INSERT INTO sinkTable SELECT user, url, cTime FROM sourceTable";
        tEnv.executeSql(sqlQuery);
    }
}
