package com.cowcow.flink.chapter03.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_FromKafkaSourceDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092") // 指定kafka节点的地址和端口
                .setTopics("first") // 指定消费的 Topic
                .setGroupId("first-1-group") // 指定消费者组的id
                .setStartingOffsets(OffsetsInitializer.earliest()) // flink消费kafka的策略
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 指定反序列化器，这个是反序列化 value
                .build();

        DataStreamSource<String> kafkaDS =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");

        kafkaDS.print("kafkaDS");
        env.execute();
    }
}
