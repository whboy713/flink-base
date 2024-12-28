package com.cowcow.flink.chapter03.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

@Slf4j
public class Flink06_FromRabbitMQSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setPort(5672) // 默认RabbitMQ端口是5672
                .setVirtualHost("/") // 默认虚拟主机是"/"
                .setUserName("guest") // 默认用户名是"guest"
                .setPassword("guest") // 默认密码是"guest"
                .build();

        final DataStream<String> rabbitMQDS = env
                .addSource(new RMQSource<String>(
                        connectionConfig,
                        "queueName",
                        true,
                        new SimpleStringSchema()))
                .setParallelism(1);

        rabbitMQDS.print("rabbitMQDS");
        env.execute();
    }
}

