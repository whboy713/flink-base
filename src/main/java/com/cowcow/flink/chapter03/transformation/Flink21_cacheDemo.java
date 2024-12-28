package com.cowcow.flink.chapter03.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink21_cacheDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 2. 添加数据源
        DataStream<String> sourceStream =
                env.fromElements("apple", "banana", "cherry", "date", "elderberry");

        // 3. 对数据进行处理
        SingleOutputStreamOperator<String> processedStream =
                sourceStream.map(String::toUpperCase);

        // 4. 缓存处理后的数据流
        DataStream<String> cachedStream = processedStream.cache();

        // 5. 在多个操作中重用缓存的数据流
        DataStream<String> output1 = cachedStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "Output 1: " + value;
            }
        });

        DataStream<String> output2 = cachedStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "Output 2: " + value;
            }
        });

        // 6. 打印结果
        output1.print("Output 1");
        output2.print("Output 2");

        // 7. 执行作业
        env.execute("Flink Cache Example");
    }
}
