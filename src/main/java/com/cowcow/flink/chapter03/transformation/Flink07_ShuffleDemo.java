package com.cowcow.flink.chapter03.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_ShuffleDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketDS = env
                .socketTextStream("hadoop102", 7777);
        // shuffle随机分区: random.nextInt(下游算子并行度)
        socketDS.shuffle().print();

        env.execute();
    }

}
