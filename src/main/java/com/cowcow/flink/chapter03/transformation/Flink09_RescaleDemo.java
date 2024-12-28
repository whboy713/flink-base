package com.cowcow.flink.chapter03.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink09_RescaleDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);
        //rescale缩放： 实现轮询， 局部组队，比rebalance更高效
        socketDS.rescale().print();

        env.execute();
    }
}
