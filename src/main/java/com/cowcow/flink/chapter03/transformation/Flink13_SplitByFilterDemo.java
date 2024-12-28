package com.cowcow.flink.chapter03.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink13_SplitByFilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<String> even = socketDS
                .filter(value -> Integer.parseInt(value) % 2 == 0);
        SingleOutputStreamOperator<String> odd = socketDS
                .filter(value -> Integer.parseInt(value) % 2 == 1);

        even.print("偶数流");
        odd.print("奇数流");

        env.execute();
    }
}
