package com.cowcow.flink.chapter03.transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class Flink18_CoFlatMapDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> intDS1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> intDS2 = env.fromElements(6, 7, 8, 9, 10);

        ConnectedStreams<Integer, Integer> conCS = intDS1.connect(intDS2);

        SingleOutputStreamOperator<Integer> flatMapDS = conCS
                .flatMap(new CoFlatMapFunction<Integer, Integer, Integer>() {
                    @Override
                    public void flatMap1(Integer value, Collector<Integer> out) throws Exception {
                        out.collect(value + 1);
                    }

                    @Override
                    public void flatMap2(Integer value, Collector<Integer> out) throws Exception {
                        out.collect(value + 1);
                    }
                });

        flatMapDS.print();


        env.execute();
    }
}
