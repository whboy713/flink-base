package com.cowcow.flink.chapter03.transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink17_CoMapDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> intDS1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> intDS2 = env.fromElements(6, 7, 8, 9, 10);

        ConnectedStreams<Integer, Integer> conCS = intDS1.connect(intDS2);

        SingleOutputStreamOperator<Integer> mapDS = conCS
                .map(new CoMapFunction<Integer, Integer, Integer>() {
                    @Override
                    public Integer map1(Integer value) throws Exception {
                        return value + 1;
                    }

                    @Override
                    public Integer map2(Integer value) throws Exception {
                        return value + 2;
                    }
                });
        mapDS.print();


        env.execute();
    }
}
