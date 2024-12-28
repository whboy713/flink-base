package com.cowcow.flink.chapter03.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink22_ChainDemo {

    public static void main(String[] args) {
        // 设置执行环境, 类似spark中初始化sparkContext一样
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 关闭操作链..
        env.disableOperatorChaining();
        final String[] WORDS = new String[] {
                "hello",
                "flink",
                "spark",
                "hbase"
        };
        DataStreamSource<String> dataStreamSource = env.fromElements(WORDS);

        SingleOutputStreamOperator<String> pairStream = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.err.println(value + " ---- 1");
                return value + "->1";
            }
        }).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.err.println(value + " ---- 2");
                return value + "->2";
            }
        }).slotSharingGroup("chain1");

        // 还可以控制更细粒度的任务链,比如指明从哪个operator开始形成一条新的链
        // someStream.map(...).startNewChain()，但不能使用someStream.startNewChain()。
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
