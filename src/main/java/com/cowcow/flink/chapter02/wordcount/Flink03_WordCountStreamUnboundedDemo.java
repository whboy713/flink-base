package com.cowcow.flink.chapter02.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink03_WordCountStreamUnboundedDemo {

    public static void main(String[] args) throws Exception {

        // TODO 1.创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        // TODO 2.从socket中读取数据
        DataStreamSource<String> socketDS = env
                .socketTextStream("hadoop102", 7777);

        // TODO 3.处理数据: 切分、转换、分组、聚合
        // TODO 3.1 切分、转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = socketDS
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                            throws Exception {
                        // 按照 空格 切分
                        String[] words = value.split(" ");
                        for (String word : words) {
                            // 转换成 二元组 （word，1）
                            Tuple2<String, Integer> wordsAndOne = Tuple2.of(word, 1);
                            // 通过 采集器 向下游发送数据
                            out.collect(wordsAndOne);
                        }
                    }
                });

        // TODO 3.2 分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value)
                            throws Exception {
                        return value.f0;
                    }
                });

        // TODO 3.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS
                .sum(1);

        // TODO 4.输出数据
        sumDS.print();

        // TODO 5.执行
        env.execute();
    }
}
