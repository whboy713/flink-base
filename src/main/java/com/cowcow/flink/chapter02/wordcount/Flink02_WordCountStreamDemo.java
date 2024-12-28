package com.cowcow.flink.chapter02.wordcount;

import org.apache.commons.math3.distribution.AbstractMultivariateRealDistribution;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Flink02_WordCountStreamDemo {

    private static final Logger logger = LoggerFactory.getLogger(Flink02_WordCountStreamDemo.class);

    public static void main(String[] args) throws Exception {

        //  1.创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        logger.info("main thread id: >>>>>>>>>>>" + env);

        // 2.从文件中读取数据：input/words.txt 按行读取
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("input/words.txt")
        ).build();
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                             @Override
                             public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                                 String[] words = value.split(" ");
                                 for (String word : words) {
                                     // 转换成 二元组 （word，1）
                                     Tuple2<String, Integer> wordsAndOne = Tuple2.of(word, 1);
                                     // 通过 采集器 向下游发送数据
                                     collector.collect(wordsAndOne);
                                 }
                             }
                         }
                );

        wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print();
        // TODO 5.执行
        env.execute();
    }
}
