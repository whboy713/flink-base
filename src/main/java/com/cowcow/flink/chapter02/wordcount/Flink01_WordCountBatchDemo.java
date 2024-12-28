package com.cowcow.flink.chapter02.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class Flink01_WordCountBatchDemo {

    public static void main(String[] args) throws Exception {

        //  1.创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //  2.从文件中读取数据：input/word.txt 按行读取(存储的元素就是每行的文本)
        DataSource<String> lineDS = env.readTextFile("input/words.txt");

        //  3.转换数据格式：切分、转换
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // TODO 3.1 按照 空格 切分单词
                        String[] words = value.split(" ");
                        // TODO 3.2 将 单词 转换为 （word，1）
                        for (String word : words) {
                            Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                            //TODO 3.3 使用 Collector 向下游发送数据
                            out.collect(wordTuple2);
                        }
                    }
                });

        //  4.按照 word 进行分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupby = wordAndOne
                .groupBy(0);//0是位置，表示第1个元素分组
        //  5.分组内聚合统计
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupby
                .sum(1);// 1是位置，表示第二个元素聚合统计
        //  6.打印结果
        sum.print();

    }
}
