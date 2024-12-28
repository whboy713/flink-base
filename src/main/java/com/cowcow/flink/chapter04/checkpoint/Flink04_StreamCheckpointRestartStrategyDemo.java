package com.cowcow.flink.chapter04.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * 读取服务器node01中端口7777的内容,并切分单词,统计数量
 * 开启checkpoint支持,每隔5s钟写入HDFS一次
 */
public class Flink04_StreamCheckpointRestartStrategyDemo {
    public static void main(String[] args) throws Exception {
        // 1.初始化flink流处理的运行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.开启checkpoint
        // 周期性的生成barrier栅栏,默认情况下checkpoint是没有开启的
        env.enableCheckpointing(5000L);
        // 设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置同一个时间只能有一个栅栏在运行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置checkpoint的执行模式，最多执行一次或者至少执行一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // Checkpointing最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 指定checkpoint的存储位置
        // 方式一
        if (args.length < 1) {
            // Linux：file:///Users/whboy/learn/temp/checkpoint
            // windows：file:///D:/temp/checkpoint
//            env.setStateBackend(new RocksDBStateBackend("file:///Users/whboy/learn/temp/checkpoint"));
            env.setStateBackend(new EmbeddedRocksDBStateBackend());
            env.getCheckpointConfig().setCheckpointStorage("file:///Users/whboy/learn/temp/checkpoint");
        } else {
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(args[0]);
        }

        // 取消作业的时候，上一次成功checkpoint的结果，被删除了！意味着不能将上次执行累加的结果无法恢复，因此我们希望取消作业的时候，不要删除已经checkpoit成功的历史数据
        // ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION: 当作业被取消时，保留外部的checkpoint。注意，在此情况下，您必须手动清理checkpoint状态。
        // ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 当作业被取消时，删除外部化的checkpoint。只有当作业失败时，检查点状态才可用。
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies
                .fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));


                // 3.指定数据源
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //4.对字符串进行空格拆分，然后每个单词记一次数
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //5.对每个单词进行分组聚合操作
        SingleOutputStreamOperator<Tuple2<String, Long>> sumed = wordAndOne.keyBy(0).sum(1);

        //6.打印测试
        sumed.print();

        //7.执行任务
        env.execute();


    }
}
