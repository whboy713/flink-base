package com.cowcow.flink.chapter04.checkpoint;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * savepoint的目的是为了从上一次保存的中间结果中恢复过来
 * 举例：
 * 在生产环境中运行着一个作业，因为今晚要升级作业，因此需要将生产环境的作业停止掉，将升级后的jar进行部署和发布
 * 希望重新发布以后可以将上一个作业的运行结果恢复后继续运行
 * <p>
 * 所以这时候可以使用savepoint进行解决这个问题问题
 * <p>
 * 面试问题：
 * checkpoint和savepoint的区别？
 * checkpoint：周期性定期运行，生成barrier（栅栏）发送到job作业的每个算子，当算子收到barrier以后会将state的中间计算结果快照存储到分布式文件系统中
 * savepoint：将指定的checkpoint的结果恢复过来，恢复到当前的作业中，继续运行
 * <p>
 * TODO 当作业重新递交的时候，并行度发生了概念，在flink1.10版本中，可以正常的递交作业，且能够恢复历史的累加结果
 * 但是之前版本中一旦发生并行度的变化，作业无法递交
 */
public class Flink05_StreamSavepointDemo {
    public static void main(String[] args) throws Exception {
        //TODO 1）初始化flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 2）开启checkpoint
        // 设置每5秒钟开启一次checkpoint
        env.enableCheckpointing(5000);
        // 设置checkpoint超时时间为60秒
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置同一个时间是否可以有多个checkpoint执行，这里设置为2，即同一时间可以有两个checkpoint执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 设置checkpointing最小时间间隔为500毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置checkpoint的执行模式为EXACTLY_ONCE，确保数据处理的精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保在作业取消时保留外部化的Checkpoint数据，而不是删除它们
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 用于指定在checkpoint发生异常的时候，是否应该fail该task，这里设置为false，即不失败
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
//        env.setDefaultSavepointDirectory("file:///Users/whboy/learn/temp/checkpoint");

        //指定checkpoint的存储位置
        if (args.length < 1) {
            //Linux：file:///Users/whboy/learn/temp/checkpoint
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage("file:///Users/whboy/learn/temp/checkpoint");
        } else {
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(args[0]);
        }

        //指定数据源
        DataStreamSource<String> socketTextStream = env
                .socketTextStream("hadoop102", 7777);

        //对字符串进行空格拆分，然后每个单词记一次数
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = socketTextStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            collector.collect(Tuple2.of(word, 1L));
                        }
                    }
                });

        // 对每个单词进行分组聚合操作
        SingleOutputStreamOperator<Tuple2<String, Long>> sumed = wordAndOne
                .keyBy(0)
                .sum(1);

        // 打印测试
        sumed.print();

        // 执行任务，递交作业
        env.execute();
    }
}