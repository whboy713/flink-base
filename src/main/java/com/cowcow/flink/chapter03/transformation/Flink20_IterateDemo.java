package com.cowcow.flink.chapter03.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


public class Flink20_IterateDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 2. 添加自定义数据源
        DataStream<Integer> initialStream = env.addSource(new InitialValueSource());

        // 3. 创建迭代流
        IterativeStream<Integer> iterationStream =
                initialStream.iterate(1000); // 1000是最大迭代时间

        // 4. 在迭代流中进行处理
        DataStream<Integer> iterationBody = iterationStream.map(new IncrementFunction());
//        DataStream<Integer> iterationBody2 = iterationStream.map(value -> value + 1);
        // 5. 定义终止条件
        DataStream<Integer> feedbackStream = iterationBody.filter(value -> value < 10);
        DataStream<Integer> output = iterationStream.closeWith(feedbackStream);

        // 6. 打印结果
        output.print("Iteration Result");

        // 7. 执行作业
        env.execute("Flink Iterate Example");
    }

    // 自定义数据源，生成初始值
    public static class InitialValueSource implements SourceFunction<Integer> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            ctx.collect(0); // 初始值为0
            isRunning = false; // 只发送一次初始值
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // 增加函数，每次迭代将值加1
    public static class IncrementFunction implements MapFunction<Integer, Integer> {
        @Override
        public Integer map(Integer value) throws Exception {
            return value + 1;
        }
    }
}
