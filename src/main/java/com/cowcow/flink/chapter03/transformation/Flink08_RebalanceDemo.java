package com.cowcow.flink.chapter03.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_RebalanceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);
        // rebalance轮询：nextChannelToSendTo = (nextChannelToSendTo + 1) % 下游算子并行度
        // 如果是 数据源倾斜的场景， source后，调用rebalance，就可以解决 数据源的 数据倾斜
        socketDS.rebalance().print();

        env.execute();
    }
}
