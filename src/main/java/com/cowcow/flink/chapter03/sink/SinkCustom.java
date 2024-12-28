package com.cowcow.flink.chapter03.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;


public class SinkCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> sensorDS = env.socketTextStream("hadoop102", 7777);

        sensorDS.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<String> {

        Connection conn = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在这里 创建连接
            // conn = new xxxx
        }

        @Override
        public void close() throws Exception {
            super.close();
            // 做一些清理、销毁连接
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            // 写出逻辑 实战案例
            // 这个方法是 来一条数据，调用一次,所以不要在这里创建 连接对象

        }
    }
}
