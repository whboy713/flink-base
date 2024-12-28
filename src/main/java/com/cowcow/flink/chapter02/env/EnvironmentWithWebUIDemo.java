package com.cowcow.flink.chapter02.env;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvironmentWithWebUIDemo {
    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.execute();
    }
}
