package com.cowcow.flink.chapter02.env;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnviromentRemoteDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createRemoteEnvironment("hadoop102", 1234, "patn/Jar");
        Configuration conf = new Configuration();
        env.execute();
    }
}
