package com.cowcow.flink.chapter03.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
@Slf4j
public class Flink03_FromSocketSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env
                .socketTextStream("hadoop102", 7777);
        log.info("socketDS=" + socketDS);
        socketDS.print();
        env.execute();
    }
}
