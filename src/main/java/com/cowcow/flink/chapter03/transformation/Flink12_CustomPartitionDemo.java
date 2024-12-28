package com.cowcow.flink.chapter03.transformation;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink12_CustomPartitionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        DataStream<String> partitionCustomDS =
                socketDS.partitionCustom(new MyPartitioner(), value -> value);
        partitionCustomDS.print();

        env.execute();
    }

    public static class MyPartitioner implements Partitioner<String> {
        public int partition(String key, int numPartitions) {
            return key.length() % numPartitions;
        }
    }


}



