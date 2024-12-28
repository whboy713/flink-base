package com.cowcow.flink.chapter03.transformation;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink05_SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);
        // 传位置索引的，适用于Tuple类型，POJO不行
        //SingleOutputStreamOperator<WaterSensor> result1 = sensorKS.sum(2);
        SingleOutputStreamOperator<WaterSensor> result2 = sensorKS.sum("vc");
//        result2.print("result2-sum:");

        // max：只会取比较字段的最大值，非比较字段保留第一次的值
        // maxby：取比较字段的最大值，同时非比较字段 取 最大值这条数据的值
        SingleOutputStreamOperator<WaterSensor> result3 = sensorKS.max("vc");
        result3.print("result3-max：");
        SingleOutputStreamOperator<WaterSensor> result4 = sensorKS.min("vc");
        SingleOutputStreamOperator<WaterSensor> result5 = sensorKS.maxBy("vc");
        SingleOutputStreamOperator<WaterSensor> result6 = sensorKS.minBy("vc");

        result3.print();
        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WaterSensor{
        public String id;
        public Long ts;
        public Integer vc;
    }
}
