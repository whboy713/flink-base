package com.cowcow.flink.chapter03.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_MapTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> waterSensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        // TODO map算子： 一进一出，类型可能会变化
        // TODO 方式一： 匿名实现类
        SingleOutputStreamOperator<String> mapDS1 = waterSensorDS
                .map(new MapFunction<WaterSensor, String>() {
                    @Override
                    public String map(WaterSensor value) throws Exception {
                        return value.getId();
                    }
                });
        // TODO 方式二： lambda表达式
        SingleOutputStreamOperator<String> mapDS2 = waterSensorDS
                .map(WaterSensor::getId);

        // TODO 方式三： 定义一个类来实现 MapFunction
        SingleOutputStreamOperator<String> mapDS3 = waterSensorDS
                .map(new MyMapFunction());

        mapDS1.print();
        env.execute();
    }

    public static class MyMapFunction implements MapFunction<WaterSensor, String> {

        @Override
        public String map(WaterSensor value) throws Exception {
            return value.getId();
        }
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
