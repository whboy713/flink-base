package com.cowcow.flink.chapter03.source;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class Flink01_FromCollectionSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Arrays 数组工具类
        List<WaterSensor> waterSensorsList1 = Arrays.asList(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 1),
                new WaterSensor("s1", 3L, 1)
        );

        ArrayList<WaterSensor> waterSensorsList12 = new ArrayList<>();
        waterSensorsList12.add(new WaterSensor("s1", 1L, 1));
        waterSensorsList12.add(new WaterSensor("s1", 2L, 1));
        waterSensorsList12.add(new WaterSensor("s1", 3L, 1));

        // 从元素读
        DataStreamSink<Integer> print1 = env.fromElements(1, 2, 33)
                .print();
        log.info("print1:{}", print1);

        // 从集合读
        DataStreamSink<WaterSensor> print2 = env.fromCollection(waterSensorsList1)
                .print()
                .name("print1");
        log.info("print2:{}", print2);
        DataStreamSink<WaterSensor> print3 = env
                .fromCollection(waterSensorsList12)
                .print();
        log.info("print3:{}", print3);

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
