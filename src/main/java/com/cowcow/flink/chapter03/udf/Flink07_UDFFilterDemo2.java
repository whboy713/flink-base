package com.cowcow.flink.chapter03.udf;

import com.cowcow.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_UDFFilterDemo2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> waterSentorDS = env
                .fromElements(
                        new WaterSensor("s1", 1L, 1),
                        new WaterSensor("s2", 2L, 2),
                        new WaterSensor("s3", 3L, 3)
                );
        SingleOutputStreamOperator<WaterSensor> filterDS = waterSentorDS.filter(new MyFilterFunction("s1"));

        filterDS.print();

        env.execute();
    }
    public static class MyFilterFunction implements FilterFunction<WaterSensor> {

        private String id;

        public MyFilterFunction(String id) {
            this.id = id;
        }

        @Override
        public boolean filter(WaterSensor value) throws Exception {
            return id.equals(value.getId());
        }
    }
}
