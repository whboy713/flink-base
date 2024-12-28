package com.cowcow.flink.chapter03.udf;

import com.cowcow.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_UDFDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> waterSensorDS = env
                .fromElements(
                        new WaterSensor("Sensor1", 1L, 1),
                        new WaterSensor("Sensor1", 2L, 2),
                        new WaterSensor("Sensor1", 3L, 3),
                        new WaterSensor("Sensor2", 4L, 1)
                );
        SingleOutputStreamOperator<WaterSensor> filterDS = waterSensorDS
                .filter(new FilterFunctionImpl());

        filterDS.print();
        env.execute();
    }

    public static class FilterFunctionImpl implements FilterFunction<WaterSensor> {
        @Override
        public boolean filter(WaterSensor value) throws Exception {
            return "Sensor1".equals(value.getId());
        }
    }
}
