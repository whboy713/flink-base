package com.cowcow.flink.chapter03.udf;

import com.cowcow.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_UDFDemo {
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
                .filter(new FilterFunction<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "Sensor1".equals(waterSensor.getId());
                    }
                });

        filterDS.print();
        env.execute();
    }
}
