package com.cowcow.flink.chapter05.process;

import com.cowcow.flink.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink05_SideOutPutDemo2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketDS
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");

                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));

                    }
                });

        WatermarkStrategy<WaterSensor> watermarStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                        return element.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark =
                waterSensorDS.assignTimestampsAndWatermarks(watermarStrategy);

        KeyedStream<WaterSensor, String> sensorStringKeyedStream = sensorDSwithWatermark
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor value) throws Exception {
                        return value.getId();
                    }
                });
        OutputTag<WaterSensor> waterSensorOutputTag1 = new OutputTag<>("sensor<10", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> process = sensorStringKeyedStream
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        if (value.getVc() > 10) {
                            out.collect(value);
                        } else {
                            ctx.output(waterSensorOutputTag1, value);
                        }
                    }
                });
        SideOutputDataStream<WaterSensor> sideOutput = process
                .getSideOutput(waterSensorOutputTag1);
        sideOutput.printToErr("1111");
        process.print("主流");


        env.execute();
    }
}
