package com.cowcow.flink.chapter03.transformation;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink14_SideOutputDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> sentorDS = socketDS
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                });

        OutputTag<WaterSensor> outputTag1 = new OutputTag<WaterSensor>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> outputTag2 = new OutputTag<WaterSensor>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> processDS = sentorDS
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {

                        if ("s1".equals(waterSensor.getId())) {
                            ctx.output(outputTag1, waterSensor);
                        } else if ("s2".equals(waterSensor.getId())) {
                            ctx.output(outputTag2, waterSensor);
                        } else {
                            // 非s1、s2的数据，放到主流中
                            out.collect(waterSensor);
                        }
                    }
                });
        SideOutputDataStream<WaterSensor> sideOutput1 = processDS.getSideOutput(outputTag1);
        SideOutputDataStream<WaterSensor> sideOutput2 = processDS.getSideOutput(outputTag2);


        // 打印主流
        processDS.print("主流-非s1、s2");

        //打印 侧输出流
        sideOutput1.printToErr("s1");
        sideOutput2.printToErr("s2");

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
