package com.cowcow.flink.chapter04.state;


import com.cowcow.flink.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * TODO 计算每种传感器的水位和
 *
 * @author cjp
 * @version 1.0
 */
public class Flink04_KeyedReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(
                        new MapFunction<String, WaterSensor>() {
                            @Override
                            public WaterSensor map(String value) throws Exception {
                                String[] datas = value.split(",");
                                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        sensorDS.keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            ReducingState<Integer> vcSumReducingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcSumReducingState = getRuntimeContext().getReducingState(
                                        new ReducingStateDescriptor<Integer>(
                                                "vcSumReducingState",
                                                new ReduceFunction<Integer>() {
                                                    @Override
                                                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                                                        return value1 + value2;
                                                    }
                                                },
                                                Types.INT
                                        )
                                );
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 来一条数据，添加到 reducing状态里
                                vcSumReducingState.add(value.getVc());
                                Integer vcSum = vcSumReducingState.get();
                                out.collect("传感器id为" + value.getId() + ",水位值总和=" + vcSum);


//                                vcSumReducingState.get();   // 对本组的Reducing状态，获取结果
//                                vcSumReducingState.add();   // 对本组的Reducing状态，添加数据
//                                vcSumReducingState.clear(); // 对本组的Reducing状态，清空数据
                            }
                        }
                )
                .print();

        env.execute();
    }
}
