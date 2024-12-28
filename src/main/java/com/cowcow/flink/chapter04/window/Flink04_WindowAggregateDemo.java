package com.cowcow.flink.chapter04.window;

import com.cowcow.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Flink04_WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);

        // 从指定IP和端口读取传感器数据，转换为WaterSensor对象
        env
                .socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                // 按传感器ID进行分组，并使用滚动处理时间窗口（每10秒）
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        /**
                         * 第一个类型： 输入数据的类型 WaterSensor
                         * 第二个类型： 累加器的类型，存储的中间计算结果的类型 Integer
                         * 第三个类型： 输出的类型 String
                         */
                        new AggregateFunction<WaterSensor, Integer, String>() {
                            @Override
                            public Integer createAccumulator() {
                                System.out.println("创建累加器");
                                return 0;
                            }

                            // 聚合逻辑
                            @Override
                            public Integer add(WaterSensor value, Integer accumulator) {
                                System.out.println("调用add方法,value=" + value);
                                return accumulator + value.getVc();
                            }

                            // 获取最终结果，窗口触发时输出
                            @Override
                            public String getResult(Integer accumulator) {
                                System.out.println("调用getResult方法");
                                return accumulator.toString();
                            }

                            @Override
                            public Integer merge(Integer a, Integer b) {
                                // 只有会话窗口才会用到
                                System.out.println("调用merge方法");
                                return null;
                            }
                        }
                )
                .print();
        env.execute();
    }
}

