package com.cowcow.flink.chapter04.window;

import com.cowcow.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink01_WindowAPIDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<WaterSensor, String> sensorKS = env
                .socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                .keyBy(sensor -> sensor.getId());


        // TODO 1. 指定 窗口分配器： 指定 用 哪一种窗口 ---  时间 or 计数？ 滚动、滑动、会话？
        // 1.1 没有keyby的窗口: 窗口内的所有数据 进入同一个 子任务，并行度只能为1
        //  sensorDS.windowAll()
        // 1.2 有keyby的窗口: 每个key上都定义了一组窗口，各自独立地进行统计计算
        // 1.2.1 基于处理时间
        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10))); // 滚动窗口，窗口长度10s
        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2))); // 滑动窗口，窗口长度10s，滑动步长2s
        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))); // 会话窗口，超时间隔5s

        // 1.2.2 基于事件时间
        sensorKS.window(TumblingEventTimeWindows.of(Time.seconds(10))); // 滚动窗口，窗口长度10s
        sensorKS.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2))); // 滑动窗口，窗口长度10s，滑动步长2s
        sensorKS.window(EventTimeSessionWindows.withGap(Time.seconds(5))); // 会话窗口，超时间隔5s

        // 1.2.3基于计数的
        sensorKS.countWindow(5);  // 滚动窗口，窗口长度=5个元素
        sensorKS.countWindow(5,2); // 滑动窗口，窗口长度=5个元素，滑动步长=2个元素
        sensorKS.window(GlobalWindows.create());  // 全局窗口，计数窗口的底层就是用的这个，需要自定义的时候才会用

        // TODO 2. 指定 窗口函数 ： 窗口内数据的 计算逻辑
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 增量聚合： 来一条数据，计算一条数据，窗口触发的时候输出计算结果
//        sensorWS
//                .reduce()
//        .aggregate(, )

        // 全窗口函数：数据来了不计算，存起来，窗口触发的时候，计算并输出结果
//        sensorWS.process()

        env.execute();
    }
}

