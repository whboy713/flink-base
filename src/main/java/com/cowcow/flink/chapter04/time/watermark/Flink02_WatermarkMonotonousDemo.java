package com.cowcow.flink.chapter04.time.watermark;

import com.cowcow.flink.pojo.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class Flink02_WatermarkMonotonousDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                });
        waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy
                        // 1.1 指定watermark生成：升序的watermark，没有等待时间
                        .<WaterSensor>forMonotonousTimestamps()
                        // 1.2 指定 时间戳分配器，从数据中提取
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                // 返回的时间戳，要 毫秒
                                System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                                return element.getTs() * 1000L;
                            }
                        })
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                             @Override
                             public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                 long startTs = context.window().getStart();
                                 long endTs = context.window().getEnd();
                                 String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                 String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                                 long count = elements.spliterator().estimateSize();

                                 out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                             }
                         }
                )
                .print();

        env.execute();
    }
}
