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

import java.time.Duration;

/**
 * 多并行度下的水印操作演示
 *
 * 测试数据：
 * 并行度设置为2测试：
 * hadoop,1626934802000 ->2021-07-22 14:20:02
 * hadoop,1626934805000 ->2021-07-22 14:20:05
 * hadoop,1626934806000 ->2021-07-22 14:20:06
 *
 * 结论：两个并行度都满足了窗口触发的条件，则窗口会被触发计算，同时以两个并行度中小的水印为准（对齐）
 * 键值：hadoop，线程号：76，事件时间：【2021-07-22 14:20:02.000】
 * 键值：hadoop，线程号：77，事件时间：【2021-07-22 14:20:05.000】
 * 键值：hadoop，线程号：76，事件时间：【2021-07-22 14:20:06.000】
 * 触发窗口计算结果>>>:2>
 *  键值：【(hadoop)】
 *      触发窗口数据的个数：【1】
 *      触发窗口的数据：2021-07-22 14:20:02.000
 *      窗口计算的开始时间和结束时间：2021-07-22 14:20:00.000----->2021-07-22 14:20:05.000
 *
 *
 * 触发窗口计算的前提：
 * 有几个线程那么需要将这些线程全部的满足窗口触发的条件才会触发窗口计算（窗内必须有数据）
 */
public class Flink05_WatermarkParallelismDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                });
        waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy
                        // 1.1 指定watermark生成：升序的watermark，没有等待时间
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
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
