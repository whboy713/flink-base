package com.cowcow.flink.chapter04.time.watermark;


import com.cowcow.flink.pojo.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class Flink03_WatermarkCustomPeriodicDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 默认周期 200ms
        env.getConfig().setAutoWatermarkInterval(2000);

        SingleOutputStreamOperator<WaterSensor> soketTextDS = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                });
        soketTextDS.assignTimestampsAndWatermarks(WatermarkStrategy
                        // 1.自定义的 周期性生成
                        .<WaterSensor>forGenerator(
                                ctx -> new MyPeriodWatermarkGenerator<>(3000L)
                        )
                        .withTimestampAssigner(
                                (element, recordTimestamp) -> {
                                    return element.getTs() * 1000L;
                                }
                        )
                )
                .keyBy(sensor -> sensor.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

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

    public static class MyPeriodWatermarkGenerator<T> implements WatermarkGenerator<T> {

        // 乱序等待时间
        private long delayTs;
        // 用来保存 当前为止 最大的事件时间
        private long maxTs;

        public MyPeriodWatermarkGenerator(long delayTs) {
            this.delayTs = delayTs;
            this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
        }

        /**
         * 每条数据来，都会调用一次： 用来提取最大的事件时间，保存下来
         *
         * @param event
         * @param eventTimestamp 提取到的数据的 事件时间
         * @param output
         */
        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(maxTs, eventTimestamp);
            System.out.println("调用onEvent方法，获取目前为止的最大时间戳=" + maxTs);
        }

        /**
         * 周期性调用： 发射 watermark
         *
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTs - delayTs - 1));
            System.out.println("调用onPeriodicEmit方法，生成watermark=" + (maxTs - delayTs - 1));
        }
    }

}
