package com.cowcow.flink.chapter05.req2;

import com.cowcow.flink.chapter05.req2.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class Flink05_Project_AdsClick {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 创建WatermarkStrategy
        WatermarkStrategy<AdsClickLog> wms = WatermarkStrategy
                .<AdsClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog>() {
                    @Override
                    public long extractTimestamp(AdsClickLog element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        env
                .readTextFile("input/AdClickLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new AdsClickLog(Long.valueOf(datas[0]),
                            Long.valueOf(datas[1]),
                            datas[2],
                            datas[3],
                            Long.valueOf(datas[4]));
                })
                .assignTimestampsAndWatermarks(wms)
                // 安装 (省份, 广告) 分组
                .keyBy(new KeySelector<AdsClickLog, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(AdsClickLog log) throws Exception {
                        return Tuple2.of(log.getProvince(), log.getAdsId());
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(10)))
                .allowedLateness(Time.seconds(10))
                .sideOutputLateData(new OutputTag<AdsClickLog>("ads_late") {
                })
                .aggregate(new AggregateFunction<AdsClickLog, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(AdsClickLog value, Long accumulator) {
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                }, new ProcessWindowFunction<Long, Tuple4<String, Long, Long, Long>, Tuple2<String, Long>, TimeWindow>() {
                    @Override
                    public void process(Tuple2<String, Long> key, Context ctx, Iterable<Long> elements, Collector<Tuple4<String, Long, Long, Long>> out) throws Exception {
                        out.collect(Tuple4.of(key.f0, key.f1, elements.iterator().next(), ctx.window().getEnd()));
                    }
                })
                .keyBy(t -> t.f3)
                .process(new KeyedProcessFunction<Long, Tuple4<String, Long, Long, Long>, String>() {

                    private ValueState<Long> windowEnd;
                    private ListState<Tuple4<String, Long, Long, Long>> datas;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        datas = getRuntimeContext()
                                .getListState(new ListStateDescriptor<Tuple4<String, Long, Long, Long>>("datas", TypeInformation.of(new TypeHint<Tuple4<String, Long, Long, Long>>() {
                                })));
                        windowEnd = getRuntimeContext().getState(new ValueStateDescriptor<Long>("windowEnd", Long.class));
                    }

                    @Override
                    public void processElement(Tuple4<String, Long, Long, Long> value, Context ctx, Collector<String> out) throws Exception {
                        // 存数据
                        datas.add(value);
                        // 注册定时器
                        if (windowEnd.value() == null) {
                            ctx.timerService().registerEventTimeTimer(value.f3 + 10L);
                            windowEnd.update(value.f3);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        ArrayList<Tuple4<String, Long, Long, Long>> result = new ArrayList<>();
                        for (Tuple4<String, Long, Long, Long> t : datas.get()) {
                            result.add(t);
                        }
                        // 清空状态
                        windowEnd.clear();
                        datas.clear();
                        // 排序, 取top3
                        result.sort(new Comparator<Tuple4<String, Long, Long, Long>>() {
                            @Override
                            public int compare(Tuple4<String, Long, Long, Long> o1, Tuple4<String, Long, Long, Long> o2) {
                                return (int) (o2.f2 - o1.f2);
                            }
                        });

                        // 返回的数据
                        StringBuilder sb = new StringBuilder();
                        sb.append("窗口结束时间: ").append(timestamp - 10).append("\n");
                        sb.append("---------------------------------\n");
                        for (int i = 0; i < Math.min(3, result.size()); i++) {
                            sb.append(result.get(i)).append("\n");
                        }
                        sb.append("---------------------------------\n\n");
                        out.collect(sb.toString());
                    }

                })
                .print();

        env.execute();
    }
}
