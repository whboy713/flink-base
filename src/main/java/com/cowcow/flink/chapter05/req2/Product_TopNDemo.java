package com.cowcow.flink.chapter05.req2;

import com.cowcow.flink.chapter05.req2.bean.HotItem;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

public class Product_TopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建WatermarkStrategy
        WatermarkStrategy<UserBehavior> wms = WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(
                Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp() * 1000L;
            }
        });

        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3],
                            Long.valueOf(split[4]));
                })
                .assignTimestampsAndWatermarks(wms)  // 添加Watermark
                .filter(data -> "pv".equals(data.getBehavior())) // 过滤出来点击数据
                .keyBy(UserBehavior::getItemId) // 按照产品id进行分组
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))) // 设置数据的窗口范围
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(UserBehavior value, Long accumulator) {
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
                }, new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                    @Override
                    public void process(Long key,
                                        Context context,
                                        Iterable<Long> elements,
                                        Collector<HotItem> out) throws Exception {
                        out.collect(new HotItem(key, elements.iterator().next(), context.window().getEnd()));
                    }
                })
                .keyBy(HotItem::getWindowEndTime) // 需要统计窗口内的名次, 则需要把属于同一窗内的元素放在一起
                .process(new KeyedProcessFunction<Long, HotItem, String>() {

                    private ListState<HotItem> hotItems;
                    private ValueState<Long> triggerTS;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hotItems = getRuntimeContext()
                                .getListState(new ListStateDescriptor<HotItem>("hotItems", HotItem.class));
                        triggerTS = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTS", Long.class));
                    }

                    @Override
                    public void processElement(HotItem value, Context ctx, Collector<String> out) throws Exception {
                        hotItems.add(value);
                        if (triggerTS.value() == null) {
                            ctx.timerService().registerEventTimeTimer(value.getWindowEndTime() + 1L);
                            triggerTS.update(value.getWindowEndTime());
                        }
                    }

                    // 等属于某个窗口的所有商品信息来了之后再开始计算topN
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        Iterable<HotItem> hotItems = this.hotItems.get();

                        // 存储最终的结果
                        ArrayList<HotItem> result = new ArrayList<>();
                        for (HotItem hotItem : hotItems) {
                            result.add(hotItem);
                        }
                        this.hotItems.clear();
                        triggerTS.clear();

                        // 对result 排序取前3
                        result.sort((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue());


                        StringBuilder sb = new StringBuilder();
                        sb.append("窗口结束时间: " + (timestamp - 1) + "\n");
                        sb.append("---------------------------------\n");
                        for (int i = 0; i < 3; i++) {
                            sb.append(result.get(i) + "\n");
                        }
                        sb.append("---------------------------------\n\n");
                        out.collect(sb.toString());
                    }

                }).setParallelism(1)
                .print();

        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserBehavior {
        private Long userId;
        private Long itemId;
        private Integer categoryId;
        private String behavior;
        private Long timestamp;
    }
}
