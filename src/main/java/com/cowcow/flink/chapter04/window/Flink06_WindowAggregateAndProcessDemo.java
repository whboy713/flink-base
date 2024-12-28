package com.cowcow.flink.chapter04.window;

import com.cowcow.flink.pojo.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink06_WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 从指定IP和端口读取传感器数据，转换为WaterSensor对象
        env
                .socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                })
                // 按传感器ID进行分组，并使用滚动处理时间窗口（每10秒）
                .keyBy(sensor -> sensor.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new MyAgg(),
                        new MyProcess()
                )
                .print();
        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor, Integer, String> {

        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器");
            return 0;
        }


        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            System.out.println("调用add方法,value=" + value);
            return accumulator + value.getVc();
        }

        @Override
        public String getResult(Integer accumulator) {
            System.out.println("调用getResult方法");
            return accumulator.toString();
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            System.out.println("调用merge方法");
            return null;
        }
    }

    // 全窗口函数的输入类型 = 增量聚合函数的输出类型
    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            long startTs = context.window().getStart();
            long endTs = context.window().getEnd();
            String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

            long count = elements.spliterator().estimateSize();

            out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());

        }
    }
}
