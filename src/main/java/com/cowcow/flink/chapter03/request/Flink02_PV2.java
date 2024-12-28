package com.cowcow.flink.chapter03.request;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink02_PV2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("input/UserBehavior.csv"))
                .build();
        DataStreamSource<String> filesource = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource");

        SingleOutputStreamOperator<UserBehavior> mapDS = filesource.map(
                line -> {
                    // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                });

        //过滤出pv行为
        SingleOutputStreamOperator<UserBehavior> filterDS = mapDS
                .filter(behavior ->
                        "pv".equals(behavior.getBehavior())
                );


        filterDS.keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    long count = 0;
                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        count++;
                        out.collect(count);
                    }
                })
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
