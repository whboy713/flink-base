package com.cowcow.flink.chapter03.request;



import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink03_UV {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("input/UserBehavior.csv"))
                .build();
        SingleOutputStreamOperator<UserBehavior> UserBehaviorDS = env
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource")
                .map(line -> {
                    // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                });
        // 过滤出pv行为
        SingleOutputStreamOperator<UserBehavior> filterDS = UserBehaviorDS
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior())
                );

        // 使用Tuple类型, 方便后面求和
        filterDS.map(behavior -> Tuple2.of("uv", behavior.getUserId()))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value -> value.f0)  // keyBy: 按照key分组
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
                    HashSet<Long> userIds = new HashSet<>();
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                        userIds.add(value.f1);
                        out.collect(userIds.size());
                    }
                })
                .print("uv:");

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
