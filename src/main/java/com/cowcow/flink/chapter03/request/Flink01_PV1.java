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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_PV1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据，使用新的Source架构
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("input/UserBehavior.csv"))
                .build();
        DataStreamSource<String> filesource = env
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource");

        // 将每行数据映射到UserBehavior对象上
        SingleOutputStreamOperator<UserBehavior> mapDS = filesource
                .map(line -> {
                    // 数据解析并封装到UserBehavior对象
                    String[] split = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                });

        // 过滤出PV行为的数据
        SingleOutputStreamOperator<UserBehavior> filterDS = mapDS
                .filter(behavior ->
                        "pv".equals(behavior.getBehavior())
                );

        // 将PV行为转换为Tuple类型，以便进行求和计算
        filterDS.map(behavior -> Tuple2.of("pv", 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value -> value.f0)  // 根据行为类型("pv")进行分组
                .sum(1) // 对计数字段进行求和
                .print(); // 打印结果

        env.execute(); // 执行计算
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

