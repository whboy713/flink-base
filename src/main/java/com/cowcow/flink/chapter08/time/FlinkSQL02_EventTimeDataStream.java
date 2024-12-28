package com.cowcow.flink.chapter08.time;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import static org.apache.flink.table.api.Expressions.*;

import java.time.Duration;

/**
 * 基于事件时间使用时间属性
 * 将dataStream转换成表的时候指定事件时间
 */
public class FlinkSQL02_EventTimeDataStream {
    public static void main(String[] args) throws Exception {
        //todo 1）构建flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        //todo 2）设置并行度
        env.setParallelism(1);

        String filePath = "./input/order.csv";
        DataStreamSource<String> inputDataStream = env.readTextFile(filePath);
        //inputDataStream.print();

        //todo 5）将读取的字符串数据转换成pojo
        SingleOutputStreamOperator<OrderBean> orderDataStream = inputDataStream
                .map(new MapFunction<String, OrderBean>() {
                    @Override
                    public OrderBean map(String value) throws Exception {
                        String[] dataArray = value.split(",");
                        return new OrderBean(dataArray[0], Long.parseLong(dataArray[1]), Double.parseDouble(dataArray[2]), dataArray[3]);
                    }
                });

        SingleOutputStreamOperator<OrderBean> waterMarkStream = orderDataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderBean>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new MyTimeAssiger()));

        //todo 6）将dataStream转换成table对象

        // 替换现有字段
        Schema schema1 = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("money", DataTypes.DOUBLE())
                .column("category", DataTypes.STRING())
                .columnByExpression("timestamp", "$rowtime")
                .build();
        Table table = tabEnv.fromDataStream(waterMarkStream, schema1);

        //作为新字段追加到schema
        Table table2 = tabEnv.fromDataStream(waterMarkStream, $("id"), $("timestamp"),
                $("money"), $("category"), $("rt").rowtime()
        );

        //todo 7）将表转换成table对象
        tabEnv.toAppendStream(table, Row.class).print();
        tabEnv.toAppendStream(table2, Row.class).print();

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderBean {
        private String id;
        private Long timestamp;
        private Double money;
        private String category;
    }

    /**
     * 自定义指定事件时间字段
     */
    private static class MyTimeAssiger implements SerializableTimestampAssigner<OrderBean> {
        @Override
        public long extractTimestamp(OrderBean orderBean, long l) {
            return orderBean.getTimestamp() * 1000L;
        }
    }
}
