package com.cowcow.flink.chapter08.temporal;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 使用时态表函数计算订单金额（批的方式实现）
 * 需要两个流数据：
 * 1）订单流
 * 2）汇率流
 * 其中汇率流的数据使用时态表函数进行关联
 */
public class TemporalTablesFunctionBatch {
    public static void main(String[] args) throws Exception {
        //todo 1）构建flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        //todo 2）设置并行度
        env.setParallelism(1);


        //todo 4）构建数据源
        //4.1）构建订单数据源
        List<Tuple3<Double, String, Long>> orderList = new ArrayList<>();
        orderList.add(new Tuple3<>(7D, "Euro", 2L));    //欧元
        orderList.add(new Tuple3<>(7D, "US Dollar", 3L)); //美元
        orderList.add(new Tuple3<>(0.05D, "Yen", 4L)); //人民币
        orderList.add(new Tuple3<>(8D, "Euro", 5L));//欧元
        SingleOutputStreamOperator<Tuple3<Double, String, Long>> orderStream = env.fromCollection(orderList)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Double, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Double, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<Double, String, Long> element, long l) {
                                return element.f2 * 1000L;
                            }
                        }));

        //4.2）构建汇率数据源
        List<Tuple3<String, Integer, Long>> rateList = new ArrayList<>();
        rateList.add(new Tuple3<>("US Dollar", 102, 1L));
        rateList.add(new Tuple3<>("Euro", 114, 1L));
        rateList.add(new Tuple3<>("Yen", 1, 1L));
        rateList.add(new Tuple3<>("Euro", 116, 5L));
        rateList.add(new Tuple3<>("Euro", 117, 7L));
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> rateStream = env.fromCollection(rateList)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, Integer, Long> element, long l) {
                                return element.f2 * 1000L;
                            }
                        }));

        //todo 5）将dataStream转换成Table对象
        Table orderTable = tabEnv.fromDataStream(orderStream, $("amount"), $("currency"), $("rowtime").rowtime());
        Table rateTable = tabEnv.fromDataStream(rateStream, $("currency"), $("rate"), $("rowtime").rowtime());

        //todo 6）将表对象注册成表
        tabEnv.createTemporaryView("Orders", orderTable);
        tabEnv.createTemporaryView("RatesHistory", rateTable);

        //todo 7）将汇率表定义为时态表函数
        TemporalTableFunction temporalTableFunction = rateTable.createTemporalTableFunction($("rowtime"), $("currency"));
        tabEnv.createTemporaryFunction("Rates", temporalTableFunction);

        //todo 8）关联查询
        Table result = tabEnv.sqlQuery("SELECT o.currency, o.amount, r.rate,\n" +
                "  o.amount * r.rate AS yen_amount\n" +
                " FROM\n" +
                "  Orders AS o,\n" +
                "  LATERAL TABLE (Rates(o.rowtime)) AS r\n" +
                " WHERE r.currency = o.currency");

        //todo 9)查询打印
        tabEnv.toAppendStream(result, Row.class).printToErr();

        env.execute();


    }
}
