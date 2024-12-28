package com.cowcow.flink.chapter03.request;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class Flink07_OrderRealTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 1. 读取Order流
        FileSource<String> fileSource1 = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("input/OrderLog.csv"))
                .build();
        SingleOutputStreamOperator<OrderEvent> orderLogSource = env.fromSource(fileSource1, WatermarkStrategy.noWatermarks(), "orderLogSource")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.valueOf(datas[3]));

                });

        // 2. 读取交易流
        FileSource<String> fileSource2 = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("input/ReceiptLog.csv"))
                .build();
        SingleOutputStreamOperator<TxEvent> txLogSource = env.fromSource(fileSource2, WatermarkStrategy.noWatermarks(), "txLogSource")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new TxEvent(datas[0], datas[1], Long.valueOf(datas[2]));
                });

        // 3. 两个流连接在一起
        ConnectedStreams<OrderEvent, TxEvent> orderAndTx = orderLogSource.connect(txLogSource);

        // 4. 因为不同的数据流到达的先后顺序不一致，所以需要匹配对账信息，输出表示对账成功与否
        orderAndTx
                .keyBy("txId", "txId")
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    // 存 txId -> OrderEvent
                    final Map<String, OrderEvent> orderMap = new HashMap<>();
                    // 存储 txId -> TxEvent
                    final Map<String, TxEvent> txMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent orderEvent, Context ctx, Collector<String> out) throws Exception {
                        // 获取交易信息
                        if (txMap.containsKey(orderEvent.getTxId())) {
                            out.collect("订单: " + orderEvent + " 对账成功");
                            txMap.remove(orderEvent.getTxId());
                        } else {
                            orderMap.put(orderEvent.getTxId(), orderEvent);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent txEvent, Context ctx, Collector<String> out) throws Exception {
                        // 获取订单信息
                        if (orderMap.containsKey(txEvent.getTxId())) {
                            OrderEvent orderEvent = orderMap.get(txEvent.getTxId());
                            out.collect("订单: " + orderEvent + " 对账成功");
                            orderMap.remove(txEvent.getTxId());
                        } else {
                            txMap.put(txEvent.getTxId(), txEvent);
                        }
                    }
                })
                .print();
        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TxEvent {
        private String txId;// 支付ID
        private String payChannel;
        private Long eventTime;

    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderEvent {
        private Long orderId;
        private String eventType;
        private String txId;
        private Long eventTime;


    }
}
