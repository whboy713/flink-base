package com.cowcow.flink.chapter03.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_ADSClickLog {

    public static void main(String[] args) throws Exception {

        // 543462, 1715,     beijing, beijing,1511658000
        // 用户ID、 广告ID、   省份、     城市和     时间戳组
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        // 1.读取数据 ADSClickLogSource
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("input/AdClickLog.csv"))
                .build();
        SingleOutputStreamOperator<AdsClickLog> AdsClickLogMapDSe = env
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "ADSClickLogSource")
                .map(new MapFunction<String, AdsClickLog>() {
                    @Override
                    public AdsClickLog map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new AdsClickLog(Long.valueOf(datas[0]), Long.valueOf(datas[1]),
                                datas[2],
                                datas[3],
                                Long.valueOf(datas[4]));
                    }
                });

        // AdsClickLog--> Tuple2<Tuple2<String, Long>, Long>
        KeyedStream<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>> tuple2Tuple2KeyedStream = AdsClickLogMapDSe
                .map(new MapFunction<AdsClickLog, Tuple2<Tuple2<String, Long>, Long>>() {
                    @Override
                    public Tuple2<Tuple2<String, Long>, Long> map(AdsClickLog value) throws Exception {
                        return Tuple2.of(Tuple2.of(value.getProvince(), value.getAdId()), 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                        return value.f0;
                    }
                });
        tuple2Tuple2KeyedStream
                .sum(1)
                .print();
        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AdsClickLog {
        private Long userId;
        private Long adId;
        private String province;
        private String city;
        private Long timestamp;

    }
}
