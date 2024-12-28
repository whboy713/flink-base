package com.cowcow.flink.chapter03.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class Flink05_FromDataGenSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // DataGeneratorSource(GeneratorFunction<Long, OUT> generatorFunction, long count, RateLimiterStrategy rateLimiterStrategy, TypeInformation<OUT> typeInfo)
        // DataGeneratorSource(GeneratorFunction<Long, OUT> generatorFunction, long count, TypeInformation<OUT> typeInfo)
        // GeneratorFunction :GeneratorFunction接口，需要实现， 输入类型固定是Long
        // count : long类型,自动生成的数字序列
        // rateLimiterStrategy: 限速策略,比如每秒生成几条数据
        // typeInfo: 返回的类型
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
                            @Override
                            public String map(Long value) throws Exception {
                                return "Number:" + value;
                            }
                        },
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(100),
                        Types.STRING
                );

        DataStreamSource<String> DataGenDS = env.fromSource(
                dataGeneratorSource,
                WatermarkStrategy.noWatermarks(),
                "generatorSource"
        );
        DataGenDS.print();

        env.execute();
    }
}

