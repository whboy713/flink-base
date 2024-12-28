package com.cowcow.flink.chapter03.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
@Slf4j
public class Flink02_SinkFileForBulkFormatDemo3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 必须开启checkpoint，否则一直都是 .inprogress
        env.enableCheckpointing(2000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 读取文本文件
        DataStream<String> text = env.readTextFile("input/words2.txt");

        DataStream<Tuple2<LongWritable, Text>> input = text.map(new MapFunction<String, Tuple2<LongWritable, Text>>() {
            @Override
            public Tuple2<LongWritable, Text> map(String value) throws Exception {
                String[] parts = value.split(" ");
                if (parts.length < 2) {
                    throw new IllegalArgumentException("Invalid line format: " + value);
                }
                LongWritable key = new LongWritable(Long.parseLong(parts[0]));
                Text textValue = new Text(parts[1]);
                return new Tuple2<>(key, textValue);
            }
        });
        input.print();

        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
        final FileSink<Tuple2<LongWritable, Text>> sink = FileSink
                .forBulkFormat(
                        new Path("output"),
                        new SequenceFileWriterFactory<>(hadoopConf, LongWritable.class, Text.class))
                .build();

        input.sinkTo(sink);

        env.execute();
    }
}
