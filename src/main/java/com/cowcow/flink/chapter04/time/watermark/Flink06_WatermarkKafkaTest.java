package com.cowcow.flink.chapter04.time.watermark;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 使用水印消费kafka里面的数据
 */
public class Flink06_WatermarkKafkaTest {
    public static void main(String[] args) throws Exception {
        //todo 1）初始化flink流处理环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);//设置webui的端口号
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(2);
        env.enableCheckpointing(5000);
        //todo 2）接入数据源
        //指定topic的名称
        String topicName = "test3";
        //实例化kafkaConsumer对象
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test001");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");
        props.setProperty("flink.partition-discovery.interval-millis", "5000");//开启一个后台线程每隔5s检测一次kafka的分区情况

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), props);
        kafkaSource.setCommitOffsetsOnCheckpoints(true);//todo 在开启checkpoint以后，offset的递交会随着checkpoint的成功而递交，从而实现一致性语义，默认就是true

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //在数据源上添加水印
        SingleOutputStreamOperator<String> watermarkStream = kafkaDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new TimestampAssignerSupplier<String>() {
                            @Override
                            public TimestampAssigner<String> createTimestampAssigner(Context context) {
                                return new TimestampAssigner<String>() {
                                    @Override
                                    public long extractTimestamp(String element, long recordTimestamp) {
                                        return Long.parseLong(element.split(",")[0]);
                                    }
                                };
                            }
                        }).withIdleness(Duration.ofSeconds(60))
        );

        //todo 3）单词计数操作
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = watermarkStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return new Tuple2<String, Long>(value.split(",")[1], 1L);
            }
        });
        //todo 4）单词分组操作
        wordAndOne.keyBy(x-> x.f0).window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS))).process(
                new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long sum = 0L;
                        Iterator<Tuple2<String, Long>> iterator = elements.iterator();
                        while (iterator.hasNext()){
                            Tuple2<String, Long> tuple2 = iterator.next();
                            System.out.println(tuple2.f0);
                            sum += tuple2.f1;
                        }
                        out.collect(s + ","+sum);
                    }
                }
        ).print();

        env.execute();

        //todo 6）启动作业
        env.execute();
    }
}
