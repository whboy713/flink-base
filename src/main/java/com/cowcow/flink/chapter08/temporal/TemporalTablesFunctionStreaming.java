package com.cowcow.flink.chapter08.temporal;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.*;
import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * 使用时态表函数对访问的商品信息进行拉宽操作（流的方式实现）
 * 需要两个流数据：
 * 1）商品访问事件流
 * 2）商品基础信息流
 * 商品访问事件流中的商品id与商品基础信息流的数据进行拉宽操作使用时态表函数进行关联
 */
public class TemporalTablesFunctionStreaming {


    public static void main(String[] args) throws Exception {
        //todo 1）构建flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        //todo 2）设置并行度
        env.setParallelism(1);


        //定义访问的kafka集群地址
        String kafkaBootstrapServers = "node01:9092";
        //定义访问事件流的topic
        String browseTopic = "browseTopic2";
        //定义商品基础信息流的topic
        String productInfoTopic = "productHistoryInfoTopic2";
        //定义访问事件流的消费者组id
        String browseTopicGroupID = "browseTopicGroupID_002";
        //定义访问商品基础信息流的消费者组id
        String productInfoTopicGroupID = "productInfoTopicGroupID_002";

        //todo 4）构建数据源
        //构建访问事件流的数据源
        //注意: 为了在北京时间和时间戳之间有直观的认识，这里的UserBrowseLog中增加了一个字段eventTimeTimestamp作为eventTime的时间戳
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers", kafkaBootstrapServers);
        browseProperties.put("group.id", browseTopicGroupID);
        browseProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        browseProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        DataStreamSource<String> browseStream = env
                .addSource(new FlinkKafkaConsumer<>(browseTopic, new SimpleStringSchema(), browseProperties));

        browseStream.print("事件流原始数据>>>");
        SingleOutputStreamOperator<UserBrowseLog> browseWatermarkStream = browseStream.process(new BrowseKafkaProcessFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBrowseLog>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBrowseLog>() {
                                    @Override
                                    public long extractTimestamp(UserBrowseLog userBrowseLog, long l) {
                                        return userBrowseLog.getEventTimeTimestamp();
                                    }
                                })
                );
        browseWatermarkStream.print("事件流水印数据>>>");

        //构建商品信息流的数据源
        Properties productInfoProperties = new Properties();
        productInfoProperties.put("bootstrap.servers", kafkaBootstrapServers);
        productInfoProperties.put("group.id", productInfoTopicGroupID);
        productInfoProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        productInfoProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        DataStreamSource<String> productInfoStream = env
                .addSource(new FlinkKafkaConsumer<>(productInfoTopic, new SimpleStringSchema(), productInfoProperties));
        browseStream.printToErr("商品流原始数据>>>");
        DataStream<ProductInfo> productInfoWatermarkStream = productInfoStream
                .process(new ProductInfoProcessFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ProductInfo>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<ProductInfo>() {
                                    @Override
                                    public long extractTimestamp(ProductInfo productInfo, long l) {
                                        return productInfo.getUpdatedAtTimestamp();
                                    }
                                })
                );
        productInfoWatermarkStream.printToErr("商品流水印数据>>>");

        //todo 5）将DataStream转换成注册为时态视图
        tabEnv.createTemporaryView("browse",browseWatermarkStream, $("userID"), $("eventTimeTimestamp"),
                $("eventType"), $("eventTime"), $("productID"), $("browseRowtime").rowtime());
        tabEnv.createTemporaryView("productInfo",productInfoWatermarkStream, $("productID"), $("updatedAtTimestamp"),
                $("productName"), $("productCategory"), $("productPrice"), $("updatedAt"), $("productInfoRowtime").rowtime());

        //todo 6）使用sql的方式连接两张表
        TemporalTableFunction productInfoFunction = tabEnv.scan("productInfo").createTemporalTableFunction($("productInfoRowtime"), $("productID"));
        tabEnv.createTemporaryFunction("productInfoFunc", productInfoFunction);

        String sql = ""
                + "SELECT "
                + "browse.userID, "
                + "browse.eventTime, "
                + "browse.eventTimeTimestamp, "
                + "browse.eventType, "
                + "browse.productID, "
                + "productInfo.productID, "
                + "productInfo.productName, "
                + "productInfo.productCategory, "
                + "productInfo.productPrice, "
                + "productInfo.updatedAt, "
                + "productInfo.updatedAtTimestamp "
                + "FROM "
                + " browse, "
                + " LATERAL TABLE (productInfoFunc(browse.browseRowtime)) as productInfo "
                + "WHERE "
                + " browse.productID=productInfo.productID";

        //todo 7）执行sql查询操作
        Table table = tabEnv.sqlQuery(sql);
        tabEnv.toAppendStream(table, Row.class).print();

        //执行
        env.execute();
    }

    /**
     * 将json字符串转换成javaBean对象
     */
    public static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBrowseLog>{
        @Override
        public void processElement(String value, Context context, Collector<UserBrowseLog> collector) throws Exception {
            UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);
            //增加一个long类型的时间戳
            DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            OffsetDateTime eventTime = LocalDateTime.parse(log.getEventTime(), format).atOffset(ZoneOffset.of("+08:00"));
            //将事件时间转换成毫秒的时间戳返回
            long eventTimestamp = eventTime.toInstant().toEpochMilli();
            log.setEventTimeTimestamp(eventTimestamp);

            collector.collect(log);
        }
    }

    /**
     * 解析Kafka数据
     */
    public static class ProductInfoProcessFunction extends ProcessFunction<String, ProductInfo> {
        @Override
        public void processElement(String value, Context ctx, Collector<ProductInfo> out) throws Exception {
            try {

                ProductInfo log = JSON.parseObject(value, ProductInfo.class);

                // 增加一个long类型的时间戳
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(log.getUpdatedAt(), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                log.setUpdatedAtTimestamp(eventTimeTimestamp);

                out.collect(log);
            } catch (Exception ex) {
            }
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class UserBrowseLog implements Serializable {
        private String userID;
        private String eventTime;
        private String eventType;
        private String productID;
        private Long eventTimeTimestamp;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ProductInfo implements Serializable {
        //产品id
        private String productID;
        //产品名称
        private String productName;
        //产品类型
        private String productCategory;
        //更新时间
        private String updatedAt;
        //更新时间戳
        private Long updatedAtTimestamp;
        private double productPrice;
    }
}
