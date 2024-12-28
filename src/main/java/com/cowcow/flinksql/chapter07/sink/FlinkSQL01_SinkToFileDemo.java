package com.cowcow.flinksql.chapter07.sink;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 将数据写入到文件测试
 */
public class FlinkSQL01_SinkToFileDemo {
    public static void main(String[] args) throws Exception {
        //todo 1）创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 2）设置并行度
        env.setParallelism(1);

        //todo 3）从文件中读取数据
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("./input/order.csv")
        ).build();
        DataStreamSource<String> inputDataStream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource");
        inputDataStream.print();
        //todo 4）将读取的字符串数据转换成pojo
        SingleOutputStreamOperator<OrderBean> orderDataStream = inputDataStream
                .map(new MapFunction<String, OrderBean>() {
                    @Override
                    public OrderBean map(String value) throws Exception {
                        String[] dataArray = value.split(",");
                        return new OrderBean(dataArray[0], dataArray[1], Double.parseDouble(dataArray[2]), dataArray[3]);
                    }
                });

        //todo 4）基于tableEnv，将流转换成表
        Table table = tableEnv.fromDataStream(orderDataStream);

        //todo 6）对table对象使用table api编程的方式进行数据的查询操作
        Table tableResult = table
                .select($("id"), $("timestamp"), $("money"), $("category"))
                .filter($("category").isEqual("电脑"));

        //todo 7）将查询的结果写入到文件中
        TableDescriptor tableDescriptor = TableDescriptor.forConnector("filesystem")
                .option("path", "./output")
                .format(
                        FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", ",")
                        .build()
                )
                .schema(Schema.newBuilder()
                        .column("id",DataTypes.STRING())
                        .column("timestamp", DataTypes.STRING())
                        .column("money", DataTypes.DOUBLE())
                        .column("category", DataTypes.STRING())
                        .build())
                .build();


        tableEnv.createTemporaryTable("outputOrder", tableDescriptor);


        //todo 9）将table查询结果数据写入到目标表中
        tableResult.executeInsert("outputOrder");

        //todo 10）运行作业
        env.execute();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderBean {
        private String id;
        private String timestamp;
        private Double money;
        private String category;
    }
}
