package com.cowcow.flinksql.chapter01.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;


public class FlinkSQL02_DataStreamToTable {
    public static void main(String[] args) throws Exception {

        // 1. 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("input/order.csv");

        // map成样例类类型
        SingleOutputStreamOperator<OrderInfo> dataStream = inputStream.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String data) throws Exception {
                String[] dataArray = data.split(",");
                return new OrderInfo(dataArray[0], dataArray[1], dataArray[2], dataArray[3], Double.parseDouble(dataArray[4]));
            }
        });

        // 2. 基于tableEnv，将流转换成表
        Table dataTable  = tableEnv.fromDataStream(dataStream);

        // 3. 转换操作，得到提取结果
        // 3.1 调用table api，做转换操作
        Table resultTable = dataTable
                .select($("id"),$("timestamp"),$("category"),$("areaName"),$("money"))
                .filter($("areaName").isEqual("北京"));

        resultTable.execute().print();
        // 3.2 写sql实现转换
        //tableEnv.registerTable("dataTable", dataTable)
        tableEnv.createTemporaryView("inputTable", dataTable);
        Table resultSqlTable  = tableEnv.sqlQuery("select id,`timestamp`,category,areaName,money from inputTable where areaName='北京'");
        resultSqlTable.execute().print();


        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderInfo {
        private String id;
        private String timestamp;
        private String category;
        private String areaName;
        private Double money;
    }


}
