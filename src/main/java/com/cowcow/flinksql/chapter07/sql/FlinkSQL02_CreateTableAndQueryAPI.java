package com.cowcow.flinksql.chapter07.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL02_CreateTableAndQueryAPI {
    public static void main(String[] args) {

        // TODO 1.创建表环境
        // 1.2 写法二
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.创建表
        DataStreamSource<String> textStreamDS = env.socketTextStream("localhost", 7777);
        // 把table对象，注册成表名
        tableEnv.createTemporaryView("tmp", textStreamDS);
        //  用table api来查询
        Table source = tableEnv.from("source");
        Table result = source
                .where($("id").isGreater(5))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sumVC"))
                .select($("id"), $("sumVC"));

        // TODO 4.输出表
        result.executeInsert("sink");
    }
}
