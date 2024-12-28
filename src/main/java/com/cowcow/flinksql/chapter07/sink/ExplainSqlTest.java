package com.cowcow.flinksql.chapter07.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

public class ExplainSqlTest {
    public static void main(String[] args) {
        //todo 1）初始化flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 2）初始化表对象的执行环境
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

        // explain Table API
        Table table1 = tabEnv.fromDataStream(stream1, $("count"), $("word"));
        Table table2 = tabEnv.fromDataStream(stream2, $("count"), $("word"));
        Table table = table1
                .where($("word").like("F%"))
                .unionAll(table2);

        System.out.println(table.explain());
    }
}
