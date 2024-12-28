package com.cowcow.flinksql.chapter07.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.or;

/**
 * 将数据写入到kafka中
 * 验证更新模式
 */
public class KafkaSinkTest {
    public static void main(String[] args) throws Exception {
        //todo 1）初始化flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 2）初始化表对象的执行环境
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        tabEnv.executeSql("CREATE TABLE students_json (\n" +
                "    id STRING,\n" +
                "    name STRING,\n" +
                "    age INT,\n" +
                "    sex STRING,\n" +
                "    clazz STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka', \n" +
                "  'topic' = 'students',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset', \n" +
                "  'format' = 'json'\n" +
                ");");
        tabEnv.executeSql("CREATE TABLE students_json2 (\n" +
                "    id STRING,\n" +
                "    name STRING,\n" +
                "    age INT,\n" +
                "    sex STRING,\n" +
                "    clazz STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka', \n" +
                "  'topic' = 'students2',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup2',\n" +
                "  'scan.startup.mode' = 'earliest-offset', \n" +
                "  'format' = 'json'\n" +
                ");\n" +
                "\n");



        //todo 4）读取数据（kafka）

        //todo 8）将查询结果输出到kafka中
//        tableResult.executeInsert("kafkaOutputTable");

        //todo 注意：将数据写入到kafka的时候，可以不加execute代码
        env.execute();
    }
}
