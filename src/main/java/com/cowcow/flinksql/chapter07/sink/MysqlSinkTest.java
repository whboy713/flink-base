package com.cowcow.flinksql.chapter07.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * 消费kafka数据，实时写入到mysql数据库中
 */
public class MysqlSinkTest {
    public static void main(String[] args) throws Exception {

        //todo 1）初始化flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 2）初始化表对象的执行环境
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        //todo 3）设置并行度
        env.setParallelism(1);

        //todo 4）读取数据（kafka）
        //Flink有多个Kafka connector：universal，0.10和0.11。
        // Flink 1.7 开始就有这个universal的Kafka connector通用版本，跟Kafka client端的尽量保持最新版本。
        // 这个版本的Kafka客户端向后兼容代理版本0.10.0或更高版本。对于大多数用户而言，universal的Kafka连接器是最合适的。
        // 对于Kafka版本0.11.x和0.10.x，我们建议分别使用专用的0.11和0.10连接器。
        String sourceTable = "CREATE TABLE KafkaInputTable (\n" +
                "  `id` varchar,\n" +
                "  `timestamp` varchar,\n" +
                "  `money` double,\n" +
                "  `category` varchar\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'order',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";

        //todo 5）运行一个sql语句，创建表（创建在哪里？catalogs中（内存））
        tabEnv.executeSql(sourceTable);

        //todo 6）表的查询操作
        Table orderTable = tabEnv.from("KafkaInputTable");

        //todo 7）将数据转换成DataStream对象
        DataStream<Row> sqlResult = tabEnv.toAppendStream(orderTable, Row.class);
        sqlResult.print("SQL >>>");

        //todo 8）定义mysql的输出表
        String sinkTable = "CREATE TABLE orderResult (\n" +
                "  id varchar,\n" +
                "  `timestamp` varchar,\n" +
                "  `money` double,\n" +
                "  `category` varchar\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://node03:3306/test?characterEncoding=utf-8&useSSL=false',\n" +
                "   'table-name' = 'orderresult'," +
                "   'driver'='com.mysql.jdbc.Driver'," +
                "   'username' = 'root'," +
                "   'password' = '123456'," +
                "   'sink.buffer-flush.interval'='1s'," +
                "   'sink.buffer-flush.max-rows'='1'," +
                "   'sink.max-retries' = '5'" +
                ")";

        //定义sql语句
        String insert = "INSERT INTO orderResult SELECT * FROM KafkaInputTable";

        //todo 9）创建目标表
        tabEnv.executeSql(sinkTable);

        //todo 10）将源表的数据写入到目标表中
        tabEnv.executeSql(insert);

        env.execute();

    }
}
