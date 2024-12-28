package com.cowcow.flink.chapter08.temporal;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 需求描述
 * 订单表和汇率表，将汇率表设置成时态表，这样用户就可以根据订单表中的下单时间Join下单时的汇率表当时最新的维度数据
 */
public class TemporalTableJoinEventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        //todo 2）设置并行度
        env.setParallelism(1);



        //todo 4）加载数据
        String rateOrderPath = TemporalTableJoinEventTime.class.getClassLoader().getResource("rateOrder.csv").getPath();
        String rateHistoryPath = TemporalTableJoinEventTime.class.getClassLoader().getResource("rateHistory.csv").getPath();

        String sqlDDL =  "create table rateOrder (" +
                "                  order_id String," +
                "                  `price` DECIMAL(32,2)," +
                "                  currency String," +
                "                  order_time TIMESTAMP(3)," +
                "                  WATERMARK FOR order_time as order_time" +
                "                  ) with (" +
                "                  'connector' = 'filesystem'," +
                "                  'path' = 'file:///"+rateOrderPath+"'," +
                "                  'format' = 'csv'" +
                "                  )";

        tabEnv.executeSql(sqlDDL);

        sqlDDL =  "create table rateHistory (" +
                "                  currency String," +
                "                  `conversion_rate` DECIMAL(32,2)," +
                "                  update_time TIMESTAMP(3)," +
                "                  PRIMARY KEY (currency) NOT ENFORCED," +
                "                  WATERMARK FOR update_time as update_time" +
                "                  ) with (" +
                "                  'connector' = 'filesystem'," +
                "                  'path' = 'file:///"+rateHistoryPath+"'," +
                "                  'format' = 'csv'" +
                "                  )";

        tabEnv.executeSql(sqlDDL);

        String sql = "select order_id," +
                "   price," +
                "   rateOrder.currency," +
                "   conversion_rate," +
                "   order_time" +
                " from rateOrder" +
                "   left join rateHistory for system_time as of rateOrder.order_time" +
                "   on rateOrder.currency=rateHistory.currency";

        Table result = tabEnv.sqlQuery(sql);
        tabEnv.toAppendStream(result, Row.class).print();

        env.execute();

    }
}
