package com.cowcow.flink.chapter05.endtoend;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.util.Collector;



import java.sql.*;
public class Flink03_FlinkMySQLEndToEndDemo {

    public static void main(String[] args) throws Exception {
        //todo 1）初始化flink流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //todo 2）如果实现端对端一次性语义，必须要开启checkpoint
        env.enableCheckpointing(5000L);

        //todo 3）判断当前的环境
        if(SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_MAC){
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage("file:///Users/whboy/learn/temp/checkpoint");
        }else{
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(args[0]);
        }

        //todo 4）设置checkpoint的其他参数
        //设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(2000L);
        //同一个时间只能有一个栅栏在运行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //设置checkpoint的执行模式。仅执行一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);

        //todo 5）接入数据源，读取文件获取数据
        DataStreamSource<String> lines = env.socketTextStream("node01", 9999);

        //todo 3）数据处理
        //  3.1：使用flatMap对单词进行拆分
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                //返回数据
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //  3.2：对拆分后的单词进行记一次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        //  3.3：使用分组算子对key进行分组
        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(t -> t.f0);

        //  3.4：对分组后的key进行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = grouped.sum(1);

        //todo 6）将消费到的数据实时写入mysql
        sumed.addSink(new MysqlTwoPhaseCommitSink());

        //todo 7）运行作业
        env.execute();
    }

    /**
     * 通过两端递交的方式实现数据写入mysql
     */
    public static class MysqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<Tuple2<String, Integer>, ConnectionState, Void> {

        public MysqlTwoPhaseCommitSink() {
            super(new KryoSerializer<>(ConnectionState.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
        }

        /**
         * 每条数据执行一次该方法
         * @param connectionState
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        protected void invoke(ConnectionState connectionState, Tuple2<String, Integer> value, Context context) throws Exception {
            System.err.println("start invoke.......");
            Connection connection = connectionState.connection;
            PreparedStatement pstm = connection.prepareStatement("INSERT INTO t_wordcount (word, counts) VALUES (?, ?) ON DUPLICATE KEY UPDATE counts = ?");
            pstm.setString(1, value.f0);
            pstm.setInt(2, value.f1);
            pstm.setInt(3, value.f1);
            pstm.executeUpdate();
            pstm.close();
            //手动制造异常
            if(value.f0.equals("hive")) {
                System.out.println(1 / 0);
            }
        }

        /**
         * 开启事务
         * @return
         * @throws Exception
         */
        @Override
        protected ConnectionState beginTransaction() throws Exception {
            System.out.println("=====> beginTransaction... ");
            Class.forName("com.mysql.jdbc.Driver");
            //closing inbound before receiving peer's close_notify，链接地址中追加参数：useSSL=false
            Connection connection = DriverManager.getConnection("jdbc:mysql://node03:3306/test?characterEncoding=UTF-8&useSSL=false", "root", "123456");
            connection.setAutoCommit(false);
            return new ConnectionState(connection);
        }

        /**
         * 预递交
         * @param connectionState
         * @throws Exception
         */
        @Override
        protected void preCommit(ConnectionState connectionState) throws Exception {
            System.out.println("start preCommit...");
        }

        /**
         * 递交操作
         * @param connectionState
         */
        @Override
        protected void commit(ConnectionState connectionState) {
            System.out.println("start transaction...");
            Connection connection = connectionState.connection;
            try {
                connection.commit();
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException("提交事物异常");
            }
        }

        /**
         * 回滚操作
         * @param connectionState
         */
        @Override
        protected void abort(ConnectionState connectionState) {
            System.out.println("start abort...");
            Connection connection = connectionState.connection;
            try {
                connection.rollback();
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException("回滚事物异常");
            }
        }
    }

    static class ConnectionState {
        private final transient Connection connection;
        ConnectionState(Connection connection) {
            this.connection = connection;
        }
    }
}
