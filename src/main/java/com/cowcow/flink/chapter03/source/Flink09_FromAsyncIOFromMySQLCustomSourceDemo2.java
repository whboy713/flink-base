package com.cowcow.flink.chapter03.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Flink09_FromAsyncIOFromMySQLCustomSourceDemo2 {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 添加自定义数据源
        DataStream<Integer> stream = env.addSource(new MySQLIdSource());

        // 3. 应用异步 I/O 转换操作。不启用重试
        DataStream<Tuple2<Integer, String>> resultStream = AsyncDataStream.unorderedWait(
                stream,
                new AsyncMySQLReader(),
                1000, // 超时时间
                TimeUnit.MILLISECONDS,
                100   // 最大并发请求数
        );

        // 4. 打印结果
        resultStream.print("MySQL Data");

        // 5. 执行作业
        env.execute("Flink Async IO from MySQL with Custom Source");
    }

    public static class MySQLIdSource implements SourceFunction<Integer> {
        private volatile boolean isRunning = true;
        private Connection connection;
        private PreparedStatement statement;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            String url = "jdbc:mysql://localhost:3306/test";
            String user = "root";
            String password = "123456";
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.prepareStatement("SELECT id FROM test");

            try (ResultSet resultSet = statement.executeQuery()) {
                while (isRunning && resultSet.next()) {
                    int id = resultSet.getInt("id");
                    ctx.collect(id);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static class AsyncMySQLReader extends RichAsyncFunction<Integer, Tuple2<Integer, String>> {
        private transient Connection connection;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化数据库连接
            String url = "jdbc:mysql://localhost:3306/your_database";
            String user = "your_username";
            String password = "your_password";
            connection = DriverManager.getConnection(url, user, password);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public void asyncInvoke(Integer id, ResultFuture<Tuple2<Integer, String>> resultFuture) throws Exception {
            CompletableFuture.supplyAsync(() -> {
                List<Tuple2<Integer, String>> results = new ArrayList<>();
                try (PreparedStatement statement = connection.prepareStatement("SELECT id, name FROM your_table WHERE id = ?")) {
                    statement.setInt(1, id);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            int resultId = resultSet.getInt("id");
                            String name = resultSet.getString("name");
                            results.add(new Tuple2<>(resultId, name));
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return results;
            }).whenComplete((result, throwable) -> {
                if (throwable == null) {
                    resultFuture.complete(result);
                } else {
                    resultFuture.completeExceptionally(throwable);
                }
            });
        }
    }
}


