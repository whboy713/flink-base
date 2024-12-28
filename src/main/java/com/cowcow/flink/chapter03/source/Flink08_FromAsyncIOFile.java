package com.cowcow.flink.chapter03.source;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Flink08_FromAsyncIOFile {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 3. 添加数据源
        DataStream<String> stream = env.fromElements("input/words.txt");

        // 4. 应用异步 I/O 转换操作。不启用重试
        DataStream<Tuple2<String, String>> resultStream = AsyncDataStream.unorderedWait(
                stream,
                new AsyncFileReader(),
                1000, // 超时时间
                TimeUnit.MILLISECONDS,
                100   // 最大并发请求数
        );

//        AsyncRetryStrategy asyncRetryStrategy =
//                new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder(3, 100L)
//                        .ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
//                        .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
//                        .build();
//
//        // 应用异步 I/O 转换操作并启用重试
//        SingleOutputStreamOperator<String> resultStream2  =
//                AsyncDataStream.unorderedWaitWithRetry(
//                        stream,
//                        new AsyncFileReader(),
//                        1000,
//                        TimeUnit.MILLISECONDS,
//                        100,
//                        asyncRetryStrategy);

        // 5. 打印结果
        resultStream.print("File Content");

        // 6. 执行作业
        env.execute("Flink Simple Async IO from File");
    }

    public static class AsyncFileReader extends RichAsyncFunction<String, Tuple2<String, String>> {

        @Override
        public void asyncInvoke(String filePath, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
            CompletableFuture.supplyAsync(() -> {
                try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                    StringBuilder content = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        content.append(line).append("\n");
                    }
                    return new Tuple2<>(filePath, content.toString());
                } catch (IOException e) {
                    return new Tuple2<>(filePath, "Error reading file: " + e.getMessage());
                }
            }).whenComplete((result, throwable) -> {
                if (throwable == null) {
                    resultFuture.complete(Collections.singletonList(result));
                } else {
                    resultFuture.completeExceptionally(throwable);
                }
            });
        }
    }

}

