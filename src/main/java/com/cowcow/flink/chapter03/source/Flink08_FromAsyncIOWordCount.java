package com.cowcow.flink.chapter03.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Flink08_FromAsyncIOWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 3. 添加数据源
        DataStream<String> stream = env.fromElements("input/words.txt");

        // 4. 应用异步 I/O 转换操作。不启用重试
        DataStream<Tuple2<String, String>> fileContentStream = AsyncDataStream.unorderedWait(
                stream,
                new AsyncFileReader(),
                1000, // 超时时间
                TimeUnit.MILLISECONDS,
                100   // 最大并发请求数
        );

        // 5. 处理文件内容并进行单词计数
        DataStream<Tuple2<String, Integer>> wordCountStream = fileContentStream
                .flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Integer>> out) {
                        String[] words = value.f1.split("\\s+");
                        for (String word : words) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                });

        // (word, 1)
        wordCountStream.keyBy(value->value.f0)
                .sum(1)
                .print("Word Count");;

        // 7. 执行作业
        env.execute("Flink Async IO Word Count");
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
