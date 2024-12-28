package com.cowcow.flink.chapter03.source;


import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class Flink08_FromAsyncIOHttp {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建数据源（例如，从文件或Kafka读取URL）
        String[] urls = {
                "http://example.com/data1",
                "http://example.com/data2",
                "http://example.com/data3"
        };

        // 3. 添加数据源
        KeyedStream<String, String> stream = env.fromElements(urls).keyBy(url -> url);

//        创建异步I/O操作：使用AsyncDataStream.unorderedWait方法，对输入的URL流执行异步HTTP请求。
//        设置超时和并发度：指定超时时间为1000毫秒，最大并发请求数为100。
//        处理结果：将异步请求的结果以元组形式（URL, 响应内容）输出，并打印为"Async HTTP Response"。
        SingleOutputStreamOperator<String> outputStreamOperator = AsyncDataStream.unorderedWait(
                stream,
                new AsyncHttpFunction(),
                1000,
                TimeUnit.MILLISECONDS,
                100);

        outputStreamOperator.print("Async HTTP Response");

        // 4. 执行作业
        env.execute("Flink Async IO from HTTP");
    }

    public static class AsyncHttpFunction extends RichAsyncFunction<String, String> {

        private transient CloseableHttpClient httpClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            httpClient = HttpClients.createDefault();
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (httpClient != null) {
                httpClient.close();
            }
        }

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
            HttpGet request = new HttpGet(input);
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String result = EntityUtils.toString(entity);
                    resultFuture.complete(Collections.singletonList(result));
                } else {
                    resultFuture.complete(Collections.emptyList());
                }
            } catch (IOException e) {
                resultFuture.completeExceptionally(e);
            }
        }
    }
}

