package com.cowcow.flink.chapter03.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class Flink02_FromFileSourceDemo1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取Resource路径
        // String filePath = Flink02_FromFileSource.class.getClassLoader().getResource("input/words.txt").getPath();
        // log.info("filePath:{}", filePath);

        DataStreamSource<String> textFileSources = env.readTextFile("input/words.txt");
        log.info("textFileSources:{}", textFileSources);
        textFileSources.print();

        env.execute();
    }
}

