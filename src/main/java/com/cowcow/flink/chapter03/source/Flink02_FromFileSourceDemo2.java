package com.cowcow.flink.chapter03.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class Flink02_FromFileSourceDemo2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取Resource路径
        //String filePath = Flink02_FromFileSource.class.getClassLoader().getResource("input/words.txt").getPath();
        //log.info("filePath:{}", filePath);
        // TODO 从文件读:新Source架构(推荐)
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("input/words.txt"))
                .build();
        log.info("fileSource:{}", fileSource);

        DataStreamSource<String> filesource = env
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource");
        filesource.print();
        env.execute();
    }
}

