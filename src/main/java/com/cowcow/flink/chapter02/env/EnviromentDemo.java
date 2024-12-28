package com.cowcow.flink.chapter02.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnviromentDemo {
    public static void main(String[] args) throws Exception {
        //自动识别是 远程集群 ，还是idea本地环境
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();

        //conf对象可以去修改一些参数
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT, "8082");//RestOptions.BIND_PORT 端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.execute();
    }
}
