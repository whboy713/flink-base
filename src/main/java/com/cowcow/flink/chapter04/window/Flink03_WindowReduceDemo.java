package com.cowcow.flink.chapter04.window;

import com.cowcow.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Flink03_WindowReduceDemo {

public static void main(String[] args) throws Exception {
    // 初始化Flink流处理环境，并设置并行度为1
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // 从指定IP和端口读取传感器数据，转换为WaterSensor对象
    env
            .socketTextStream("hadoop102", 7777)
            .map(value -> {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
            })
            // 按传感器ID进行分组，并使用滚动处理时间窗口（每10秒）
            .keyBy(WaterSensor::getId)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            // 在每个窗口内对传感器数据进行累加求和
            .reduce((value1, value2) -> {
                        // 打印reduce方法调用信息，并返回新的WaterSensor实例，包含累加的电压值
                        // 相同key的第一条数据来的时候，不会调用reduce方法,来一条数据，就会计算一次，但是不会输出
                        System.out.println("调用reduce方法，value1=" + value1 + ",value2=" + value2);
                        return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                    }
            )
            // 输出结果
            .print();

    // 执行流处理作业
    env.execute();
}

}
