package izone.stream.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.source
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-20 10:36
 */

public class StreamingDemoWithNoparalleSorce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new MyNoParalleSourceJava()).setParallelism(1);

        DataStream<Long> map = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据：" + value);

                return value;
            }
        });

        //每2秒处理一次数据
        DataStream<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        env.execute("StreamingDemoWithNoparalleSorce");
    }
}