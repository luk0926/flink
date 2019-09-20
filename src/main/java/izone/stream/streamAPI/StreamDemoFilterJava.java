package izone.stream.streamAPI;

import izone.stream.source.MyNoParalleSourceJava;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.streamAPI
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-20 14:13
 */

public class StreamDemoFilterJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new MyNoParalleSourceJava()).setParallelism(1);

        SingleOutputStreamOperator<Long> map = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据：" + value);

                return value;
            }
        });

        SingleOutputStreamOperator<Long> filter = map.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });

        SingleOutputStreamOperator<Long> res = filter.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("过滤之后的数据：" + value);
                return value;
            }
        });

        SingleOutputStreamOperator<Long> sum = res.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        env.execute(Thread.currentThread().getStackTrace()[1].getClassName());
    }
}