package izone.stream.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.source
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-20 11:20
 */

public class StreamingDempWirhRichParalleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> text = env.addSource(new MyRichParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<Long> map = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据：" + value);

                return value;
            }
        });

        SingleOutputStreamOperator<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        env.execute("StreamingDempWirhRichParalleSource");
    }
}