package izone.stream.streamAPI;

import izone.stream.source.MyNoParalleSourceJava;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.streamAPI
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-20 15:12
 */

public class StreamDemoSplitJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new MyNoParalleSourceJava()).setParallelism(1);

        SplitStream<Long> split = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPutName = new ArrayList<>();

                if (value % 2 == 0) {
                    outPutName.add("even");
                } else {
                    outPutName.add("odd");
                }

                return outPutName;
            }
        });

        DataStream<Long> odd = split.select("odd");
        DataStream<Long> all = split.select("even", "odd");

        odd.print().setParallelism(1);
        all.print().setParallelism(1);

        odd.shuffle();

        env.execute(Thread.currentThread().getStackTrace()[1].getClassName());
    }
}