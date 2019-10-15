package izone.stream.timeWindow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.timeWindow
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-14 17:02
 */

public class WindowWordCountFull {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.addSource(new MyRichParalleSourceWord());

        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleData = text.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });

        SingleOutputStreamOperator<String> process = tupleData.keyBy(0).timeWindow(Time.seconds(2))
                //全量聚合
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        System.out.println("执行process。。。");
                        long count = 0;
                        for (Tuple2<String, Integer> element : elements) {
                            count++;
                        }

                        out.collect("window:" + context.window() + ",count:" + count);
                    }
                });

        process.print().setParallelism(1);

        env.execute(WindowWordCountWithCheckPoint.class.getSimpleName());
    }
}