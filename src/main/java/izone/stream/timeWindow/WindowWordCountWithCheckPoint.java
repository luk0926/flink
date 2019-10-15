package izone.stream.timeWindow;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.timeWindow
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-14 15:03
 */

public class WindowWordCountWithCheckPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

         // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend
        //env.setStateBackend(new MemoryStateBackend());  默认
        //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true));

        DataStreamSource<String> text = env.addSource(new MyRichParalleSourceWord()).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleData = text.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        //timeWindow：滑动窗口、滚动窗口
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tupleData.keyBy(0)
                .timeWindow(Time.seconds(2), Time.seconds(1))
                //增量聚合
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });
                //.sum(1);


        //countWindow
        /*SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tupleData.keyBy(0)
                .countWindow(100, 10)
                .sum(1);*/

        sum.print().setParallelism(1);

        env.execute(WindowWordCountWithCheckPoint.class.getSimpleName());
    }

    public static class WordCount {
        public String word;
        public int count;

        public WordCount() {

        }

        public WordCount(String word, int count) {
            this.word = word;
            this.count = count;
        }
    }
}