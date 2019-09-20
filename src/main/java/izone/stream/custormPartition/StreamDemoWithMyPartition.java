package izone.stream.custormPartition;

import izone.stream.source.MyNoParalleSourceJava;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.custormPartition
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-20 16:09
 */

public class StreamDemoWithMyPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new MyNoParalleSourceJava()).setParallelism(1);

        //将long类型转换为Tuole1类型
        SingleOutputStreamOperator<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });

        DataStream<Tuple1<Long>> tuple1DataStream = tupleData.partitionCustom(new MyPartition(), 0);

        SingleOutputStreamOperator<Long> result = tuple1DataStream.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程id：" + Thread.currentThread().getId() + ", value" + value);

                return value.getField(0);
            }
        });

        result.print().setParallelism(1);

        env.execute(Thread.currentThread().getStackTrace()[1].getClassName());
    }
}