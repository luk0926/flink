package izone.stream.streamAPI;

import izone.stream.source.MyNoParalleSourceJava;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.streamAPI
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-20 14:40
 */

public class StreamDemoConnectJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text1 = env.addSource(new MyNoParalleSourceJava()).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new MyNoParalleSourceJava()).setParallelism(1);

        SingleOutputStreamOperator<String> text2_str = text2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return value + "_str";
            }
        });

        ConnectedStreams<Long, String> text3 = text1.connect(text2_str);

        SingleOutputStreamOperator<Object> map = text3.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value1) throws Exception {
                System.out.println("1_接收到的数据：" + value1);

                return value1;
            }

            @Override
            public Object map2(String value2) throws Exception {
                System.out.println("2_接收到的数据：" + value2);

                return value2;
            }
        });

        map.print().setParallelism(1);

        env.execute(Thread.currentThread().getStackTrace()[1].getClassName());
    }
}