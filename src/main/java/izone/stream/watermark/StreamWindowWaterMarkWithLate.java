package izone.stream.watermark;

import izone.stream.timeWindow.MyRichParalleSourceWord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.watermark
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-16 14:26
 */

public class StreamWindowWaterMarkWithLate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1， 默认为当前机器的cpu数量
        env.setParallelism(1);

        //设置使用Eventime, 默认是processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //获取数据
        DataStreamSource<String> text = env.addSource(new MyRichParalleSourceWaterMark());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        SingleOutputStreamOperator<Tuple2<String, Long>> tupleData = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], Long.parseLong(split[1]));
            }
        });

        //抽取timestamp和生成watermark
        SingleOutputStreamOperator<Tuple2<String, Long>> waterMarkStream = tupleData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            long currentMaxTimestamp = 0L;

            //允许最大乱序时间10s
            final long maxOutOfOrderness = 10000L;

            /*
             *
             * 定义生成watermark的逻辑
             *
             * 默认每100ms调用一次
             *
             * */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            //定义提取timestamp
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {

                Long timestamp = element.f1;

                currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);

                long id = Thread.currentThread().getId();
                System.out.println("currentThreadId:" + id +
                        ", key:" + element.f0 +
                        ", eventime:[" + element.f1 + "|" + sdf.format(element.f1) +
                        "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) +
                        "],watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");

                return timestamp;
            }
        });

        //保存被丢弃的数据
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-date"){};

        SingleOutputStreamOperator<String> window = waterMarkStream.keyBy(0)
                .timeWindow(Time.seconds(3))
                .allowedLateness(Time.seconds(2)) //允许数据迟到2秒
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();

                        ArrayList<Long> arrayList = new ArrayList<>();

                        Iterator<Tuple2<String, Long>> iterator = input.iterator();
                        while (iterator.hasNext()) {
                            arrayList.add(iterator.next().f1);
                        }

                        Collections.sort(arrayList);

                        String result = key + "," + arrayList.size() + "," + sdf.format(arrayList.get(0)) + ","
                                + sdf.format(arrayList.get(arrayList.size() - 1)) + "," + sdf.format(timeWindow.getStart()) + "," + sdf.format(timeWindow.getEnd());

                        out.collect(result);
                    }
                });


        //把迟到的数据暂时打印到控制台，实际中可以保存到其他存储介质中
        DataStream<Tuple2<String, Long>> sideOutput = window.getSideOutput(outputTag);
        sideOutput.print();

        window.print();

        env.execute(StreamWindowWaterMarkWithLate.class.getSimpleName());
    }
}