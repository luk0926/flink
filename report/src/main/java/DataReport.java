import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sink.EsSink;
import sink.KafkaSink;
import source.KafkaSource;
import util.GetEnv;
import util.MyAggFunction;
import util.MyWatermark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by dell on 2019/10/21.
 */
public class DataReport {
    public static Logger logger = LoggerFactory.getLogger(DataReport.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = GetEnv.getEnv();

        //设置使用EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        /*
        * 获取kafka中的数据
        *
        *审核数据格式：
        * {"dt":"年-月-日 时:分:秒","type":"审核类型","username":"审核人员姓名","area":"大区"}
        *
        * */
        FlinkKafkaConsumer011<String> myConsumer = KafkaSource.getKafkaSource();
        DataStreamSource<String> data = env.addSource(myConsumer);

        SingleOutputStreamOperator<Tuple3<Long, String, String>> mapData = data.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                String dt = jsonObject.getString("dt");
                long time = 0;

                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = sdf.parse(dt);
                    time = date.getTime();
                } catch (ParseException e) {
                    //也可以把日志数据存储到其他介质中
                    logger.error("时间解析异常， dt:" + dt, e.getCause());
                }

                String type = jsonObject.getString("type");
                String area = jsonObject.getString("area");

                return new Tuple3<>(time, type, area);
            }
        });

        /*
        * 过滤掉异常的数据
        * */
        SingleOutputStreamOperator<Tuple3<Long, String, String>> filterData = mapData.filter(new FilterFunction<Tuple3<Long, String, String>>() {
            @Override
            public boolean filter(Tuple3<Long, String, String> value) throws Exception {
                boolean flag = true;

                if (value.f0 == 0) {
                    flag = false;
                }

                return flag;
            }
        });

        //保存迟到的数据
        OutputTag<Tuple3<Long, String, String>> outputTag = new OutputTag<Tuple3<Long, String, String>>("late-data"){};

        //窗口统计操作
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> resultData = filterData.assignTimestampsAndWatermarks(new MyWatermark())
                .keyBy(1, 2)
                .timeWindow(Time.minutes(1))
                .allowedLateness(Time.seconds(30)) //允许迟到30s
                .sideOutputLateData(outputTag) //记录迟到的数据
                .apply(new MyAggFunction());

        //获取迟到太久的数据
        DataStream<Tuple3<Long, String, String>> sideOutput = resultData.getSideOutput(outputTag);

        //把迟到的数据写入到kafka中
        FlinkKafkaProducer011<String> myProducer = KafkaSink.getKafkaSink();

        sideOutput.map(new MapFunction<Tuple3<Long, String, String>, String>() {
            @Override
            public String map(Tuple3<Long, String, String> value) throws Exception {
                return value.f0 + "\t" + value.f1 + "\t" + value.f2;
            }
        }).addSink(myProducer);


        /*
        * 把计算的结果写入到es中
        * */
        ElasticsearchSink.Builder<Tuple4<String, String, String, Long>> esSinkBuilder = EsSink.getEsSink();

        //设置批量写数据的缓冲区大小：每100条数据写一次
        esSinkBuilder.setBulkFlushMaxActions(100);
        //刷新前缓冲区的最大数据大小（以MB为单位）
        esSinkBuilder.setBulkFlushMaxSizeMb(500);

        resultData.addSink(esSinkBuilder.build());

        env.execute(DataReport.class.getSimpleName());
    }
}
