import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @BelongsProject: flink
 * @BelongsPackage: PACKAGE_NAME
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-22 15:09
 */

public class DataReportAll {
    static Logger logger = LoggerFactory.getLogger(DataReport.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置使用EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
         * 配置kafka source
         *
         * */
        String topic = "allData";
        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "cts04:9092");
        prop.setProperty("group.id", "con1");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);

        DataStreamSource<String> kafkaData = env.addSource(myConsumer);

        SingleOutputStreamOperator<Tuple3<Long, String, String>> mapData = kafkaData.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                String dt = jsonObject.getString("dt");
                String area = jsonObject.getString("area");
                String type = jsonObject.getString("type");

                long time = 0;

                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = sdf.parse(dt);
                    time = date.getTime();
                } catch (ParseException e) {
                    logger.error("时间解析异常， dt:" + dt, e.getCause());
                }


                return new Tuple3<>(time, type, area);
            }
        });

        //过滤掉异常数据
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

        //存储迟到的数据
        OutputTag<Tuple3<Long, String, String>> outputTag = new OutputTag<Tuple3<Long, String, String>>("late-data") {
        };


        //窗口统计操作
        SingleOutputStreamOperator<Tuple4<Long, Long, String, String>> res = filterData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>>() {
            long currentMaxTimestamp = 0L;
            long maxOutOfOrderness = 10000L; //最大乱序时间10s

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
                Long timestamp = element.f0;

                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);

                return timestamp;
            }
        }).keyBy(1, 2)
                .timeWindow(Time.minutes(1))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(outputTag)
                .process(new ProcessWindowFunction<Tuple3<Long, String, String>, Tuple4<Long, Long, String, String>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context ctx, Iterable<Tuple3<Long, String, String>> input, Collector<Tuple4<Long, Long, String, String>> out) throws Exception {
                        //获取分组信息
                        String type = tuple.getField(0).toString();
                        String area = tuple.getField(1).toString();

                        Iterator<Tuple3<Long, String, String>> it = input.iterator();

                        ArrayList<Long> arrayList = new ArrayList<>();
                        Long count = 0L;
                        while (it.hasNext()) {
                            Tuple3<Long, String, String> next = it.next();
                            arrayList.add(next.f0);
                            count += 1;
                        }

                        //排序
                        Collections.sort(arrayList);

                        Long maxTimestamp = arrayList.get(arrayList.size() - 1);

                        out.collect(new Tuple4<>(maxTimestamp, count, type, area));
                    }
                });

        //将迟到的数据写入到kafka
        DataStream<Tuple3<Long, String, String>> sideOutput = res.getSideOutput(outputTag);

        String outTopic = "allDataClean";
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", "cts04:9092");
        outProp.setProperty("transaction.timeout.ms", 60000 * 15 + "");

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(outTopic, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), outProp);

        sideOutput.map(new MapFunction<Tuple3<Long, String, String>, String>() {
            @Override
            public String map(Tuple3<Long, String, String> value) throws Exception {
                return value.f0 + "\t" + value.f1 + "\t" + value.f2;
            }
        }).addSink(myProducer);

        //将结果数据写入到es
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("cts04", 9200, "http"));

        ElasticsearchSink.Builder<Tuple4<Long, Long, String, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple4<Long, Long, String, String>>() {
                    public IndexRequest createIndexRequest(Tuple4<Long, Long, String, String> element) {
                        HashMap<String, Object> json = new HashMap<>();

                        json.put("time", element.f0);
                        json.put("count", element.f1);
                        json.put("type", element.f2);
                        json.put("area", element.f3);

                        return Requests.indexRequest()
                                .index("audit_index")
                                .type("audit_type")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple4<Long, Long, String, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));

                    }
                }
        );

        //设置批量写数据的缓冲区大小， 每100条数据写一次
        esSinkBuilder.setBulkFlushMaxActions(100);
        //刷新前缓冲区的最大数据大小
        esSinkBuilder.setBulkFlushMaxSizeMb(500);

        res.addSink(esSinkBuilder.build());


        env.execute(DataReportAll.class.getSimpleName());
    }
}