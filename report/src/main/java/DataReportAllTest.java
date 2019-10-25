import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
 * @CreateTime: 2019-10-25 14:35
 */

public class DataReportAllTest {
    private static Logger logger = LoggerFactory.getLogger(DataReportAllTest.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(5);

        // 每隔一分钟进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(60000);
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少30 s的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        // 检查点必须在10 s钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend
        env.setStateBackend(new RocksDBStateBackend("hdfs://cts04:9000/flink/checkpoints", true));


        //设置使用eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
         * 配置kafka source
         *
         * 审核数据格式：
         * {"dt":"年-月-日 时:分:秒","type":"审核类型","username":"审核人员姓名","area":"大区"}
         *
         * */
        String topic = "allData";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "cts04:9092");
        prop.setProperty("group.id", "con1");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);
        DataStreamSource<String> data = env.addSource(myConsumer);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        SingleOutputStreamOperator<Tuple3<Long, String, String>> map = data.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String value) throws Exception {
                long time = 0L;

                JSONObject jsonObject = JSON.parseObject(value);
                String dt = jsonObject.getString("dt");
                String type = jsonObject.getString("type");
                String username = jsonObject.getString("username");
                String area = jsonObject.getString("area");

                try {
                    Date date = sdf.parse(dt);
                    time = date.getTime();
                } catch (ParseException e) {
                    logger.error("时间解析异常:" + dt, e.getCause());
                }

                return new Tuple3<>(time, type, area);
            }
        });

        //过滤掉解析异常的数据
        SingleOutputStreamOperator<Tuple3<Long, String, String>> filter = map.filter(new FilterFunction<Tuple3<Long, String, String>>() {
            @Override
            public boolean filter(Tuple3<Long, String, String> value) throws Exception {
                boolean flag = true;

                if (value.f0 == 0) {
                    flag = false;
                }

                return flag;
            }
        });

        OutputTag<Tuple3<Long, String, String>> outputTag = new OutputTag<Tuple3<Long, String, String>>("late-data") {
        };

        //窗口统计
        SingleOutputStreamOperator<Tuple4<Long, Long, String, String>> resData = filter.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>>() {
            long currentMaxTimeStamp = 0L;

            //最大乱序时间为10s
            long maxOutOfOrders = 10000;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimeStamp - maxOutOfOrders);
            }

            @Override
            public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimeStamp) {
                Long timestamp = element.f0;

                currentMaxTimeStamp = Math.max(currentMaxTimeStamp, timestamp);

                return timestamp;
            }
        }).keyBy(1, 2)
                .timeWindow(Time.minutes(1))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(outputTag)
                .process(new ProcessWindowFunction<Tuple3<Long, String, String>, Tuple4<Long, Long, String, String>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple3<Long, String, String>> value, Collector<Tuple4<Long, Long, String, String>> out) throws Exception {
                        String type = tuple.getField(0).toString();
                        String area = tuple.getField(1).toString();

                        ArrayList<Long> arrayList = new ArrayList<>();
                        Long count = 0l;
                        Iterator<Tuple3<Long, String, String>> it = value.iterator();
                        while (it.hasNext()) {
                            Tuple3<Long, String, String> next = it.next();
                            arrayList.add(next.f0);

                            count += 1;
                        }

                        //排序
                        Collections.sort(arrayList);

                        long maxTimeStamp = arrayList.get(arrayList.size() - 1);

                        out.collect(new Tuple4<>(maxTimeStamp, count, type, area));
                    }
                });

        //将迟到的数据回传到kafka
        DataStream<Tuple3<Long, String, String>> sideOutput = resData.getSideOutput(outputTag);

        String outTopic = "";
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", "cts04:9092");
        outProp.setProperty("transaction.timeout.ms", 60000 * 15 + "");
        FlinkKafkaProducer011<String> myProoducer = new FlinkKafkaProducer011<>(outTopic, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), outProp);

        sideOutput.map(new MapFunction<Tuple3<Long, String, String>, String>() {
            @Override
            public String map(Tuple3<Long, String, String> value) throws Exception {
                return value.f0 + "\t" + value.f1 + "\t" + value.f2;
            }
        }).addSink(myProoducer);


        //将数据写入到es
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

        //设置最大缓冲区大小，每100条写一次
        esSinkBuilder.setBulkFlushMaxActions(100);
        //刷新前缓冲区的最大数据大小
        esSinkBuilder.setBulkFlushMaxSizeMb(500);

        resData.addSink(esSinkBuilder.build());

        env.execute(DataReportAllTest.class.getSimpleName());
    }
}