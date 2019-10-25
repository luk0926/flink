package cn.jiguang;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * @BelongsProject: flink
 * @BelongsPackage: cn.jiguang
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-25 10:17
 */

public class DataCleanAll {
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


        //指定kafka source
        //{"dt":"2019-10-20 17:43:00","countryCode":"US","data":[{"type":"s1","score":"0.3","level":"A"},{"type":"s2","score":"0.1","level":"B"}]}
        String topic = "allData";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "cts04:9092");
        prop.setProperty("group.id", "con1");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);
        DataStreamSource<String> kafkaData = env.addSource(myConsumer);

        //获取redis中的数据
        DataStream<HashMap<String, String>> redisData = env.addSource(new MyRedisSource()).broadcast();

        ConnectedStreams<String, HashMap<String, String>> connect = kafkaData.connect(redisData);

        SingleOutputStreamOperator<String> resData = connect.flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {
            HashMap<String, String> allMap = new HashMap<String, String>();

            //处理kafka数据
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String dt = jsonObject.getString("dt");
                Short countryCode = jsonObject.getShort("countryCode");
                JSONArray jsonArray = jsonObject.getJSONArray("data");

                //获取大区
                String area = allMap.get(countryCode);

                for (int i = 0; i < jsonArray.size() - 1; i++) {
                    JSONObject jsonObject1 = jsonArray.getJSONObject(i);

                    jsonObject1.put("area", area);
                    jsonObject1.put("dt", dt);

                    out.collect(jsonObject1.toJSONString());
                }
            }

            //处理redis中的数据
            @Override
            public void flatMap2(HashMap<String, String> value, Collector<String> collector) throws Exception {
                this.allMap = value;
            }
        });

        String outTopic = "allDataClean";
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", "cts04:9092");
        //设置事务最大时间
        outProp.setProperty("transaction.timeout.ms", 60000*15+"");

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(outTopic, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), outProp);

        resData.addSink(myProducer);

        env.execute(DataCleanAll.class.getSimpleName());
    }
}