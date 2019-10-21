package cn.jiguang;

import cn.jiguang.sink.KafkaSink;
import cn.jiguang.source.KafkaSource;
import cn.jiguang.source.MyRedisSource;
import cn.jiguang.util.GetEnv;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Created by dell on 2019/10/20.
 */
public class DataClean {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = GetEnv.getStreamExecutionEnvironment();

        //获取kafka中的数据
        //{"dt":"2019-10-20 17:43:00","countryCode":"US","data":[{"type":"s1","score":"0.3","level":"A"},{"type":"s2","score":"0.1","level":"B"}]}
        DataStreamSource<String> kafkaData = env.addSource(KafkaSource.getKafkaSource());

        DataStream<HashMap<String, String>> redisData = env.addSource(new MyRedisSource()).broadcast();  //broadcast可以把数据发送到后面所有并行实例中

        SingleOutputStreamOperator<String> resData = kafkaData.connect(redisData).flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {
            private HashMap<String, String> allMap = new HashMap<String, String>();

            //处理kafka中的数据
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");

                //获取大区
                String area = allMap.get(countryCode);

                JSONArray jsonArray = jsonObject.getJSONArray("data");
                for (int i = 0; i < jsonArray.size(); i++) {
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

        //指定kafka sink
        resData.addSink(KafkaSink.getKafkaSink());

        env.execute(DataClean.class.getSimpleName());
    }
}
