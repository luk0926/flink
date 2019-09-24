package izone.stream.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.sink
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-24 16:20
 */

public class StreamDemoToRedisJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.readTextFile("C:\\Users\\JIGUANG\\Desktop\\data.txt");

        SingleOutputStreamOperator<Tuple2<String, String>> words = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("words", value);
            }
        });

        //创建redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();

        //创建redisSink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());

        words.addSink(redisSink);

        env.execute("StreamDemoToRedisJava");
    }


    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>>{

        //设置数据使用的数据结构
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> stringStringTuple2) {
            return stringStringTuple2.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> stringStringTuple2) {
            return stringStringTuple2.f1;
        }
    }
}