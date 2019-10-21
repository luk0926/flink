package cn.jiguang.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * @BelongsProject: flink
 * @BelongsPackage: cn.jiguang.sink
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-21 16:03
 */

public class KafkaSink {
    public static FlinkKafkaProducer011<String> getKafkaSink() {
        String outTopic = "allDataClean";
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", "cts04:9092");
        //设置事务超时时间
        outProp.setProperty("transaction.timeout.ms", 60000*15+"");

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(outTopic, new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), outProp,FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

        return myProducer;
    }
}