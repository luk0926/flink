package cn.jiguang.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @BelongsProject: flink
 * @BelongsPackage: cn.jiguang.source
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-21 15:58
 */

public class KafkaSource {
    public static FlinkKafkaConsumer011<String> getKafkaSource() {
        //指定kafka source
        String topic = "allData";
        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "cts04:9092");
        prop.setProperty("group.id", "con1");
        FlinkKafkaConsumer011<String> myConsumer =  new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);

        return myConsumer;
    }
}