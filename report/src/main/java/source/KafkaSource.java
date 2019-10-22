package source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @BelongsProject: flink
 * @BelongsPackage: source
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-22 10:10
 */

public class KafkaSource {
    public static FlinkKafkaConsumer011<String> getKafkaSource() {
        /*
         * 配置kafka source
         *
         * */
        String topic = "allData";
        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "cts04:9092");
        prop.setProperty("group.id", "con1");
        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);

        return myConsumer;
    }
}