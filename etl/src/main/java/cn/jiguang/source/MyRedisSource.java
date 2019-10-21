package cn.jiguang.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * redis中保存国家和大区的关系
 * <p>
 * hset areas AREA_US US
 * <p>
 * hset areas AREA_CT TW,HK
 * <p>
 * hset areas AREA_AR PK,KW,SA
 * <p>
 * 需要把大区和个国家的关系组成hashMap
 * <p>
 * Created by dell on 2019/10/20.
 */
public class MyRedisSource implements SourceFunction<HashMap<String, String>> {
    private Logger logger = LoggerFactory.getLogger(MyRedisSource.class);

    //每一分钟读取一次数据
    private final long SLEEP_MILLION = 60000;

    private boolean isRunning = true;
    private Jedis jedis = null;

    @Override
    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {
        this.jedis = new Jedis("cts04", 6379);
        //存储所有国家和大区的对应关系
        HashMap<String, String> keyValueMap = new HashMap<>();
        while (isRunning) {
            try {
                keyValueMap.clear();

                Map<String, String> areas = jedis.hgetAll("areas");
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        keyValueMap.put(split, key);
                    }
                }

                if (keyValueMap.size() > 0) {
                    sourceContext.collect(keyValueMap);
                } else {
                    logger.warn("从redis中获取的数据为空！！！");
                }

                Thread.sleep(SLEEP_MILLION);

            } catch (JedisConnectionException e) {
                logger.error("redis连接异常,重新获取连接", e.getCause());
                jedis = new Jedis("cts04", 6379);
            } catch (Exception e) {
                logger.error("source 数据源异常", e.getCause());
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (jedis != null) {
            jedis.close();
        }
    }
}
