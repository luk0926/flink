package cn.jiguang;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * @BelongsProject: flink
 * @BelongsPackage: cn.jiguang
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-25 10:24
 */

public class MyRedisSource implements SourceFunction<HashMap<String, String>> {
    private Logger logger = LoggerFactory.getLogger(MyRedisSource.class);

    //每一分钟从redis中读取一次数据
    private long SLEEP_MILLION = 60000L;

    private boolean isRunning = true;

    private Jedis jedis = null;

    @Override
    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {
        this.jedis = new Jedis("cts04", 6379);

        //存储国家和大区的关系
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
                    logger.warn("从redis中获取到的数据为空！");
                }
            } catch (JedisConnectionException e) {
                logger.error("redis连接异常", e.getCause());
                jedis = new Jedis("cts04", 6379);
            } catch (Exception e) {
                logger.error("source数据源异常", e.getCause());
            }

            Thread.sleep(SLEEP_MILLION);

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