package util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Created by dell on 2019/10/21.
 */
public class MyWatermark implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>> {
    long current_max_timestamp = 0L;
    final long maxOutOfOrderness = 10000L; //最大允许乱序的时间是10s

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(current_max_timestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
        Long timestamp = element.f0;

        current_max_timestamp = Math.max(timestamp, current_max_timestamp);

        return timestamp;
    }
}
