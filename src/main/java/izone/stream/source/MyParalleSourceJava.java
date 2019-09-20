package izone.stream.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.source
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-20 10:52
 */

public class MyParalleSourceJava implements ParallelSourceFunction<Long> {
    Long count = 1l;
    Boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
            sourceContext.collect(count);
            count += 1;
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}