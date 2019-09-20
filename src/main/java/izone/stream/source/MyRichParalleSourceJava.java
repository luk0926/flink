package izone.stream.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.source
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-20 11:16
 */

public class MyRichParalleSourceJava extends RichParallelSourceFunction<Long> {
    private Long count = 1l;
    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
            sourceContext.collect(count);
            count += 1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open...");

        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}