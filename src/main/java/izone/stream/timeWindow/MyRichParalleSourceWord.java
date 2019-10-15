package izone.stream.timeWindow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.timeWindow
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-14 14:41
 */

public class MyRichParalleSourceWord extends RichParallelSourceFunction<String> {

    Boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning) {

            //随机产生 a-z
            sourceContext.collect(String.valueOf((char) (Math.random() * 26 + 97)));

            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}