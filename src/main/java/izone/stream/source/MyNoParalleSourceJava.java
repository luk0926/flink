package izone.stream.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.source
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-20 10:30
 */

public class MyNoParalleSourceJava implements SourceFunction<Long> {

    Long count = 1l;
    Boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {

        while (true){
            sourceContext.collect(count);
            count+=1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}