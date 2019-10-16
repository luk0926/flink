package izone.stream.watermark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.watermark
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-16 10:40
 */

public class MyRichParalleSourceWaterMark extends RichParallelSourceFunction<String> {

    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        ArrayList<String> data = new ArrayList<>();
        data.add("0001,1538359882000");//2018-10-01 10:11:22
        data.add("0001,1538359892000");//2018-10-01 10:11:32
        data.add("0001,1538359886000");//2018-10-01 10:11:26
        data.add("0001,1538359893000");//2018-10-01 10:11:33

        data.add("0001,1538359906000");//2018-10-01 10:11:46

        //data.add("0001,1538359896000");//2018-10-01 10:11:36
        //data.add("0001,1538359897000");//2018-10-01 10:11:37

        //data.add("0001,1538359899000");//2018-10-01 10:11:39
        data.add("0001,1538359894000");//2018-10-01 10:11:34
        data.add("0001,1538359891000");//2018-10-01 10:11:31
        //data.add("0001,1538359903000");//2018-10-01 10:11:43

        if (isRunning) {
            for (String s : data) {
                sourceContext.collect(s);

                Thread.sleep(2000);
            }
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