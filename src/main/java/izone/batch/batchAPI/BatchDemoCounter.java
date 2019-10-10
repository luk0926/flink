package izone.batch.batchAPI;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.batch.batchAPI
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-09 15:45
 */

public class BatchDemoCounter {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        MapOperator<String, String> res = data.map(new RichMapFunction<String, String>() {
            //创建累加器
            private IntCounter numlines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                //注册累加器
                getRuntimeContext().addAccumulator("num-lines", this.numlines);
            }

            @Override
            public String map(String s) throws Exception {
                this.numlines.add(1);
                return s;
            }
        });

        res.writeAsText("C:\\Users\\JIGUANG\\Desktop\\res.txt");

        JobExecutionResult batchDemoCounter = env.execute("BatchDemoCounter");
        //获取累加器
        Object accumulatorResult = batchDemoCounter.getAccumulatorResult("num-lines");
        System.out.println(accumulatorResult);
    }
}