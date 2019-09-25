package izone.batch.batchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.batch.batchAPI
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-25 14:12
 */

public class BatchDemoDistinctJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> arr = new ArrayList<>();
        arr.add("a b");
        arr.add("b d");
        arr.add("a c");
        arr.add("c a");

        DataSource<String> text = env.fromCollection(arr);

        FlatMapOperator<String, String> res = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String word : s1) {
                    collector.collect(word);
                }
            }
        });

        DistinctOperator<String> distinct = res.distinct();

        distinct.print();

        //env.execute(Thread.currentThread().getStackTrace()[1].getClassName());
    }
}