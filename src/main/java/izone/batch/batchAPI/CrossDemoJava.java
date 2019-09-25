package izone.batch.batchAPI;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.batch.batchAPI
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-25 14:03
 */

public class CrossDemoJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> arr1 = new ArrayList<>();
        arr1.add("a");
        arr1.add("b");

        ArrayList<Integer> arr2 = new ArrayList<>();
        arr2.add(1);
        arr2.add(2);

        DataSource<String> text1 = env.fromCollection(arr1);
        DataSource<Integer> text2 = env.fromCollection(arr2);

        CrossOperator.DefaultCross<String, Integer> cross = text1.cross(text2);

        cross.print();

        //env.execute(Thread.currentThread().getStackTrace()[1].getClassName());
        //env.execute();
    }
}