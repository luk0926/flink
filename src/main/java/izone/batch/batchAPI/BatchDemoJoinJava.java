package izone.batch.batchAPI;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.batch.batchAPI
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-25 16:09
 */

public class BatchDemoJoinJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "zs"));
        data1.add(new Tuple2<>(2, "ls"));
        data1.add(new Tuple2<>(3, "ww"));

        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, "beijing"));
        data2.add(new Tuple2<>(2, "shanghai"));
        data2.add(new Tuple2<>(3, "guangzhou"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        /*text1.join(text2).where(0) //指定第一个数据集中需要进行比较的元素角标
                .equalTo(0) //指定第二个数据集中需要进行比较的元素角标
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                          @Override
                          public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> last) throws Exception {
                              return new Tuple3<>(first.f0, first.f1, last.f1);
                          }
                      }

                ).print();
*/
        text1.join(text2).where(0)
                .equalTo(0)
                .map(new MapFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
                        return new Tuple3<>(value.f0.f0, value.f0.f1, value.f1.f1);
                    }
                }).print();
    }
}