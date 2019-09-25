package izone.batch.batchAPI;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.batch.batchAPI
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-25 14:40
 */

public class BatchDemoFirstNJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2,"zs"));
        data.add(new Tuple2<>(4,"ls"));
        data.add(new Tuple2<>(3,"ww"));
        data.add(new Tuple2<>(1,"xw"));
        data.add(new Tuple2<>(1,"aw"));
        data.add(new Tuple2<>(1,"mw"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        //获取前三条数据
        text.first(3).print();
        System.out.println("=====================");

        //根据第一列进行分组，获取每组中前2个元素
        text.groupBy(0).first(2).print();
        System.out.println("=====================");

        //根据第一列进行分组，再根据第二列进行组内排序【升序】，获取每组的前两个元素
        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("=====================");

        //不分组，全局排序获取集合中的前三个元素，针对第一个元素升序，第二个元素降序
        text.sortPartition(0, Order.ASCENDING).sortPartition(1,Order.DESCENDING).first(3).print();

    }
}