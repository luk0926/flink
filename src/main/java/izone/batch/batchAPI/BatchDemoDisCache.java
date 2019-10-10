package izone.batch.batchAPI;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.batch.batchAPI
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-09 17:51
 */

public class BatchDemoDisCache {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //注册一个文件
        env.registerCachedFile("C:\\Users\\JIGUANG\\Desktop\\res.txt", "res.txt");

        DataSource<String> data = env.fromElements("10", "20", "30", "40");

        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                //使用文件
                File file = getRuntimeContext().getDistributedCache().getFile("res.txt");
                List<String> lines = FileUtils.readLines(file);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.out.println(line);
                }
            }

            @Override
            public String map(String s) throws Exception {
                for (String s1 : dataList) {
                    s += s1;
                }

                return s;
            }
        });

        result.print();
    }
}