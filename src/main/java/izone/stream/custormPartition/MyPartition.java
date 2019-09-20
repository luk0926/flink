package izone.stream.custormPartition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @BelongsProject: flink
 * @BelongsPackage: izone.stream.custormPartition
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-20 16:05
 */

public class MyPartition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        if(key % 2==0){
            return 0;
        }else{
            return 1;
        }
    }
}