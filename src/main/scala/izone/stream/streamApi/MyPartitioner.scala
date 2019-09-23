package izone.stream.streamApi

import org.apache.flink.api.common.functions.Partitioner

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.stream.streamApi
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-23 15:36
  */

class MyPartitioner extends Partitioner[Long] {
  override def partition(k: Long, numPartitions: Int): Int = {
    println("分区总数：" + numPartitions)

    if (k % 2 == 0) {
      0
    } else {
      1
    }
  }
}
