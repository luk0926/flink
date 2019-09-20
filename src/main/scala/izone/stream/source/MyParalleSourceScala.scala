package izone.stream.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.stream.source
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-20 11:47
  */


class MyParalleSourceScala extends ParallelSourceFunction[Long] {
  private var count: Long = 1L
  private var isRunning: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      sourceContext.collect(count)
      count += 1
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
