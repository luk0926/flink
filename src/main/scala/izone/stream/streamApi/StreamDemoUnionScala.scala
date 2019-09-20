package izone.stream.streamApi

import izone.stream.source.MyNoParalleSourceScala
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.stream.streamApi
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-20 18:19
  */

object StreamDemoUnionScala {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val text1: DataStream[Long] = env.addSource(new MyNoParalleSourceScala).setParallelism(1)
    val text2: DataStream[Long] = env.addSource(new MyNoParalleSourceScala).setParallelism(1)

    val text3: DataStream[Long] = text1.union(text2)

    val value: DataStream[Long] = text3.map(t => {
      println("接收到的数据：" + t)

      t
    })

    val sum: DataStream[Long] = value.timeWindowAll(Time.seconds(2)).sum(0)

    sum.print().setParallelism(1)

    env.execute(this.getClass.getSimpleName)
  }
}
