package izone.stream.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.stream.source
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-20 13:52
  */

object StreamDemoWithMyRichParalleSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val text: DataStream[Long] = env.addSource(new MyRichParalleSourceScala)

    val map: DataStream[Long] = text.map(t => {
      println("接收到的数据：" + t)

      t
    })

    val sum: DataStream[Long] = map.timeWindowAll(Time.seconds(2)).sum(0)

    sum.print().setParallelism(1)

    env.execute(this.getClass.getSimpleName)
  }
}
