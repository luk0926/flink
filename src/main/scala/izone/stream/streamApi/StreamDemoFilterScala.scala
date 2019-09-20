package izone.stream.streamApi

import izone.stream.source.MyNoParalleSourceScala
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.stream.streamApi
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-20 18:14
  */

object StreamDemoFilterScala {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val text: DataStream[Long] = env.addSource(new MyNoParalleSourceScala).setParallelism(1)

    val map: DataStream[Long] = text.map(t => {
      println("接收到的数据：" + t)

      t
    })

    val filter: DataStream[Long] = map.filter(t => t % 2 == 0)

    val res: DataStream[Long] = filter.map(t => {
      println("过滤后的数据：" + t)

      t
    })

    res.print().setParallelism(1)

    env.execute(this.getClass.getSimpleName)
  }
}
