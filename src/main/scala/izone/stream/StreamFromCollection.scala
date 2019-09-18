package izone.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.stream
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-18 10:42
  */

object StreamFromCollection {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val arr: Array[Int] = Array[Int](1, 10, 100)

    val value: DataStream[Int] = env.fromCollection(arr)

    val res: DataStream[Int] = value.map(_ + 1)

    res.print().setParallelism(1)

    env.execute("StreamFromCollection")
  }
}
