package izone.stream.streamApi

import izone.stream.source.MyNoParalleSourceScala
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.stream.streamApi
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-23 15:41
  */

object StreamDemoMyPartition {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val text: DataStream[Long] = env.addSource(new MyNoParalleSourceScala).setParallelism(1)

    val tuple: DataStream[Tuple1[Long]] = text.map(t => {

      Tuple1(t)
    })

    val value: DataStream[Tuple1[Long]] = tuple.partitionCustom(new MyPartitioner, 0)

    val res: DataStream[Long] = value.map(t => {
      println("当前线程：" + Thread.currentThread().getId + ", value" + t)

      t._1
    })

    res.print().setParallelism(1)

    env.execute(this.getClass.getSimpleName)
  }
}
