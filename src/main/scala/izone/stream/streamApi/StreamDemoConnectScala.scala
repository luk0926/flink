package izone.stream.streamApi

import izone.stream.source.MyNoParalleSourceScala
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.stream.streamApi
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-20 18:23
  */

object StreamDemoConnectScala {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val text1: DataStream[Long] = env.addSource(new MyNoParalleSourceScala).setParallelism(1)
    val text2: DataStream[Long] = env.addSource(new MyNoParalleSourceScala).setParallelism(1)

    val text2_str: DataStream[String] = text2.map(t => {
      t + "_str"
    })

    val text3: ConnectedStreams[Long, String] = text1.connect(text2_str)

    val result: DataStream[Any] = text3.map(t1=>t1, t2=>t2)

    result.print().setParallelism(1)

    env.execute(this.getClass.getSimpleName)
  }
}
