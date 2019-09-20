package izone.stream.streamApi

import java.{lang, util}

import izone.stream.source.MyNoParalleSourceScala
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.stream.streamApi
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-20 18:32
  */

object StreamDemoSplitScala {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val text: DataStream[Long] = env.addSource(new MyNoParalleSourceScala).setParallelism(1)

    val split: SplitStream[Long] = text.split(new OutputSelector[Long] {
      override def select(value: Long) = {
        val outPutNames: util.ArrayList[String] = new util.ArrayList[String]()

        if (value % 2 == 0) {
          outPutNames.add("even")
        } else {
          outPutNames.add("odd")
        }

        outPutNames
      }
    })

    val value: DataStream[Long] = split.select("odd","even")

    value.print().setParallelism(1)


    env.execute(this.getClass.getSimpleName)
  }
}
