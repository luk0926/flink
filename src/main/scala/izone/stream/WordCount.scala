package izone.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by luk@jiguang.cn on 2019/9/3.
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val textStream: DataStream[String] = env.socketTextStream("localhost", 9999)

    //注意：必须要添加这一行隐式转行，否则下面的flatmap方法执行会报错
    import org.apache.flink.api.scala._

    val sum: DataStream[WordWithCount] = textStream.flatMap(_.split(" "))
      .map(t => WordWithCount(t, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(2), Time.seconds(1)) // 每隔1秒统计最近2秒的数据
      .sum("count")

    sum.print().setParallelism(1)

    //执行任务
    env.execute("Socket window count")
  }

  case class WordWithCount(word:String, count:Int)
}
