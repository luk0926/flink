package izone.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * Created by dell on 2019/9/15.
  */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val input: String = "C:\\Users\\dell\\Desktop\\data.txt"
    val output: String = "F:\\result.txt"

    val text: DataSet[String] = env.readTextFile(input)

    import org.apache.flink.api.scala._

    val sum: DataSet[(String, Int)] = text.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    sum.writeAsText(output).setParallelism(1)

    env.execute("batch word count")
  }
}
