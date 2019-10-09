package izone.batch.batchAPI

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.batch.batchAPI
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-10-09 16:10
  */

object BatchDemoCounterScala {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data: DataSet[String] = env.fromElements("a", "b", "c", "d")

    val res: DataSet[String] = data.map(new RichMapFunction[String, String] {

      //创建一个累加器
      private val numLines: IntCounter = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)

        getRuntimeContext.addAccumulator("num-lines", this.numLines)
      }

      override def map(value: String): String = {
        numLines.add(1)
        value
      }
    })

    res.writeAsText("C:\\Users\\JIGUANG\\Desktop\\res")
    val jobExecution: JobExecutionResult = env.execute("BatchDemoCounter")
    val int :Int = jobExecution.getAccumulatorResult("num-lines")
    print(int)
  }
}
