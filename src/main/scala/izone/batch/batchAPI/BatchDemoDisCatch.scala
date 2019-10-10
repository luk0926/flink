package izone.batch.batchAPI

import java.io.File
import java.util

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.batch.batchAPI
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-10-09 18:15
  */

object BatchDemoDisCatch {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    import scala.collection.JavaConversions._

    val data: DataSet[String] = env.fromElements("10", "20", "30", "40")

    env.registerCachedFile("C:\\Users\\JIGUANG\\Desktop\\res.txt", "res.txt")

    val result: DataSet[String] = data.map(new RichMapFunction[String, String] {

      //private val list: ListBuffer[String] = new ListBuffer[String]
      private val list:util.ArrayList[String] = new util.ArrayList[String]()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)

        val file: File = getRuntimeContext.getDistributedCache.getFile("res.txt")
        val lines: java.util.List[String] = FileUtils.readLines(file)
        val it = lines.iterator()
        while (it.hasNext){
          val line: String = it.next()

          list.add(line)
          println("line:"+line)
        }
      }

      override def map(value: String): String = {
        var s: String = value

        for (s1 <- list) {
          s += s1
        }

        s
      }
    })

    result.print()
  }
}
