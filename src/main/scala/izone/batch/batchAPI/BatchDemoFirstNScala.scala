package izone.batch.batchAPI

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.batch.batchAPI
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-27 14:47
  */

object BatchDemoFirstNScala {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data = ListBuffer[Tuple2[Int,String]]()
    data.append((2,"zs"))
    data.append((4,"ls"))
    data.append((3,"ww"))
    data.append((1,"xw"))
    data.append((1,"aw"))
    data.append((1,"mw"))

    val text: DataSet[(Int, String)] = env.fromCollection(data)

    text.first(3).print()
    println("=========================")

    text.groupBy(0).first(2).print()
    println("==========================")

    text.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).first(3).print()
  }
}
