package izone.batch.batchAPI

import java.util

import org.apache.commons.collections.iterators.ArrayListIterator
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.batch.batchAPI
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-10-09 14:51
  */

object BarchDemoBroadCastScala {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val arr: ListBuffer[(String, Integer)] = new ListBuffer[Tuple2[String, Integer]]()
    arr += (("a", 13));
    arr += (("b", 15));
    arr += (("c", 18));
    arr += (("d", 20));

    val tupleData: DataSet[(String, Integer)] = env.fromCollection(arr)

    val broadCastData: DataSet[Map[String, Integer]] = tupleData.map(t => {
      Map((t._1, t._2))
    })

    val data: DataSet[String] = env.fromElements("a", "b", "c")

    val result: DataSet[String] = data.map(new RichMapFunction[String, String] {
      private var list: util.List[Map[String, Int]] = null
      private var allMap: Map[String, Int] = Map[String, Int]()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)

        this.list = getRuntimeContext.getBroadcastVariable[Map[String, Int]]("broadCastName")

        val it: util.Iterator[Map[String, Int]] = list.iterator()

        while (it.hasNext) {
          val next: Map[String, Int] = it.next()
          allMap = allMap.++(next)
        }

      }

      override def map(value: String): String = {
        val age: Int = allMap.get(value).get

        value + "," + age
      }
    }).withBroadcastSet(broadCastData, "broadCastName")

    result.print()
  }
}
