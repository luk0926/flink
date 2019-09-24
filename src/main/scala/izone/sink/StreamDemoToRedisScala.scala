package izone.sink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @BelongsProject: flink
  * @BelongsPackage: izone.sink
  * @Author: luk@jiguang.cn
  * @CreateTime: 2019-09-24 16:50
  */

object StreamDemoToRedisScala {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val text: DataStream[String] = env.readTextFile("C:\\Users\\JIGUANG\\Desktop\\data.txt")

    val data: DataStream[(String, String)] = text.map(t => {
      ("words", t)
    })

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build()

    val redisSink: RedisSink[(String, String)] = new RedisSink[Tuple2[String,String]](conf, new MyRedisMapper)

    data.addSink(redisSink)

    env.execute(this.getClass.getSimpleName)
  }

  class MyRedisMapper extends RedisMapper[Tuple2[String,String]] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }

    override def getKeyFromData(t: (String, String)): String = {
      t._1
    }

    override def getValueFromData(t: (String, String)): String = {
      t._2
    }
  }
}
