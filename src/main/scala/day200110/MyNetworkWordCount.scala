package day200110

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * streaming.NetworkWordCount
 * sparkStreaming实现wordcount
 * [root@bd-01-01 ~]# nc -l 1234
 * aa bb cc dd aa bb dd cc
 */
object MyNetworkWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //获取sparkStreaming的环境:local[2]使用两核,必须是两个及以上,一个用来接受数据,另一个计算
    val sparkConf: SparkConf = new SparkConf().setAppName("").setMaster("local[2]")

    //第一个参数:conf,第二个:采样时间
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    //创建一个Dstream
    val dt: ReceiverInputDStream[String] = streamingContext.socketTextStream("bd-01-01", 1234, StorageLevel.MEMORY_ONLY)

    //处理数据
//    val word: DStream[(String, Int)] = dt.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    val result: DStream[String] = dt.flatMap(_.split(" "))
    //transform和map类似，
    val word: DStream[(String, Int)] = result.transform(x => x.map((_, 1)))

    //打印数据
    word.print()

    //启动 spark streaming
    streamingContext.start()
    //等待任务结束
    streamingContext.awaitTermination()
  }
}
