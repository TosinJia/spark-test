package day200112

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * https://www.scala-lang.org/api/2.11.8/#scala.collection.mutable.Queue
 * 数据源 3. RDD队列流
 * 循环5，打印5次，每次打印一个
 */
object RDDStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //获取StreamingContext；在Streaming中计算机核数需要配置至少2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("FileStream")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //创建一个RDD队列，队列中的类型是RDD
    var rDDQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    //往队列中写一些数据
    for(i <- 1 to 5){
      rDDQueue += streamingContext.sparkContext.makeRDD(1 to 10)

    }
    //将这个RDD队列转换成DStream
    val queueDS: InputDStream[Int] = streamingContext.queueStream(rDDQueue)
    //打印
    queueDS.print()
    //启动
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
