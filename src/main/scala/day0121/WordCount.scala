package day0121

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sparkStreaming-wordcount
  * rdd: 创建程序入口 sparkContext
  * dataFrame: 创建程序入口 sparkSesion
  *
  * 之前flink测试
  *   瑞士军刀 nc
  *   yum insall nc
  *   nc -lk 7788
  * */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 1 创建sparkContext
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // 2 创建sparkContext 产生批次的时间2秒
    val streamingContext: StreamingContext = new StreamingContext(sc,Milliseconds(2000))

    // 3 创建Dsteam，首先接入数据源
    // socket
    val receiverInputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("127.0.0.1",7788)

    // 4 进行计算 创建 Dstream
    val dStream: DStream[(String, Int)] = receiverInputDStream.flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_ + _)

    // 5 打印结果
    dStream.print()

    //6 注意：需要启动sparkstreaming程序
    streamingContext.start()
    // 正常退出 eixt quit
    streamingContext.awaitTermination()
  }
}
