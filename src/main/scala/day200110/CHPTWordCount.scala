package day200110

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 设置检查点
 * [root@bd-01-01 ~]# nc -l 1234
 * aa bb cc dd aa bb dd cc
 */
object CHPTWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //获取streamingcontext
    val sparkConf: SparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    //第一个参数:conf,第二个:采样时间
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //设置检查点:本地目录,设置到hdfs中,一般使用hdfs:hdfs://192.1682.111:9000:/streaming/checkpt
    streamingContext.checkpoint("E:\\temp\\checkpoint")

    //读取数据Dstream
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("bd-01-01", 1234, StorageLevel.MEMORY_ONLY)

    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) =>{
      //通过spark内部的reduceByKey按key归约，然后这里传入某key当前批次的Seq/List，再计算当前批次的总和
      val currentCount: Int = currValues.sum
      //已累加的值
      val previousCount: Int = prevValueState.getOrElse(0)
      //返回累加后的结果，是一个Option[Int]类型
      Some(currentCount+previousCount)
    }
    //计算
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_,1))

    val totalWordCounts: DStream[(String, Int)] = pairs.updateStateByKey[Int](addFunc)
    //打印
    totalWordCounts.print
    //启动
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
