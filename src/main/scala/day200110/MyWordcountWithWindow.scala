package day200110

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 4、窗口操作
 */
object MyWordcountWithWindow {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //获取sparkStreaming的环境:local[2]使用两核,必须是两个及以上,一个用来接受数据,另一个计算
    val sparkConf: SparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    //第一个参数:conf,第二个:采样时间
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(1))

    //读取数据
    val lines = streamingContext.socketTextStream("bd-01-01", 1234, StorageLevel.MEMORY_ONLY)
    //执行wordcount
    //计算
    val words = lines.flatMap(_.split(" "))
    val wordPair = words.map(x => (x, 1))
    //使用窗口操作,每个10秒采集前30秒的数据:第一个参数数据计算,第二个参是窗口长度,但三个参数窗口移动时间
    val wordCountResult = wordPair.reduceByKeyAndWindow((a:Int, b:Int) => (a+b), Seconds(30), Seconds(10))

    wordCountResult.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
