package day200112

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 数据源 4. flume将数据推送到sparkStreaming
 * https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-flume_2.11/2.1.0
 */
object FlumePush {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //获取StreamingContext；在streaming中核数需要配置至少2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("FileStream")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //Flume获取DStream
    val flumeEvent: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(streamingContext, "192.168.0.108", 1234, StorageLevel.MEMORY_ONLY)
    //处理数据
    val lineDStream: DStream[String] = flumeEvent.map(e => {new String(e.event.getBody.array)})
    //打印
    lineDStream.print
    //启动
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
