package day200112

import day200108.Student
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 数据源 5. streaming拉取flume中的数据
 * https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-flume-sink_2.11/2.1.0
 * https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-flume_2.11/2.1.0
 */
object FlumePull {
  def main(args: Array[String]): Unit = {
//    org.apache.spark.streaming.flume.sink.SparkSink
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //获取StreamingContext:在Streaming中计算机核数需要配置至少2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePull")
    //使用高性能的序列化类库
    sparkConf.registerKryoClasses(Array(classOf[String], classOf[Student]))
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    //创建DStream
    val flumeEvent: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(streamingContext, "bd-01-01", 9999, StorageLevel.MEMORY_ONLY)
    //处理数据
    val lineDStream: DStream[String] = flumeEvent.map(e => {new String(e.event.getBody.array)})
    //打印
    lineDStream.print
    //启动
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
