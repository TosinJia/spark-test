package day200112

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 数据源 2. 文件流：很少使用
 * 必须新建文件 放入指定文件夹；旧文件检测不到，赋值出来的旧文件
 */
object FileStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //获取StreamingContext；在Streaming中计算机核数需要配置至少2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("FileStream")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //使用文件流创建DStream
    val line: DStream[String] = streamingContext.textFileStream("E:\\temp\\filestream")
    //处理数据
    val result: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //打印
    result.print(1000)
    //启动
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
