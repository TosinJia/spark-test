package day200112

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 数据源 1. 套接字流：在streaming中使用sql
 * [root@bd-01-01 ~]# nc -l 1234
 * [root@bd-01-01 ~]# nc -l bd-01-01 1234
 */
object StreamingWithSQL {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //获取StreamingContext；在Streaming中计算机核数需要配置至少2核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingWithSQL")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    //获取数据：套接字流，生成DStream
    val line: ReceiverInputDStream[String] = streamingContext.socketTextStream("bd-01-01", 1234, StorageLevel.MEMORY_ONLY)
    val word: DStream[String] = line.flatMap(_.split(" "))
    //使用spark-sql操作数据
    word.foreachRDD(
      rdd => {
        //需要spark sql环境
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//        val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        //将RDD转换成DF
        import spark.sqlContext.implicits._
        val df: DataFrame = rdd.toDF("word")
        df.createOrReplaceTempView("streamingwithsql")

        spark.sql("select word, count(1) from streamingwithsql group by word").show()
      }
    )

    //启动
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
