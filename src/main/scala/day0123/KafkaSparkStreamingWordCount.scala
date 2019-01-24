package day0123

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * 需求：kafka消费数据到sparkStreaming计算
  * */
object KafkaSparkStreamingWordCount {
  def main(args: Array[String]): Unit = {
    //1 创建sparkStreaming
    val conf: SparkConf = new SparkConf().setAppName("KafkaSparkStreamingWordCount")
      .setMaster("local[2]")
    // ?秒一个批次、打印一次
    val streamingContext: StreamingContext = new StreamingContext(conf, Milliseconds(2000))
    //2 接入kafka数据源（如何访问kafka集群？zookeeper） zk地址
    val zkQuorum = "172.16.88.240:2181"
    //访问组
    val groupId = "g1"
    //访问主题 wc
    val topics: Map[String, Int] = Map[String,Int]("wc"->1)
    //创建Dstream （往kafka写入的key，实际写入的内容）
    val receiverInputDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, zkQuorum,groupId,topics)
    //3 处理kafka数据
    val dStream: DStream[String] = receiverInputDStream.map(_._2)
    val rDStream: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    rDStream.print()
    //4 启动streaming程序
    streamingContext.start()
    //5 关闭资源
    streamingContext.awaitTermination()
  }
}
