package day0123

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * 需求：kafka消费数据到sparkStreaming计算 累加历史的结果
  * */
object StateKafkaWC {
  //保持历史状态 wc 单词，次数 聚合的key，第一个类型就是单词，第二个类型是该单词在每个分区中出现的次数，第三个类型是以前的结果
  //累加历史的结果
  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])]) => {
    //总的次数=但前出现的次数+以前返回的结果
    iter.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))
  }

  def main(args: Array[String]): Unit = {
    //1 创建程序入口
    val conf: SparkConf = new SparkConf().setAppName("StateKafkaWC").setMaster("local[2]")
    val streamingContext: StreamingContext = new StreamingContext(conf, Milliseconds(20000))

    //需要累加历史数据 checkpoint 如果结果很重要
    //问题一：java.lang.IllegalArgumentException: requirement failed: The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint().
    //问题二：org.apache.hadoop.security.AccessControlException: Permission denied: user=TosinJia, access=WRITE, inode="/":root:supergroup:drwxr-xr-x
    // [root@bd ~]# hdfs dfs -mkdir /test
    // [root@bd ~]# hdfs dfs -chmod 777 /test
    streamingContext.checkpoint("hdfs://192.168.1.150:9000/test")
    //2 接入kafka数据源
    val zkQuorum = "172.16.88.240:2181"
    //val zkQuorum = "192.168.1.150:2181,192.168.1.151:2181,192.168.1.152:2181"
    // 组
    val groupId = "g1"
    // 主题
    val topics: Map[String, Int] = Map[String,Int]("wc"->1)

    val receiverInputDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext,zkQuorum,groupId,topics)
    //3 处理数据
    val dStream: DStream[String] = receiverInputDStream.map(_._2)
    //4 加入历史数据计算 参数1 自定义函数 参数2：分区器设置 参数3 是否使用
    val rDStream: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_, 1))
      .updateStateByKey(updateFunc, new HashPartitioner(streamingContext.sparkContext.defaultParallelism), true)
    //5 打印
    rDStream.print()
    //6 启动程序
    streamingContext.start()
    //7 关闭资源
    streamingContext.awaitTermination()
  }
}
