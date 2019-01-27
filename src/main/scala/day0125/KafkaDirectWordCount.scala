package day0125

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, Milliseconds, StreamingContext}

/**
  * 直连方式
  * sparkStreaming 与 kafka整合
  * 测试：kafka正在启动，对于kafka消费数据进行处理
  *   如果程序终止，kafka依然在运行 并且有数据
  *   再次启动这个程序的话，数据是否会发生丢失
  * ./bin/kafka-topics.sh --zookeeper 192.168.1.150:2181,192.168.1.152:2181,192.168.1.152:2181 --create --replication-factor 3 --partitions 4 --topic kdwc
  * org.apache.spark.SparkException: Couldn't find leader offsets for Set([kdwc,0], [kdwc,1], [kdwc,2], [kdwc,3])
  *     配置hosts
  * */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    //1 创建sparkStreaming程序入口
    val conf: SparkConf = new SparkConf().setAppName("KafkaDirectWordCount")
      .setMaster("local[2]")
    val streamingContext: StreamingContext = new StreamingContext(conf, Duration(500))

    // 连接kafka 2 创建消费者组
    val group = "g1"
    // 3 创建主题
    val topic = "kdwc"
    // 4 指定kafka的broker地址
    val brokerList = "192.168.1.150:9092,192.168.1.151:9092,192.168.1.150:9092"
    // 5 指定zookeeper地址、更新偏移量记录使用zk（1、存储 2、监听）
    val zkQuorum = "192.168.1.150:2181,192.168.1.152:2181,192.168.1.152:2181"

    // 6 创建stream时使用topic集合
    val topics: Set[String] = Set(topic)
    // 7 创建zookeeper目录（存储偏移量）
    val zKGroupTopicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(group, topic)
    // 8 获取zookeeper存储偏移量的路径  consumers/g1/offsets/topic/0
    val zkTopicPath = s"${zKGroupTopicDirs.consumerOffsetDir}"

    // 9 设置kafka参数
    val kafkaParams = Map(
      //设置broker地址
      "metadata.broker.list"->brokerList,
      //设置组
      "group.id"->group,
      //设置偏移量 从头读Smallest
      "auto.offset.reset"->kafka.api.OffsetRequest.SmallestTimeString
    )
    // 10 创建zookeeper客户端，用户更新偏移量
    val zkClient: ZkClient = new ZkClient(zkQuorum)
    // 11 如果有记录则说明以前有记录过偏移量 /consumer/g1/topics
    val children: Int = zkClient.countChildren(zkTopicPath)
    //print(children)
    // 12 创建kafkaStream 存在两种情况 1 第一次读 2 从当前偏移量读
    var kafkaStream:InputDStream[(String,String)] = null
    // 13 如果zk保存offset 我们利用offset作为kafkaStream的起始位置
    var fromOffsets:Map[TopicAndPartition,Long] = Map()

    // 14 判断 如果保存过offset
    if(children>0){
      for(i <- 0 until children){
        //kafka分区中的偏移量
        val partitionOffset:String = zkClient.readData[String](s"$zkTopicPath/${i}")
        val topicAndPartition: TopicAndPartition = TopicAndPartition(topic,i)

        fromOffsets += (topicAndPartition -> partitionOffset.toLong)
      }
      //key：kafka的key value："hello tosin hello nn"
      // 定义如何读取数据
      val messageHandler = (mmd:MessageAndMetadata[String,String])=>(mmd.key(),mmd.message())

      // 15 创建KafkaUtils String,String 表示Dstream装的数据类型 key与value的解码器
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streamingContext, kafkaParams, fromOffsets, messageHandler)
    }else{
      //如果没有保存偏移量
      kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](streamingContext,kafkaParams,topics)
    }

    // 16 指定偏移量范围
    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()
    // 17 从kafka读取消息 DStream
    val transform: DStream[(String, String)] = kafkaStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    // 18 拿到数据
    val messages: DStream[String] = transform.map(_._2)
    // 19 依次迭代DStream的RDD进行计算
    // 遍历RDD
    messages.foreachRDD{rdd =>{
      // 遍历分区
      rdd.foreachPartition(partition =>{
        // 遍历元素
        partition.foreach(x =>{
          println(x)
        })
      })

      // 20 把分区对应的偏移量写入zookeeper中
      for(o <- offsetRanges){
        // 拿到分区号
        val zkPath = s"${zKGroupTopicDirs.consumerOffsetDir}/${o.partition}"
        // 分区的offset保存到zk
        ZkUtils.updatePersistentPath(zkClient,zkPath,o.untilOffset.toString)
      }
    }}
    // 21 启动sparkStreaming
    streamingContext.start()
    // 22 退出
    streamingContext.awaitTermination()
  }
}
