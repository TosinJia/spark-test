package day0112

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：计算网页访问量前三名
  * 用户，喜欢视频 直播
  * 帮助企业做经营和决策
  * 当天所有界面的访问量
  */
object UrlCount {
  def main(args: Array[String]): Unit = {
    //1. 加载数据
    val conf: SparkConf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    //spark程序入口
    val sc: SparkContext = new SparkContext(conf)

    //载入数据
    val rdd1: RDD[String] = sc.textFile("e:/temp/itstar.log")
    //2. 对数据进行计算
    val rdd2: RDD[(String, Int)] = rdd1.map(line => {
      val strings: Array[String] = line.split("\t")
      //标记为出现1次
      (strings(1), 1)
    })
    //3. 将相同的网址进行累加求和 （网址,次数）
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_+_)
    //4. 排序 取出前三
    //val rdd4: Array[(String, Int)] = rdd3.sortBy(_._2, false).take(3)
    val rdd4: Array[(String, Int)] = rdd3.sortBy(_._2, false).collect
    //5. 遍历打印
    rdd4.foreach(x => {
      println("网址："+x._1+"\t访问量："+x._2)
    })
    //6.转换 java:toString scala:toBuffer
    println(rdd4.toBuffer)
    //关闭资源
    sc.stop()
  }
}
