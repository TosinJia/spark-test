package day0112

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  需求：求出每个学院，访问第一位的网址
  *  bigdata:video
  *  java:video
  *  python:teacher
  */
object UrlGroupCount {
  def main(args: Array[String]): Unit = {
    //1. 创建SparkContext
    val conf: SparkConf = new SparkConf().setAppName("UrlGroupCount").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    //2. 加载数据
    val rdd1: RDD[String] = sc.textFile("e:/temp/itstar.log")
    //3. 切分
    val rdd2: RDD[(String, Int)] = rdd1.map(line => {
      val strings: Array[String] = line.split("\t")
      //（网址，1）
      (strings(1), 1)
    })

    //4. 求出总的访问量 （网址，总访问量）
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_+_)
    //5. 取出学院
    val rdd4: RDD[(String, String, Int)] = rdd3.map(x => {
      //拿到url
      val url: String = x._1
      //url中拿到主机名
      val host: String = new URL(url).getHost.split("[.]")(0)
      //元祖输出
      (host, url, x._2)
    })
    //6. 按照学院进行分组
    val rdd5: RDD[(String, List[(String, String, Int)])] = rdd4.groupBy(_._1).mapValues(it => {
      //倒序
      it.toList.sortBy(_._3).reverse.take(1)
    })
    rdd5
    //7. 遍历打印
    rdd5.foreach(x => {
      println("学院："+x._1+"，访问第一的为："+x._2)
    })
    //关闭资源
    sc.stop()
  }
}
