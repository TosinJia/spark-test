package day200103

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TomcatLogCount extends App {
  //获取sc
  val sparkConf: SparkConf = new SparkConf().setAppName("tomcatlogcount").setMaster("local")
  val sparkContext = new SparkContext(sparkConf)
  //1.读取文件
  private val lineRDD: RDD[String] = sparkContext.textFile("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\spark-test\\data\\localhost_access_log.txt")
  //2.解析日志，网页名称
  /**
   * 192.168.88.1 - - [30/Jul/2017:12:53:43 +0800] "GET /MyDemoWeb/head.jsp HTTP/1.1" 200 713
   * 网页名称 MyDemoWeb/head.jsp
   */
  val rdd1 = lineRDD.map(line => {
    //1.或两个引号之间的数据
    val index1: Int = line.indexOf("\"")
    val index2: Int = line.lastIndexOf("\"")
    val info1 = line.substring(index1,index2)
    //2.获取两个空格之间的数据
    val index3 = info1.indexOf(" ")
    val index4 = info1.lastIndexOf(" ")
    val info2: String = info1.substring(index3,index4)
    //3.获取jsp的名字
    val name = info2.substring(info2.indexOf("/")+1)
    (name, 1)
  })

  //3.聚合
  val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_+_)
  //4.排序，访问量降序
  val rdd3: RDD[(String, Int)] = rdd2.sortBy(_._2, false)
  //5.打印
  rdd3.foreach(println)

  sparkContext.stop()
}
