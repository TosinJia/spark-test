package day200103

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * 自定义分区
 * 按照网页名字分区
 *
 */
object TomcatLogPartition {
  def main(args: Array[String]): Unit = {
    //获取sc
    val sparkConf: SparkConf = new SparkConf().setAppName("tomcatlogcount").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    //1.读取文件
    val lineRDD: RDD[String] = sparkContext.textFile("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\spark-test\\data\\localhost_access_log.txt")
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
      (name, line)
    })
    //数据转换rdd转换为数组:jsp名字的数组
    val jspList: Array[String] = rdd1.map(_._1).distinct.collect()
    //自定义分区：分区规则，需要一个新的类
    val myPartition: MyPartition = new MyPartition(jspList)
    val rdd2: RDD[(String, String)] = rdd1.partitionBy(myPartition)
    //输出
    rdd2.saveAsTextFile("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\spark-test\\data\\TomcatLogPartition")

    sparkContext.stop()
  }


  //自定义分区：分区规则
  class MyPartition(name:Array[String]) extends Partitioner{
    //定义一个保存分区的条件的集合
    private val partitionMap: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
    //初始化分区号
    var partId = 0

    for(x <- name){
      partitionMap.put(x, partId)
      partId += 1
    }
    partitionMap.foreach(println)

    //多少个分区 返回分区个数
    override def numPartitions: Int = partitionMap.size

    //按什么来分区，根据名称返回分区号
    override def getPartition(key: Any): Int = {
      partitionMap.getOrElse(key.toString, 0)
    }
  }
}
