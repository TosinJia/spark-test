package day0111

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ForeachDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ForeachDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    //创建rdd
    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6),3)
    rdd1.foreach(println(_))

    val rdd2: RDD[String] = sc.parallelize(List("tosin","reba","mimi"),3)
    val rdd3: RDD[(Int, String)] = rdd2.keyBy(_.length)
    println(rdd3.collect().toBuffer)

    println("keys:"+rdd3.keys.collect().toBuffer)
    println("values:"+rdd3.values.collect.toBuffer)
    //关闭资源
    sc.stop()
  }
}
