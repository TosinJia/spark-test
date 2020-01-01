package day191227

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * scala版本的wordcount
 *
 * 删除掉.setMaster("local") , textFile hdfs路径 打包，上传
 *
 * ./bin/spark-submit --master spark://bd-01-01:7077 --class day191227.WordCountScala /root/jars/spark-test-1.0-SNAPSHOT.jar hdfs://bd-01-01:9000/word.txt
 * 打印内容查看 web app id stdout
 */
object WordCountScala extends App {
  //获取Spark环境，setAppName定义程序名称，setMaster指定任务运行模式
  //一般在企业中先在测试环境运行，通过之后再在正式集群运行
  //程序中不指定任务运行模式，在提交任务的时候再指定运行模式
//  val sparkConf = new SparkConf().setAppName("wordcountscala-spark").setMaster("local")
    val sparkConf = new SparkConf().setAppName("wordcountscala-spark")
  val sparkContext = new SparkContext(sparkConf)

  var textFile = "E:\\BaiduNetdiskDownload\\data\\word.txt"
  if(args.length>0){
    textFile = args(0)
  }
  println("----spark/方式一-----")
  //读取数据
  val line = sparkContext.textFile(textFile)
  //切分
  val words: RDD[String] = line.flatMap(_.split(" "))
  //拼接
  val word21s: RDD[(String, Int)] = words.map((_,1))
  //合并
  val result: RDD[(String, Int)] = word21s.reduceByKey(_+_)
  //打印
  result.foreach(println)

  println("----spark/方式二-----")
  val result2: RDD[(String, Int)] = line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
  //打印
  result2.foreach(println)

  println("----spark/方式三-----")
  sparkContext.textFile(textFile).flatMap(_.split(" "))
    .map((_,1)).reduceByKey(_+_).foreach(println)
}