package day200103

import java.sql.{Connection, DriverManager, PreparedStatement}

import day200103.TomcatLogCount.rdd1
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 将数据写入maysql
 * 先mysql创建表 CREATE TABLE tomcat_access_log(name VARCHAR(200), access_count INT)
 *
 */
object Spark2Mysql {
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
      (name, 1)
    })

    //3.聚合
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_+_)
    //4.排序，访问量降序
    val result: RDD[(String, Int)] = rdd2.sortBy(_._2, false)

    //创建mysql连接
    //问题：一条数据链接一个MySQL，对MySQL的压力会比较大
//    result.foreach(t => {
//      //1. 获取mysql连接
//      connection = DriverManager.getConnection("jdbc:mysql://bd-01-01:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "root", "000000")
//      preparedStatement = connection.prepareStatement("INSERT INTO tomcat_access_log (name ,access_count) VALUES(?,?)")
//      //往MySQL写数据
//      preparedStatement.setString(1, t._1)
//      preparedStatement.setInt(2, t._2)
//      preparedStatement.executeUpdate()
//    })

    //通过分区避免每条数据建立一次连接，每个分区建立一次连接
    result.foreachPartition(myConnection)

    sparkContext.stop()
  }
  var connection:Connection = null
  var preparedStatement: PreparedStatement = null

  def myConnection(t: Iterator[(String, Int)]): Unit ={
    try{
      //先获取链接
      connection = DriverManager.getConnection("jdbc:mysql://bd-01-01:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "root", "000000")
      preparedStatement = connection.prepareStatement("INSERT INTO tomcat_access_log (name ,access_count) VALUES(?,?)")
      t.foreach(
        it => {
          preparedStatement.setString(1, it._1)
          preparedStatement.setInt(2, it._2)
          preparedStatement.executeUpdate()
        }
      )
    }catch {
      case t:Throwable => t.printStackTrace()
    }finally {
      if(preparedStatement != null) preparedStatement.close()
      if(connection != null) connection.close()
    }

  }
}
