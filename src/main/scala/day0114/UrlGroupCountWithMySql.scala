package day0114

import java.net.URL
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 集成mysql
  * 把最终结果存储到mysql中
  * */
object UrlGroupCountWithMySql {
  def main(args: Array[String]): Unit = {
    //1. 创建spark程序入口
    val conf: SparkConf = new SparkConf().setAppName("UrlGroupCountWithMySql").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    //sc.setCheckpointDir("hdfs://")
    //2. 加载数据
    val rdd1: RDD[String] = sc.textFile("")
    //3. 将数据切分
    val rdd2: RDD[(String, Int)] = rdd1.map(line => {
      val strings: Array[String] = line.split("\t")
      //元组输出
      (strings(1), 1)
    })
    //4. 累加求和
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_+_)
    //把rdd进行cache，增加程序执行效率
    //val rddCache: RDD[(String, Int)] = rdd3.cache()

    //5. 取出分组的学院
    val rdd4: RDD[(String, Int)] = rdd3.map(x => {
      val url = x._1
      val host: String = new URL(url).getHost.split("[.]")(0)
      (host, x._2)
    })
    //rdd4.checkpoint()
    //6. 根据学院分组
    val rdd5: RDD[(String, List[(String, Int)])] = rdd4.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._2).reverse.take(1)
    })
    //7. 把计算结果保存到mysql中
    rdd5.foreach(x =>{
      //把数据写到mysql
      val conn: Connection = DriverManager.getConnection("", "", "")
      //把spark结果插入到mysql中
      val sql: String = "INSERT INTO url_data(xueyuan,number_one) VALUES(?,?)"
      //执行sql
      val statement: PreparedStatement = conn.prepareStatement(sql)
      statement.setString(1, x._1)
      statement.setString(2, x._2.toString())
      statement.executeUpdate()
      statement.close()
      conn.close()
    })
    //8. 关闭资源 停掉应用 释放内存数据
    sc.stop()
  }
}
