package day0114

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark提供的连接mysql的方式
  *
  * */
object JdbcRddDemo {
  def main(args: Array[String]): Unit = {
    //spark程序入口
    val conf: SparkConf = new SparkConf().setAppName("JdbcRddDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    //匿名函数
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://192.168.1.150:3306/spark_urlcount?charactorEncoding=utf-8","root","TosinJia_1")
    }

    //查询数据
    val jdbcRdd: JdbcRDD[(Int, String, String)] = new JdbcRDD(sc, connection, "SELECT * FROM url_data WHERE uid >= ? AND uid <= ?", 1, 2, 2, mapRow => {
      val uid = mapRow.getInt(1)
      val xueyuan = mapRow.getString(2)
      val number_one = mapRow.getString(3)
      (uid, xueyuan, number_one)
    })

    val array: Array[(Int, String, String)] = jdbcRdd.collect()
    println(array.toBuffer)
    //关闭资源
    sc.stop()
  }
}
