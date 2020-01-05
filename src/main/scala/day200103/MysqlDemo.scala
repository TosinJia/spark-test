package day200103

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取mysql
 * create table emp(id int, name VARCHAR(200), postion VARCHAR(200), superior int, birthday VARCHAR(200), wage double, bonus double, deptid int)
 * data/emp.csv
 */
object MysqlDemo {
  //匿名函数
  var connection = () =>{
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    DriverManager.getConnection("jdbc:mysql://bd-01-01:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "root", "000000")
  }
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setAppName("mysqlDemo").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    var mysqlRDD = new JdbcRDD(sparkContext, connection, "select * from emp where wage>? and wage<?", 1000, 2000, 1,
      r => {
        val ename = r.getString(2)
        val esal = r.getInt(6)
        (ename, esal)
      })
    mysqlRDD.foreach(println)
    val result = mysqlRDD.collect()
    println(result.toBuffer)

    sparkContext.stop()
  }
}
