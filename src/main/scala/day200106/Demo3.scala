package day200106

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * 3、就数据保存到数据库
 * 将数据写进MySQL指定表student_1中
 *
 * student_1表会自动创建，单name列有中文导致数据插入异常
 * #修改表的字符集，解决中文数据插入异常
 * ALTER TABLE student_1 CONVERT TO CHARACTER SET utf8 COLLATE utf8_general_ci;
 * #查看库的字符集
 * SHOW CREATE DATABASE company;
 * #查看表的字符集
 * SHOW CREATE TABLE student_1;
 */
object Demo3 {
  def main(args: Array[String]): Unit = {
    //创建spark session
    val spark = SparkSession.builder().appName("SparkSessionDemo").master("local").getOrCreate()

    //获取数据RDD
    val fileRDD = spark.sparkContext.textFile("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\spark-test\\data\\SparkSql\\student.txt").map(_.split("\t"))

    //通过StructType 获取schema
    val schema = StructType(
      List(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )
    //将fileRDD映射到Row RDD中，将数据映射到Row，创建DF
    val rowRDD = fileRDD.map(p => Row(p(0).toInt, p(1), p(2).toInt))
    val fileFD: DataFrame = spark.createDataFrame(rowRDD, schema)
    // 注册表
    fileFD.createOrReplaceTempView("p_student")

    // 执行sql
    val result: DataFrame = spark.sql("select * from p_student order by age desc")
    //写进MySQL中
    val pro = new Properties()
    pro.setProperty("user", "root")
    pro.setProperty("password", "000000")
    pro.setProperty("driver", "com.mysql.jdbc.Driver")

    result.write.mode("append").jdbc("jdbc:mysql://bd-01-01:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "student_1", pro)

    spark.stop()
  }
}
