package day200106

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * 创建DF及操作
 * 1、指定Schema格式
 * spark session方式使用spark sql
 */
object Demo1 {
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
    spark.sql("select * from p_student order by age desc limit 3").show

    spark.stop()
  }
}
